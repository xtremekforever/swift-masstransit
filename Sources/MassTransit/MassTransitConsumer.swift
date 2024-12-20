import AMQPClient
import Logging
import NIOCore
import RabbitMq
import ServiceLifecycle
import Tracing

/// MassTransit Consumer, which can take one or more message types.
///
/// This implements the definition of a MassTransit Consumer which can receive one or more
/// message types from a single RabbitMq consumer and queue.
public actor MassTransitConsumer: Service {
    let connection: Connection
    let queueName: String
    let exchangeName: String
    let configuration: MassTransitConsumerConfiguration
    let routingKey: String
    let retryInterval: Duration
    let onConsumerReady: (@Sendable () async throws -> Void)?
    let logger: Logger

    private(set) public var isConsumerReady = false
    private var consumers: [String: (ByteBuffer) throws -> Void] = [:]

    /// Create the MassTransit Consumer.
    ///
    /// - Parameters:
    ///   - connection: RabbitMq `Connection` to use for this consumer.
    ///   - queueName: The queue name to use for this consumer. Example: "MyAwesomeConsumer"
    ///   - exchangeName: The exchange name to use for this consumer: Example: "MyAwesomeConsumer"
    ///   - routingKey: An optional routing key to use for routing from the exchange to the queue.
    ///   - configuration: Configuration for the consumer, which includes queue, exchange, and consumer options.
    ///   - retryInterval: The retry interval to use if unable to declare, bind, or consume. This will retry forever unless cancelled.
    ///   - onConsumerReady: Callback that is called once the RabbitMq consumer is "ready" (actually consuming events).
    ///   - logger: The logger to use for this instance.
    public init(
        using connection: Connection,
        queueName: String,
        exchangeName: String,
        routingKey: String = "",
        configuration: MassTransitConsumerConfiguration = .init(),
        retryInterval: Duration = MassTransit.defaultRetryInterval,
        onConsumerReady: (@Sendable () async throws -> Void)? = nil,
        logger: Logger = .init(label: .init(describing: MassTransitConsumer.self))
    ) {
        self.connection = connection
        self.queueName = queueName
        self.exchangeName = exchangeName
        self.routingKey = routingKey
        self.configuration = configuration
        self.retryInterval = retryInterval
        self.onConsumerReady = onConsumerReady
        self.logger = logger
    }

    private func createMessageConsumer<T: MassTransitMessage>(
        _: T.Type, messageType: String
    ) -> AnyAsyncSequence<MassTransitWrapper<T>> {
        // Create a stream + continuation
        logger.info("Consuming messages of type \(messageType) on queue \(queueName)...")
        let (stream, continuation) = AsyncStream.makeStream(of: MassTransitWrapper<T>.self)

        // Create a message handler
        consumers[messageType] = { buffer in
            let wrapper = try MassTransitWrapper(T.self, from: buffer)
            continuation.yield(wrapper)
        }

        // Handle termination
        continuation.onTermination = { _ in
            Task { await self.removeConsumer(messageType: messageType) }
        }

        return .init(
            stream.compactMap { wrapper in
                self.logger.trace("Decoded buffer from \(self.queueName) to wrapper: \(wrapper)")
                return wrapper
            }
        )
    }

    private func removeConsumer(messageType: String) {
        logger.debug("Removing consumer \(messageType) for \(queueName)...")
        consumers.removeValue(forKey: urn(from: messageType))
    }

    private func bindMessageExchange(
        _ messageExchange: String,
        _ exchangeOptions: ExchangeOptions,
        _ routingKey: String,
        _ bindingOptions: BindingOptions
    ) async throws {
        var firstAttempt = true

        while !Task.isCancelledOrShuttingDown {
            do {
                guard let channel = try await connection.getChannel() else {
                    throw AMQPConnectionError.connectionClosed(replyCode: nil, replyText: nil)
                }

                // Declare messageExchange using options
                try await channel.exchangeDeclare(messageExchange, exchangeOptions, logger)

                // Bind messageExchange to main exchange for this consumer
                try await channel.exchangeBind(
                    exchangeName, messageExchange, routingKey, bindingOptions, logger
                )

                return
            } catch AMQPConnectionError.connectionClosed(_, _) {
                if !firstAttempt {
                    logger.error(
                        "Connection closed while setting up message binding \(messageExchange)"
                    )
                }

                // Wait for connection, timeout after retryInterval
                await connection.waitForConnection(timeout: retryInterval)

                firstAttempt = false
            } catch {
                logger.error("Error setting up message binding \(messageExchange): \(error)")

                // Consume retry
                logger.debug("Will retry setting up \(messageExchange) in \(retryInterval)")
                try await Task.sleep(for: retryInterval)
            }
        }
    }

    /// Consume a specific message type from this consumer.
    ///
    /// This method adds a consumer for a specific message type and returns a stream of messages of
    /// that type when they are received and parsed. The message type determined by the
    /// `messageExchange` by default, or can be overridden by the `customMessageType` parameter.
    ///
    /// - Parameters:
    ///   - _: Type of the message to consume (example: `MyMessage.self`).
    ///   - messageExchange: The exchange name to use for this message. Defaults to the message type string but can be set to any custom value.
    ///   - exchangeOptions: Options for the message exchange. Defaults to `.massTransitDefaults` but can be customized as desired.
    ///   - routingKey: Optional routing key to use for the binding from the message exchange to the main consumer exchange.
    ///   - bindingOptions: Options for binding the message exchange to the main consumer exchange.
    ///   - customMessageType: Custom message type to use. This will not affect the message exchange name,
    ///     only the message type "urn" that is interpreted in the MassTransit wrapper.
    /// - Throws: `CancellationError()` if the task is cancelled when consuming or performing retries.
    /// - Returns: An `AnyAsyncSequence<T>`, which is essentially a stream of messages of the requested type from the consumer.
    public func consume<T: MassTransitMessage>(
        _: T.Type,
        messageExchange: String = String(describing: T.self),
        exchangeOptions: ExchangeOptions = .massTransitDefaults,
        routingKey: String = "",
        bindingOptions: BindingOptions = .init(),
        customMessageType: String? = nil
    ) async throws -> AnyAsyncSequence<T> {
        // Determine message type
        let messageType = customMessageType ?? messageExchange

        // We need to declare & bind an exchange for this message
        try await bindMessageExchange(messageExchange, exchangeOptions, routingKey, bindingOptions)

        // Create a stream and message handler
        let consumerStream = createMessageConsumer(T.self, messageType: messageType)

        return .init(
            consumerStream.compactMap { wrapper in
                self.logger.debug("Consumed message \(wrapper.message) from queue \(self.queueName)")
                return wrapper.message
            }
        )
    }

    /// Consume a specific message type from this consumer with attached `RequestContext`.
    ///
    /// This works the same as the regular `consume()` method, but returns a `RequestContext` for
    /// each message that can be used to `.respond` to the message that was received by the application.
    ///
    /// - Parameters:
    ///   - _: Type of the message to consume (example: `MyMessage.self`).
    ///   - messageExchange: The exchange name to use for this message. Defaults to the message type string but can be set to any custom value.
    ///   - exchangeOptions: Options for the message exchange. Defaults to `.massTransitDefaults` but can be customized as desired.
    ///   - routingKey: Optional routing key to use for the binding from the message exchange to the main consumer exchange.
    ///   - bindingOptions: Options for binding the message exchange to the main consumer exchange.
    ///   - customMessageType: Custom message type to use. This will not affect the message exchange name,
    ///     only the message type "urn" that is interpreted in the MassTransit wrapper.
    /// - Throws: `CancellationError()` if the task is cancelled when consuming or performing retries.
    /// - Returns: An `AnyAsyncSequence<RequestContext<T>>`, which is a stream of `RequestContext` containing the message received the consumer.
    public func consumeWithContext<T: MassTransitMessage>(
        _: T.Type,
        messageExchange: String = String(describing: T.self),
        exchangeOptions: ExchangeOptions = .massTransitDefaults,
        routingKey: String = "",
        bindingOptions: BindingOptions = .init(),
        customMessageType: String? = nil
    ) async throws -> AnyAsyncSequence<RequestContext<T>> {
        // Determine message type
        let messageType = customMessageType ?? messageExchange

        // We need to declare & bind an exchange for this message
        try await bindMessageExchange(messageExchange, exchangeOptions, routingKey, bindingOptions)

        // Create a stream and message handler
        let consumerStream = createMessageConsumer(T.self, messageType: messageType)

        return .init(
            consumerStream.compactMap { wrapper in
                self.logger.debug("Consumed message \(wrapper.message) from queue \(self.queueName)")

                // Create RequestContext from message
                return RequestContext(
                    connection: self.connection,
                    requestId: wrapper.requestId,
                    responseAddress: wrapper.responseAddress,
                    logger: self.logger,
                    message: wrapper.message
                )
            }
        )
    }

    /// Run the consumer.
    ///
    /// This is *REQUIRED* to run the RabbitMq consumer and process messages from the resulting
    /// stream. The message type in each message is checked and attempted to be routed to a different
    /// MassTransit consumer, otherwise an error is printed that an unknown message type was received.
    public func run() async throws {
        logger.info("Starting consumer on queue \(queueName)...")
        let consumer = configuration.createConsumer(using: connection, queueName, exchangeName, routingKey)
        let consumeStream = try await consumer.retryingConsumeBuffer(retryInterval: retryInterval)
        try await onConsumerReady?()
        isConsumerReady = true

        // Consume messages from the consumer
        for await buffer in consumeStream {
            process(buffer)
        }
    }

    private func process(_ buffer: ByteBuffer) {
        withConsumeSpan(self.queueName, .consume, self.routingKey) { span in
            logger.trace("Consumed buffer from \(queueName): \(String(buffer: buffer))")
            var handled = false

            do {
                // We parse the wrapper only to see what the messageTypes are
                let wrapper = try MassTransitWrapper(Wrapper.self, from: buffer)

                // Looking for matching consumers
                for messageType in wrapper.messageType.map({ $0.replacingOccurrences(of: "urn:message:", with: "") }) {
                    guard let handler = consumers[messageType] else {
                        continue
                    }

                    // If there is a matching type, try to process it
                    logger.trace("Message type associated for queue \(queueName): \(messageType)")
                    try handler(buffer)
                    handled = true
                }

                // If the message is not handled, print an error
                if !handled {
                    logger.error(
                        "Message of type(s) \(wrapper.messageType) from queue \(queueName) is missing a consumer!"
                    )

                    // TODO: This message should then be routed to a different error or unhandled queue
                }
            } catch {
                logger.error("Error in message consumed from \(queueName): \(error)")

                // TODO: We should route this to an error queue
            }
        }
    }
}
