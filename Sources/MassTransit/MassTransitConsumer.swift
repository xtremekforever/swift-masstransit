import Logging
import NIOCore
import RabbitMq
import Tracing

public actor MassTransitConsumer {
    let connection: Connection
    let queueName: String
    let exchangeName: String
    let configuration: MassTransitConsumerConfiguration
    let routingKey: String
    let retryInterval: Duration
    let logger: Logger

    private var consumers: [String: (ByteBuffer) throws -> Void] = [:]

    public init(
        using connection: Connection,
        queueName: String,
        exchangeName: String,
        routingKey: String = "",
        configuration: MassTransitConsumerConfiguration = .init(),
        retryInterval: Duration = MassTransit.defaultRetryInterval,
        logger: Logger = .init(label: .init(describing: MassTransitConsumer.self))
    ) {
        self.connection = connection
        self.queueName = queueName
        self.exchangeName = exchangeName
        self.routingKey = routingKey
        self.configuration = configuration
        self.retryInterval = retryInterval
        self.logger = logger
    }

    private func createMessageConsumer<T: MassTransitMessage>(
        _: T.Type, messageType: String
    ) -> AnyAsyncSequence<MassTransitWrapper<T>> {
        // Create a stream + continuation
        logger.info("Consuming messages of type \(messageType) on queue \(queueName)...")
        let (stream, continuation) = AsyncStream.makeStream(of: MassTransitWrapper<T>.self)

        // Create a message handler
        let urn = urn(from: messageType)
        consumers[urn] = { buffer in
            let wrapper = try MassTransitWrapper(T.self, from: buffer)
            continuation.yield(wrapper)
        }

        // Handle termination
        continuation.onTermination = { _ in
            Task { await self.removeConsumer(messageType: urn) }
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
        consumers.removeValue(forKey: messageType)
    }

    private func bindMessageExchange(
        _ messageExchange: String,
        _ exchangeOptions: ExchangeOptions,
        _ routingKey: String,
        _ bindingOptions: BindingOptions
    ) async throws {
        // TODO: Retry pattern here
        while await !connection.isConnected && !Task.isCancelledOrShuttingDown {
            await connection.waitForConnection(timeout: retryInterval)
        }
        let channel = try await connection.getChannel()

        // Bind messageExchange to consumer exchange
        try await channel?.exchangeDeclare(messageExchange, exchangeOptions, logger)

        // Bind messageExchange to consumer exchange
        try await channel?.exchangeBind(
            exchangeName, messageExchange, routingKey, bindingOptions, logger
        )
    }

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

        return .init(consumerStream.compactMap { $0.message })
    }

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

    public func run() async throws {
        logger.info("Starting consumer on queue \(queueName)...")
        let consumer = configuration.createConsumer(using: connection, queueName, exchangeName, routingKey)
        let consumeStream = try await consumer.retryingConsumeBuffer(retryInterval: retryInterval)

        // Consume messages from the consumer
        for await buffer in consumeStream {
            process(buffer)
        }
    }

    private func process(_ buffer: ByteBuffer) {
        withSpan("\(queueName) consume", ofKind: .consumer) { span in
            logger.trace("Consumed buffer from \(queueName): \(String(buffer: buffer))")
            var handled = false

            do {
                // We parse the wrapper only to see what the messageTypes are
                let wrapper = try MassTransitWrapper(Wrapper.self, from: buffer)

                // Looking for matching consumers
                for messageType in wrapper.messageType {
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
