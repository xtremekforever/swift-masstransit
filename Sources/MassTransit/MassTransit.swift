import AMQPClient
import Foundation
import Logging
import NIOCore
import RabbitMq
import Tracing
import TracingOpenTelemetrySemanticConventions

/// Create the MassTransit connection wrapper.
/// 
/// This is a simple wrapper around a RabbitMq `Connection` that provides MassTransit functions such
/// as send, publish, consume, and request/response. This provides the simplest possible functionality
/// for compatibility with the C# MassTransit library while making the API simple to use.
public struct MassTransit: Sendable {
    /// Default retry interval for `send`/`publish`/`consume`.
    public static let defaultRetryInterval = Duration.seconds(30)
    /// Default response timeout for a `request` message.
    public static let defaultRequestTimeout = Duration.seconds(30)

    private let connection: Connection
    private let logger: Logger

    /// Create the MassTransit connection wrapper.
    /// 
    /// - Parameters:
    ///   - connection: A RabbitMq `Connection` that should be connected to the broker and managed externally.
    ///   - logger: Logger to use for this instance.
    public init(
        _ connection: Connection,
        logger: Logger = Logger(label: "\(MassTransit.self)")
    ) {
        self.connection = connection
        self.logger = logger
    }

    /// Send a message to the broker on a given exchange.
    /// 
    /// This provides similar functionality to `publish`, but does not retry on failure, which is
    /// useful for applications that need to send a message without blocking on failure.
    ///
    /// - Parameters:
    ///   - value: The `MassTransitMessage`-conforming message to send.
    ///   - exchangeName: The name of the exchange to publish to. Defaults to the name of the message.
    ///   - routingKey: Optional routing key to use to publish to the exchange.
    ///   - configuration: Configuration to use for publishing, which contains exchange and publisher options.
    ///   - customMessageType: Custom message type to use. This will not affect the exchange name,
    ///     only the message type "urn" that is sent in the MassTransit message wrapper.
    /// - Throws: Error if unable to send the message.
    public func send<T: MassTransitMessage>(
        _ value: T,
        exchangeName: String = String(describing: T.self),
        routingKey: String = "",
        configuration: MassTransitPublisherConfiguration = .init(),
        customMessageType: String? = nil
    ) async throws {
        let publisher = configuration.createPublisher(using: connection, exchangeName)
        let messageType = customMessageType ?? exchangeName

        // Create MassTransitWrapper to send the message
        let wrapper = MassTransitWrapper<T>.create(using: value, messageType: messageType)
        logger.trace("Wrapper for send to \(exchangeName): \(wrapper)")

        // Encode to JSON
        let messageJson = try wrapper.jsonEncode()
        logger.trace("Message JSON to send: \(String(buffer: messageJson))")

        // Publish message with span processor
        logger.debug("Sending message of type \(messageType) on exchange \(exchangeName)...")
        try await withPublishSpan(wrapper.messageId, messageType, .send, exchangeName, routingKey) {
            try await publisher.publish(messageJson, routingKey: routingKey)
            logger.debug("Sent message \(value) to exchange \(exchangeName)")
        }
    }

    /// Publish a message to the broker on a given exchange.
    /// 
    /// This method will block until it is able to publish or is cancelled.
    /// 
    /// - Parameters:
    ///   - value: The `MassTransitMessage`-conforming message to send.
    ///   - exchangeName: The name of the exchange to publish to. Defaults to the name of the message.
    ///   - routingKey: Optional routing key to use to publish to the exchange.
    ///   - configuration: Configuration to use for publishing, which contains exchange and publisher options.
    ///   - customMessageType: Custom message type to use. This will not affect the exchange name,
    ///     only the message type "urn" that is sent in the MassTransit message wrapper.
    ///   - retryInterval: The retry interval to use if unable to publish. This will retry forever unless cancelled.
    /// - Throws: `CancellationError()` if the task is cancelled when publishing or retrying.
    public func publish<T: MassTransitMessage>(
        _ value: T,
        exchangeName: String = String(describing: T.self),
        routingKey: String = "",
        configuration: MassTransitPublisherConfiguration = .init(),
        customMessageType: String? = nil,
        retryInterval: Duration = defaultRetryInterval
    ) async throws {
        let publisher = configuration.createPublisher(using: connection, exchangeName)
        let messageType = customMessageType ?? exchangeName

        // Create MassTransitWrapper to send the message
        let wrapper = MassTransitWrapper<T>.create(using: value, messageType: messageType)
        logger.trace("Wrapper for publish to \(exchangeName): \(wrapper)")

        // Encode to JSON
        let messageJson = try wrapper.jsonEncode()
        logger.trace("Message JSON to publish: \(String(buffer: messageJson))")

        // Publish message with span processor
        logger.debug("Publishing message of type \(messageType) on exchange \(exchangeName)...")
        try await withPublishSpan(wrapper.messageId, messageType, .publish, exchangeName, routingKey) {
            try await publisher.retryingPublish(messageJson, routingKey: routingKey, retryInterval: retryInterval)
            logger.debug("Published message \(value) to exchange \(exchangeName)")
        }
    }

    private func processConsumeBuffer<T: MassTransitMessage>(
        _ buffer: ByteBuffer,
        as: T.Type,
        _ consumeKind: ConsumeKind,
        _ queueName: String,
        _ routingKey: String
    ) throws -> MassTransitWrapper<T>? {
        logger.trace("Consumed buffer from \(queueName): \(String(buffer: buffer))")

        return withConsumeSpan(queueName, consumeKind, routingKey) { span in
            do {
                let wrapper = try MassTransitWrapper(T.self, from: buffer)
                logger.trace("Decoded buffer from \(queueName) to wrapper: \(wrapper)")
                span.attributes.messaging.messageID = wrapper.messageId
                return wrapper
            } catch {
                logger.error("Error in message consumed from \(queueName): \(error)")

                // TODO: We should route this to an error queue
            }

            return nil
        }
    }

    /// Consume a stream of a message type from a given queue bound to an exchange.
    /// 
    /// This method will block until it is able to consume or is cancelled.
    /// 
    /// - Parameters:
    ///   - _: The `MassTransitMessage`-conforming message to consume.
    ///   - queueName: The name of the queue for this consumer. Defaults to the message type + "-Consumer".
    ///   - exchangeName: The name of the exchange to bind to the queue. Defaults to the name of the message.
    ///   - routingKey: Optional routing key to use for binding the exchange to the queue.
    ///   - configuration: Configuration to use for consuming, which contains queue, exchange, and consumer options.
    ///   - customMessageType: Custom message type to use. This will not affect the exchange name.
    ///   - retryInterval: The retry interval to use if unable to consume. This will retry forever unless cancelled.
    /// - Throws: `CancellationError()` if the task is cancelled when consuming or retrying.
    /// - Returns: An `AnyAsyncSequence<T>`, which is essentially a stream of messages of the requested type from the consumer.
    public func consume<T: MassTransitMessage>(
        _: T.Type,
        queueName: String = "\(String(describing: T.self))-Consumer",
        exchangeName: String = String(describing: T.self),
        routingKey: String = "",
        configuration: MassTransitConsumerConfiguration = .init(),
        customMessageType: String? = nil,
        retryInterval: Duration = defaultRetryInterval
    ) async throws -> AnyAsyncSequence<T> {
        let consumer = configuration.createConsumer(using: connection, queueName, exchangeName, routingKey)

        // Determine message type
        let messageType = customMessageType ?? exchangeName

        // Consume messages with span tracing
        logger.debug("Consuming messages of type \(messageType) on queue \(queueName)...")
        let consumeStream = try await consumer.retryingConsumeBuffer(retryInterval: retryInterval)

        return .init(
            consumeStream.compactMap { buffer in
                let wrapper = try processConsumeBuffer(buffer, as: T.self, .consume, queueName, routingKey)
                if let wrapper {
                    logger.debug("Consumed message \(wrapper.message) from queue \(queueName)")
                }
                return wrapper?.message
            }
        )
    }

    /// Consume a stream of `RequestContext` for a specific message type.
    /// 
    /// This is the same as the regular `consume()` method, but adds a `RequestContext` wrapper that
    /// allows the application to `.respond()` to the requests that may come in.
    /// 
    /// - Parameters:
    ///   - _: The `MassTransitMessage`-conforming message to consume.
    ///   - queueName: The name of the queue for this consumer. Defaults to the message type + "-Consumer".
    ///   - exchangeName: The name of the exchange to bind to the queue. Defaults to the name of the message.
    ///   - routingKey: Optional routing key to use for binding the exchange to the queue.
    ///   - configuration: Configuration to use for consuming, which contains queue, exchange, and consumer options.
    ///   - customMessageType: Custom message type to use. This will not affect the exchange name.
    ///   - retryInterval: The retry interval to use if unable to consume. This will retry forever unless cancelled.
    /// - Throws: `CancellationError()` if the task is cancelled when consuming or retrying.
    /// - Returns: An `AnyAsyncSequence<RequestContext<T>>`, which is a stream of `RequestContext` containing the message received the consumer.
    public func consumeWithContext<T: MassTransitMessage>(
        _: T.Type,
        queueName: String = "\(String(describing: T.self))-Consumer",
        exchangeName: String = String(describing: T.self),
        routingKey: String = "",
        configuration: MassTransitConsumerConfiguration = .init(),
        customMessageType: String? = nil,
        retryInterval: Duration = defaultRetryInterval
    ) async throws -> AnyAsyncSequence<RequestContext<T>> {
        let consumer = configuration.createConsumer(using: connection, queueName, exchangeName, routingKey)

        // Determine message type
        let messageType = customMessageType ?? exchangeName

        // Consume messages with span tracing
        logger.debug("Consuming messages of type \(messageType) on queue \(queueName)...")
        let consumeStream = try await consumer.retryingConsumeBuffer(retryInterval: retryInterval)

        return .init(
            consumeStream.compactMap { buffer in
                let wrapper = try processConsumeBuffer(buffer, as: T.self, .consume, queueName, routingKey)
                if let wrapper {
                    logger.debug("Consumed message \(wrapper.message) from queue \(queueName)")

                    // Create ConsumeContext from message
                    return RequestContext(
                        connection: connection,
                        requestId: wrapper.requestId,
                        responseAddress: wrapper.responseAddress,
                        logger: logger,
                        message: wrapper.message
                    )
                }
                return nil
            }
        )
    }

    /// Perform a request using a specific message type, expecting another message type as a response.
    /// 
    /// This method implements the most basic subset of the C# MassTransit library's Request/Response
    /// functionality by publishing a request, then awaiting a response on a specific exchange.
    /// 
    /// - Parameters:
    ///   - value: The `MassTransitMessage`-conforming request to publish.
    ///   - _: The type of the `MassTransitMessage`-conforming response we expect.
    ///   - exchangeName: The name of the exchange to publish the request to.
    ///   - routingKey: Optional routing key to use for the request.
    ///   - timeout: Timeout for the response to the request to arrive.
    ///   - configuration: Configuration to use for the publisher, which contains exchange and publisher options.
    ///   - customMessageType: Custom message type to use. This will not affect the exchange name,
    ///     only the message type "urn" that is sent in the MassTransit message wrapper.
    /// - Throws:
    ///     - 'MassTransitError.timeout' if no response is received within the timeout.
    ///     - `CancellationError()` if the task is cancelled while publishing, consuming, or waiting for a response.
    /// - Returns: 
    public func request<T: MassTransitMessage, TResponse: MassTransitMessage>(
        _ value: T,
        _: TResponse.Type,
        exchangeName: String = String(describing: T.self),
        routingKey: String = "",
        timeout: Duration = defaultRequestTimeout,
        configuration: MassTransitPublisherConfiguration = .init(),
        customMessageType: String? = nil
    ) async throws -> TResponse {
        // Use task group to timeout request
        return try await withThrowingTaskGroup(of: TResponse.self) { group in
            group.addTask {
                return try await performRequest(
                    value, TResponse.self, exchangeName, routingKey, configuration, customMessageType
                )
            }
            group.addTask {
                await gracefulCancellableDelay(timeout)
                throw MassTransitError.timeout
            }

            let result = try await group.next()
            group.cancelAll()
            return result!
        }
    }

    private func performRequest<T: MassTransitMessage, TResponse: MassTransitMessage>(
        _ value: T,
        _: TResponse.Type,
        _ exchangeName: String,
        _ routingKey: String,
        _ configuration: MassTransitPublisherConfiguration,
        _ customMessageType: String?
    ) async throws -> TResponse {
        // Publisher is used to send the request
        let publisher = configuration.createPublisher(using: connection, exchangeName)

        // Consumer is used to get a response with a custom requestName and address provided
        let requestName = "\(ProcessInfo.processInfo.hostName)_\(getModuleName(self))_bus_\(randomString(length: 26))"
        let address = "\(await connection.getConnectionAddress())/\(requestName)?temporary=true"
        let consumer = Consumer(
            connection, requestName, requestName, routingKey,
            exchangeOptions: ExchangeOptions(type: .fanout, durable: false, autoDelete: true),
            queueOptions: QueueOptions(autoDelete: true, durable: false, args: ["x-expires": .int32(60000)]),
            consumerOptions: ConsumerOptions(noAck: true)
        )

        let messageType = customMessageType ?? exchangeName

        // Create MassTransitWrapper to send the request
        var request = MassTransitWrapper<T>.create(using: value, messageType: messageType)
        request.requestId = UUID().uuidString
        request.sourceAddress = address
        request.responseAddress = address
        logger.trace("Wrapper for request to \(exchangeName): \(request)")

        // Start consuming before publishing request so we can get the response
        let responseStream = try await consumer.consumeBuffer()

        // Send the request with a regular publish
        let requestJson = try request.jsonEncode()
        logger.trace("Request JSON: \(String(buffer: requestJson))")

        logger.info("Sending request of type \(messageType) to exchange \(exchangeName)...")
        try await withPublishSpan(request.messageId, messageType, .request, exchangeName, routingKey) {
            try await publisher.publish(requestJson, routingKey: routingKey)
        }

        logger.debug("Waiting for response of type \(TResponse.self) on queue \(requestName)...")
        for await response in responseStream {
            let wrapper = try processConsumeBuffer(response, as: TResponse.self, .response, requestName, routingKey)
            if let wrapper {
                logger.debug("Consumed response \(wrapper.message) from queue \(requestName)")
                return wrapper.message
            }
        }
        throw MassTransitError.timeout
    }
}
