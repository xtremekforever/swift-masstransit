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

    let connection: Connection
    let logger: Logger

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
        let logger = self.logger.withMetadata([
            "exchangeName": .string(exchangeName),
            "routingKey": .string(routingKey),
            "messageType": .string(messageType),
        ])

        // Create MassTransitWrapper to send the message
        let wrapper = MassTransitWrapper<T>.create(using: value, messageType: messageType)
        wrapper.logAsTrace(using: logger)

        // Encode to JSON
        let messageJson = try wrapper.jsonEncode()
        messageJson.logJsonAsTrace(using: logger)

        // Publish message with span processor
        logger.debug("Sending message to exchange...")
        try await withPublishSpan(wrapper.messageId, messageType, .send, exchangeName, routingKey) {
            try await publisher.publish(messageJson, routingKey: routingKey)
            logger.trace("Successfully sent message to exchange")
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
        let logger = self.logger.withMetadata([
            "exchangeName": .string(exchangeName),
            "routingKey": .string(routingKey),
            "messageType": .string(messageType),
        ])

        // Create MassTransitWrapper to send the message
        let wrapper = MassTransitWrapper<T>.create(using: value, messageType: messageType)
        wrapper.logAsTrace(using: logger)

        // Encode to JSON
        let messageJson = try wrapper.jsonEncode()
        messageJson.logJsonAsTrace(using: logger)

        // Publish message with span processor
        logger.debug("Publishing message to exchange...")
        try await withPublishSpan(wrapper.messageId, messageType, .publish, exchangeName, routingKey) {
            try await publisher.retryingPublish(messageJson, routingKey: routingKey, retryInterval: retryInterval)
            logger.trace("Successfully published message to exchange")
        }
    }

    func processConsumeBuffer<T: MassTransitMessage>(
        _ buffer: ByteBuffer,
        as: T.Type,
        _ consumeKind: ConsumeKind,
        _ queueName: String,
        _ routingKey: String
    ) throws -> MassTransitWrapper<T>? {
        let logger = logger.withMetadata([
            "queueName": .string(queueName),
            "routingKey": .string(routingKey),
        ])

        // Trace logging for buffer
        buffer.logJsonAsTrace(using: logger)

        return withConsumeSpan(queueName, consumeKind, routingKey) { span in
            do {
                let wrapper = try MassTransitWrapper(T.self, from: buffer)
                wrapper.logAsTrace(using: logger)
                span.attributes.messaging.messageID = wrapper.messageId
                return wrapper
            } catch {
                logger.error("Error decoding message consumed from queue", metadata: ["error": .string("\(error)")])
                span.recordError(error)

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
        let logger = logger.withMetadata([
            "messageType": .string(messageType),
            "queueName": .string(queueName),
            "exchangeName": .string(exchangeName),
            "routingKey": .string(routingKey),
        ])

        // Consume messages with span tracing
        logger.debug("Starting consuming of messages from queue...")
        let consumeStream = try await consumer.retryingConsumeBuffer(retryInterval: retryInterval)

        return .init(
            consumeStream.compactMap { buffer in
                logger.debug("Consumed message from queue")
                let wrapper = try processConsumeBuffer(buffer, as: T.self, .consume, queueName, routingKey)
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
        let logger = logger.withMetadata([
            "messageType": .string(messageType),
            "queueName": .string(queueName),
            "exchangeName": .string(exchangeName),
            "routingKey": .string(routingKey),
        ])

        // Consume messages with span tracing
        logger.debug("Starting consuming of messages from queue...")
        let consumeStream = try await consumer.retryingConsumeBuffer(retryInterval: retryInterval)

        return .init(
            consumeStream.compactMap { buffer in
                logger.debug("Consumed message from queue")
                let wrapper = try processConsumeBuffer(buffer, as: T.self, .consume, queueName, routingKey)
                if let wrapper {
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
}
