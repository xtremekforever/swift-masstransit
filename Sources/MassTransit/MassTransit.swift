import AMQPClient
import Foundation
import Logging
import NIOCore
import RabbitMq
import Tracing
import TracingOpenTelemetrySemanticConventions

public struct MassTransit: Sendable {
    public static let defaultRetryInterval = Duration.seconds(30)
    public static let defaultRequestTimeout = Duration.seconds(30)

    private let connection: Connection
    private let logger: Logger

    public init(
        _ connection: Connection,
        logger: Logger = Logger(label: "\(MassTransit.self)")
    ) {
        self.connection = connection
        self.logger = logger
    }

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
