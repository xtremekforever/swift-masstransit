import AMQPClient
import Foundation
import Logging
import RabbitMq
import Tracing
import TracingOpenTelemetrySemanticConventions

public let MassTransitDefaultRetryInterval = Duration.seconds(30)
public let MassTransitDefaultTimeout = Duration.seconds(30)

public struct MassTransit: Sendable {
    private let rabbitMq: Connection
    private let logger: Logger

    public init(
        _ rabbitMq: Connection,
        logger: Logger = Logger(label: "\(MassTransit.self)")
    ) {
        self.rabbitMq = rabbitMq
        self.logger = logger
    }

    public func send<T: MassTransitMessage>(
        _ value: T,
        exchangeName: String = String(describing: T.self),
        routingKey: String = "",
        configuration: MassTransitPublisherConfiguration = .init(),
        customMessageType: String? = nil
    ) async throws {
        let publisher = configuration.createPublisher(using: rabbitMq, exchangeName)
        let messageType = customMessageType ?? exchangeName

        // Create MassTransitWrapper to send the message
        let wrapper = MassTransitWrapper<T>.create(using: value, urn: messageType)
        logger.trace("Wrapper for send: \(wrapper)")

        // Encode to JSON
        let messageJson = try wrapper.jsonEncode()

        // Publish message with span processor
        logger.debug("Sending message of type \(messageType) on exchange \(exchangeName)...")
        try await withSpan("\(messageType) send", ofKind: .producer) { span in
            span.attributes.messaging.messageID = wrapper.messageId
            span.attributes.messaging.destination = exchangeName
            span.attributes.messaging.rabbitMQ.routingKey = routingKey
            span.attributes.messaging.system = "rabbitmq"
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
        retryInterval: Duration = MassTransitDefaultRetryInterval
    ) async throws {
        let publisher = configuration.createPublisher(using: rabbitMq, exchangeName)
        let messageType = customMessageType ?? exchangeName

        // Create MassTransitWrapper to send the message
        let wrapper = MassTransitWrapper<T>.create(using: value, urn: messageType)
        logger.trace("Wrapper for publish: \(wrapper)")

        // Encode to JSON
        let messageJson = try wrapper.jsonEncode()

        // Publish message with span processor
        logger.info("Publishing message of type \(messageType) on exchange \(exchangeName)...")
        try await withSpan("\(messageType) publish", ofKind: .producer) { span in
            span.attributes.messaging.messageID = wrapper.messageId
            span.attributes.messaging.destination = exchangeName
            span.attributes.messaging.rabbitMQ.routingKey = routingKey
            span.attributes.messaging.system = "rabbitmq"
            try await publisher.retryingPublish(messageJson, routingKey: routingKey, retryInterval: retryInterval)
            logger.debug("Published message \(value) to exchange \(exchangeName)")
        }
    }

    public func consume<T: MassTransitMessage>(
        _: T.Type,
        queueName: String = "\(String(describing: T.self))-Consumer",
        exchangeName: String = String(describing: T.self),
        routingKey: String = "",
        configuration: MassTransitConsumerConfiguration = .init(),
        retryInterval: Duration = MassTransitDefaultRetryInterval
    ) async throws -> AnyAsyncSequence<T> {
        let consumer = configuration.createConsumer(using: rabbitMq, queueName, exchangeName, routingKey)

        // Consume messages with span tracing
        logger.info("Consuming messages of type \(T.self) on queue \(queueName)...")
        let consumeStream = try await consumer.retryingConsumeBuffer(retryInterval: retryInterval)
        return AnyAsyncSequence<T>(
            consumeStream.compactMap { buffer in
                return try withSpan("\(T.self) consume", ofKind: .consumer) { span in
                    logger.trace("Received buffer: \(String(buffer: buffer))")

                    let wrapper = try MassTransitWrapper(T.self, from: buffer)
                    logger.trace("Wrapper consumed: \(wrapper)")

                    // Log!
                    logger.debug("Consumed message \(wrapper.message) from queue \(queueName)")

                    // Return the message
                    return wrapper.message
                }
            }
        )
    }

    public func consumeWithContext<T: MassTransitMessage>(
        _: T.Type,
        queueName: String = "\(String(describing: T.self))-Consumer",
        exchangeName: String = String(describing: T.self),
        routingKey: String = "",
        configuration: MassTransitConsumerConfiguration = .init(),
        retryInterval: Duration = MassTransitDefaultRetryInterval
    ) async throws -> AnyAsyncSequence<RequestContext<T>> {
        let consumer = configuration.createConsumer(using: rabbitMq, queueName, exchangeName, routingKey)

        // Consume messages with span tracing
        logger.info("Consuming messages of type \(T.self) on queue \(queueName)...")
        let consumeStream = try await consumer.retryingConsumeBuffer(retryInterval: retryInterval)
        return AnyAsyncSequence<RequestContext<T>>(
            consumeStream.compactMap { buffer in
                return try withSpan("\(T.self) consume", ofKind: .consumer) { span in
                    let wrapper = try MassTransitWrapper(T.self, from: buffer)
                    logger.trace("Wrapper consumed: \(wrapper)")

                    // Log!
                    logger.debug("Consumed message \(wrapper.message) from queue \(queueName)")

                    // Create ConsumeContext from message
                    return RequestContext(
                        connection: rabbitMq,
                        requestId: wrapper.requestId,
                        responseAddress: wrapper.responseAddress,
                        logger: logger,
                        message: wrapper.message
                    )
                }
            }
        )
    }

    public func request<T: MassTransitMessage, TResponse: MassTransitMessage>(
        _ value: T,
        _: TResponse.Type,
        exchangeName: String = String(describing: T.self),
        routingKey: String = "",
        timeout: Duration = MassTransitDefaultTimeout,
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
        let publisher = configuration.createPublisher(using: rabbitMq, exchangeName)

        // Consumer is used to get a response with a custom requestName and address provided
        let requestName = "\(ProcessInfo.processInfo.hostName)_\(getModuleName(self))_bus_\(randomString(length: 26))"
        let address = "\(await rabbitMq.getConnectionAddress())/\(requestName)?temporary=true"
        let consumer = Consumer(
            rabbitMq, requestName, requestName, routingKey,
            exchangeOptions: ExchangeOptions(type: .fanout, durable: false, autoDelete: true),
            queueOptions: QueueOptions(autoDelete: true, durable: false, args: ["x-expires": .int32(60000)]),
            consumerOptions: ConsumerOptions(noAck: true)
        )

        let messageType = customMessageType ?? exchangeName

        // Create MassTransitWrapper to send the request
        var request = MassTransitWrapper<T>.create(using: value, urn: messageType)
        request.requestId = UUID().uuidString
        request.sourceAddress = address
        request.responseAddress = address
        logger.trace("Wrapper for request: \(request)")

        // Start consuming before publishing request so we can get the response
        let responseStream = try await consumer.consumeBuffer()

        // Send the request with a regular publish
        logger.info("Sending request of type \(messageType) to exchange \(exchangeName)...")
        let requestJson = try request.jsonEncode()
        try await withSpan("\(messageType) request", ofKind: .producer) { span in
            span.attributes.messaging.messageID = request.messageId
            span.attributes.messaging.destination = exchangeName
            span.attributes.messaging.rabbitMQ.routingKey = routingKey
            span.attributes.messaging.system = "rabbitmq"
            try await publisher.publish(requestJson, routingKey: routingKey)
            logger.debug("Sent request \(value) to exchange \(exchangeName)")
        }

        logger.debug("Waiting for response of type \(TResponse.self) on queue \(requestName)...")
        for await responseJson in responseStream {
            let response = try MassTransitWrapper(TResponse.self, from: responseJson)
            logger.debug("Received response \(response.message) from queue \(requestName)")
            return response.message
        }
        throw MassTransitError.timeout
    }
}
