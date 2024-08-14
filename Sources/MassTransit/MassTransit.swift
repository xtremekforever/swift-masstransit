import AMQPClient
import Foundation
import Logging
import RabbitMq
import Tracing
import TracingOpenTelemetrySemanticConventions

public let MassTransitDefaultRetryInterval = Duration.seconds(30)
public let MassTransitDefaultTimeout = Duration.seconds(30)

public struct MassTransit: Sendable {
    private let rabbitMq: RabbitMq.Connectable
    private let logger: Logger

    public init(
        _ rabbitMq: RabbitMq.Connectable,
        logger: Logger = Logger(label: "\(MassTransit.self)")
    ) {
        self.rabbitMq = rabbitMq
        self.logger = logger
    }

    public func publish<T: MassTransitMessage>(
        _ value: T,
        exchangeName: String = "\(T.self)",
        routingKey: String = "",
        retryInterval: Duration = MassTransitDefaultRetryInterval
    )
        async throws
    {
        let connection = try await rabbitMq.waitGetConnection()
        let publisher = Publisher(
            connection, exchangeName, exchangeOptions: ExchangeOptions(type: .fanout, durable: true)
        )

        // Create MassTransitWrapper to send the message
        let wrapper = MassTransitWrapper(
            messageId: UUID().uuidString,
            messageType: ["urn:message:\(exchangeName)"],
            message: value
        )

        // Encode to JSON
        let messageJson = try wrapper.jsonEncode()

        // Publish message with span processor
        logger.info("Publishing message of type \(T.self) on exchange \(exchangeName)...")
        try await withSpan("\(T.self) publish", ofKind: .producer) { span in
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
        queueName: String = "\(T.self)-Consumer",
        exchangeName: String = "\(T.self)",
        routingKey: String = "",
        retryInterval: Duration = MassTransitDefaultRetryInterval
    )
        async throws -> AnyAsyncSequence<T>
    {
        let connection = try await rabbitMq.waitGetConnection()
        let consumer = Consumer(
            connection, queueName, exchangeName, routingKey,
            exchangeOptions: ExchangeOptions(type: .fanout, durable: true),
            queueOptions: QueueOptions(autoDelete: true, durable: true),
            consumerOptions: ConsumerOptions(noAck: true)
        )

        // Consume messages with span tracing
        logger.info("Consuming messages of type \(T.self) on queue \(queueName)...")
        return AnyAsyncSequence<T>(
            try await consumer.retryingConsume(retryInterval: retryInterval).compactMap { message in
                return try withSpan("\(T.self) consume", ofKind: .consumer) { span in
                    let wrapper = try MassTransitWrapper(T.self, from: message)

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
        queueName: String = "\(T.self)-Consumer",
        exchangeName: String = "\(T.self)",
        routingKey: String = "",
        retryInterval: Duration = MassTransitDefaultRetryInterval
    )
        async throws -> AnyAsyncSequence<RequestContext<T>>
    {
        let connection = try await rabbitMq.waitGetConnection()
        let consumer = Consumer(
            connection, queueName, exchangeName, routingKey,
            exchangeOptions: ExchangeOptions(type: .fanout, durable: true),
            queueOptions: QueueOptions(autoDelete: true, durable: true),
            consumerOptions: ConsumerOptions(noAck: true)
        )

        // Consume messages with span tracing
        logger.info("Consuming messages of type \(T.self) on queue \(queueName)...")
        return AnyAsyncSequence<RequestContext<T>>(
            try await consumer.retryingConsume(retryInterval: retryInterval).compactMap { message in
                return try withSpan("\(T.self) consume", ofKind: .consumer) { span in
                    let wrapper = try MassTransitWrapper(T.self, from: message)

                    // Log!
                    logger.debug("Consumed message \(wrapper.message) from queue \(queueName)")

                    // Create ConsumeContext from message
                    return RequestContext(
                        connection: connection,
                        requestId: wrapper.requestId,
                        responseAddress: wrapper.responseAddress,
                        message: wrapper.message
                    )
                }
            }
        )
    }

    public func request<T: MassTransitMessage, TResponse: MassTransitMessage>(
        _ value: T,
        _: TResponse.Type,
        exchangeName: String = "\(T.self)",
        routingKey: String = "",
        timeout: Duration = MassTransitDefaultTimeout
    ) async throws -> TResponse {
        // Use task group to timeout request
        return try await withThrowingTaskGroup(of: TResponse.self) { group in
            group.addTask {
                return try await performRequest(value, TResponse.self, exchangeName, routingKey)
            }
            group.addTask {
                try await gracefulCancellableDelay(timeout: timeout)
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
        _ exchangeName: String = "\(T.self)",
        _ routingKey: String = ""
    ) async throws -> TResponse {
        let connection = try await rabbitMq.waitGetConnection()

        // Publisher is used to send the request
        let publisher: Publisher = Publisher(
            connection, exchangeName, exchangeOptions: ExchangeOptions(type: .fanout, durable: true)
        )

        // Consumer is used to get a response with a custom requestName and address provided
        let requestName = "\(ProcessInfo.processInfo.hostName)_\(getModuleName(self))_bus_\(randomString(length: 26))"
        let address = "\(await connection.getConnectionAddress())/\(requestName)?temporary=true"
        let consumer = Consumer(
            connection, requestName, requestName, routingKey,
            exchangeOptions: ExchangeOptions(type: .fanout, durable: false, autoDelete: true),
            queueOptions: QueueOptions(autoDelete: true, durable: false, args: ["x-expires": .int32(60000)]),
            consumerOptions: ConsumerOptions(noAck: true)
        )

        // Create MassTransitWrapper to send the request
        let request = MassTransitWrapper(
            messageId: UUID().uuidString,
            requestId: UUID().uuidString,
            sourceAddress: address,
            responseAddress: address,
            messageType: ["urn:message:\(exchangeName)"],
            message: value
        )

        // Start consuming before publishing request so we can get the response
        let responseStream = try await consumer.consume()

        // Send the request with a regular publish
        logger.info("Sending request of type \(T.self) to exchange \(exchangeName)...")
        let requestJson = try request.jsonEncode()
        try await withSpan("\(T.self) request", ofKind: .producer) { span in
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
