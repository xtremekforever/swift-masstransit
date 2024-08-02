import AMQPClient
import Foundation
import Logging
import RabbitMq
import Tracing
import TracingOpenTelemetrySemanticConventions

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

    public func publish<T: Codable>(
        _ value: T,
        exchangeName: String = "\(T.self)",
        routingKey: String = "",
        timeout: Duration = .seconds(30)
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
        let encoder = JSONEncoder()
        guard let json = try? encoder.encode(wrapper),
            let messageJson = String(data: json, encoding: .utf8)
        else {
            throw MassTransitError.parsingError
        }

        // Publish message with span processor
        logger.info("Publishing message of type \(T.self) on exchange \(exchangeName)...")
        try await withSpan("\(T.self) publish", ofKind: .producer) { span in
            span.attributes.messaging.messageID = wrapper.messageId
            span.attributes.messaging.destination = exchangeName
            span.attributes.messaging.rabbitMQ.routingKey = routingKey
            span.attributes.messaging.system = "rabbitmq"
            try await publisher.retryingPublish(messageJson, routingKey: routingKey, retryInterval: timeout)
            logger.debug("Published message \(value) to exchange \(exchangeName)")
        }
    }

    public func consume<T: Codable>(
        _: T.Type,
        queueName: String = "\(T.self)-Consumer",
        exchangeName: String = "\(T.self)",
        routingKey: String = "",
        timeout: Duration = .seconds(30)
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
            try await consumer.retryingConsume(retryInterval: timeout).compactMap { message in
                return try withSpan("\(T.self) consume", ofKind: .consumer) { span in
                    // Decode from JSON
                    let decoder = JSONDecoder()
                    guard let data = message.data(using: .utf8),
                        let wrapper = try? decoder.decode(MassTransitWrapper<T>.self, from: data)
                    else {
                        throw MassTransitError.parsingError
                    }

                    logger.debug("Consumed message \(wrapper.message) from queue \(queueName)")

                    // Return the message
                    return wrapper.message
                }
            }
        )
    }
}
