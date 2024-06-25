import AMQPClient
import Foundation
import Logging
import RabbitMq
import Tracing
import TracingOpenTelemetrySemanticConventions

public struct MassTransit: Sendable {
    private let rabbitMq: RabbitMq.Connectable
    private let logger: Logger

    init(
        _ rabbitMq: RabbitMq.Connectable,
        logger: Logger = Logger(label: "\(MassTransit.self)")
    ) {
        self.rabbitMq = rabbitMq
        self.logger = logger
    }

    public func publish<T: Codable>(_ value: T, exchangeName: String = "\(T.self)", routingKey: String = "")
        async throws
    {
        let connection = try await waitForConnection()
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
            try await publisher.publish(messageJson, routingKey: routingKey)
        }
    }

    public func consume<T: Codable>(
        _: T.Type, queueName: String = "\(T.self)-Consumer", exchangeName: String = "\(T.self)", routingKey: String = ""
    )
        async throws -> AnyAsyncSequence<T>
    {
        let connection = try await waitForConnection()
        let consumer = Consumer(
            connection, queueName, exchangeName, routingKey,
            exchangeOptions: ExchangeOptions(type: .fanout, durable: true),
            queueOptions: QueueOptions(autoDelete: true, durable: true)
        )

        // Consume messages with span tracing
        logger.info("Consuming messages of type \(T.self) on queue \(queueName)...")
        return AnyAsyncSequence<T>(
            try await consumer.consume().compactMap { message in
                return try withSpan("\(T.self) consume", ofKind: .consumer) { span in
                    // Decode from JSON
                    let decoder = JSONDecoder()
                    guard let data = message.data(using: .utf8),
                        let wrapper = try? decoder.decode(MassTransitWrapper<T>.self, from: data)
                    else {
                        throw MassTransitError.parsingError
                    }

                    // Return the message
                    return wrapper.message
                }
            }
        )
    }

    private func calculateTimeDifference(between now: DispatchTime, and start: DispatchTime) -> Double {
        if now < start {
            return 0.0
        }
        return Double(now.uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000_000
    }

    private func waitForConnection(timeout: Int = 30) async throws -> Connection {
        let start = DispatchTime.now()
        while !Task.isCancelled {
            if let connection = await rabbitMq.getConnection() {
                return connection
            }

            let elapsed = calculateTimeDifference(between: DispatchTime.now(), and: start)
            if elapsed > Double(timeout) {
                throw MassTransitError.brokerTimeout
            } else {
                try await Task.sleep(for: .milliseconds(10))
            }
        }

        throw CancellationError()
    }
}
