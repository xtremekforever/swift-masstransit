import ArgumentParser
import AsyncAlgorithms
import Foundation
import Logging
import MassTransit
import RabbitMq

// This example demonstrates consuming multiple message types from a single `MassTransitConsumer`

@main
struct MultiMessageConsumer: AsyncParsableCommand {
    @Option
    var rabbitUrl: String = "amqp://guest:guest@localhost/%2F"

    @Option
    var logLevel: String = "info"

    struct Event1: MassTransitMessage {
        let date: Date
        let value: String
    }

    struct Event2: MassTransitMessage {
        let id: UUID
        let value: Int
    }

    // Customizable log level
    func createLogger() -> Logger {
        var logger = Logger(label: "MultiMessageConsumer")
        logger.logLevel = Logger.Level(rawValue: logLevel) ?? .info
        return logger
    }

    mutating func run() async throws {
        let logger = createLogger()
        let rabbitMq = RetryingConnection(rabbitUrl, logger: logger)
        let massTransit = MassTransit(rabbitMq, logger: logger)

        // These options are to be used by the publisher and consumer for the message exchanges
        let exchangeOptions = ExchangeOptions(durable: false, autoDelete: true)

        let consumer = MassTransitConsumer(
            using: rabbitMq, queueName: "MultiMessageConsumer", exchangeName: "MultiMessageExchange",
            configuration: .init(exchangeOptions: exchangeOptions),
            logger: logger
        )

        try await withThrowingDiscardingTaskGroup { group in
            // Supervise RabbitMq connection
            group.addTask { await rabbitMq.run() }

            // Run the consumer
            group.addTask { try await consumer.run() }

            // Consume Event1 messages
            group.addTask {
                let events = try await consumer.consume(
                    Event1.self,
                    exchangeOptions: exchangeOptions
                )
                for await event in events {
                    logger.info("Consumed Event1: \(event)")
                }
            }

            // Consume Event2 messages
            group.addTask {
                let events = try await consumer.consume(
                    Event2.self,
                    exchangeOptions: exchangeOptions
                )
                for await event in events {
                    logger.info("Consumed Event2: \(event)")
                }
            }

            // Publish a random event after a random timeout
            group.addTask {
                while !Task.isCancelledOrShuttingDown {
                    // Random sleep duration
                    let sleepDuration = Int.random(in: 1..<6)
                    try await Task.sleep(for: .seconds(sleepDuration))

                    let eventType = Int.random(in: 1..<3)
                    switch eventType {
                    // Publish Event1
                    case 1:
                        let event = Event1(
                            date: Date.now,
                            value: "Slept for \(sleepDuration) seconds!"
                        )
                        logger.info("Publishing Event1: \(event)")
                        try await massTransit.publish(event, configuration: .init(exchangeOptions: exchangeOptions))
                    // Publish Event2
                    case 2:
                        let event = Event2(
                            id: UUID(),
                            value: sleepDuration
                        )
                        logger.info("Publishing Event2: \(event)")
                        try await massTransit.publish(event, configuration: .init(exchangeOptions: exchangeOptions))
                    default:
                        continue
                    }
                }
            }
        }
    }
}
