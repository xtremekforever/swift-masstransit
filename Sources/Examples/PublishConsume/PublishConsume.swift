import ArgumentParser
import AsyncAlgorithms
import Foundation
import Logging
import MassTransit
import RabbitMq

@main
struct PublishConsume: AsyncParsableCommand {
    @Option
    var rabbitUrl: String = "amqp://guest:guest@localhost/%2F"

    @Option
    var logLevel: String = "info"

    @Option(help: "The interval at which to publish the test event in milliseconds.")
    var publishInterval: Int = 1000

    struct MyTestEvent: MassTransitMessage {
        let id: UUID
        let name: String
    }

    // Customizable log level
    func createLogger() -> Logger {
        var logger = Logger(label: "PublishConsume")
        logger.logLevel = Logger.Level(rawValue: logLevel) ?? .info
        return logger
    }

    mutating func run() async throws {
        let logger = createLogger()
        let rabbitMq = RetryingConnection(rabbitUrl, logger: logger)
        let massTransit = MassTransit(rabbitMq, logger: logger)

        try await withThrowingDiscardingTaskGroup { group in
            // Supervise RabbitMq connection
            group.addTask {
                await rabbitMq.run()
            }

            let publishInterval = self.publishInterval

            // Publish on an interval
            group.addTask {
                let timerSequence = AsyncTimerSequence(
                    interval: .milliseconds(publishInterval), clock: .continuous
                )
                for await _ in timerSequence.buffer(policy: .bufferingLatest(1)) {
                    let event = MyTestEvent(
                        id: UUID(),
                        name: "My Event!"
                    )
                    logger.info("Publishing event: \(event)")
                    try await massTransit.publish(event)
                }
            }

            // Consume events
            group.addTask {
                let events = try await massTransit.consume(MyTestEvent.self)
                for await event in events {
                    logger.info("Consumed event: \(event)")
                }
            }
        }
    }

}
