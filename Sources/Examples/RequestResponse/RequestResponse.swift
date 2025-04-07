import ArgumentParser
import AsyncAlgorithms
import Foundation
import Logging
import MassTransit
import RabbitMq

@main
struct RequestResponse: AsyncParsableCommand {
    @Option
    var rabbitUrl: String = "amqp://guest:guest@localhost/%2F"

    @Option
    var logLevel: String = "info"

    @Option(help: "The interval at which to send requests in milliseconds.")
    var requestInterval: Int = 1000

    struct MyRequest: MassTransitMessage {
        let value: String
    }

    struct MyResponse: MassTransitMessage {
        let value: String
    }

    // Customizable log level
    func createLogger() -> Logger {
        var logger = Logger(label: "RequestResponse")
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

            let requestInterval = self.requestInterval

            // This will request on an interval
            group.addTask {
                let timerSequence = AsyncTimerSequence(interval: .milliseconds(requestInterval), clock: .continuous)
                for await _ in timerSequence.buffer(policy: .bufferingLatest(1)) {
                    do {
                        let response = try await massTransit.request(
                            MyRequest(value: "please give me something"), MyResponse.self,
                            timeout: .seconds(15))
                        logger.info("Got response: \(response)")
                    } catch {
                        logger.error("Request failed: \(error)")
                    }
                }
            }

            // This will respond to requests
            group.addTask {
                let requestStream = try await massTransit.consumeWithContext(
                    MyRequest.self, queueName: "RequestResponse-MyRequestConsumer")

                for await request in requestStream {
                    let message = request.message
                    logger.info("Got request: \(message)")

                    let response = MyResponse(value: message.value)
                    logger.info("Sending response: \(response)")
                    try await request.respond(response)
                }
            }
        }

    }
}
