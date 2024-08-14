import AsyncAlgorithms
import Foundation
import Logging
import MassTransit

struct MyRequest: MassTransitMessage {
    let value: String
}

struct MyResponse: MassTransitMessage {
    let value: String
}

var logger = Logger(label: "RequestResponse")
//logger.logLevel = .debug
let rabbitMq = try SimpleRabbitMqConnector("amqp://guest:guest@localhost/%2F", logger: logger)
let massTransit = MassTransit(rabbitMq, logger: logger)

try await withThrowingDiscardingTaskGroup { group in
    // Supervise RabbitMq connection
    group.addTask {
        try await rabbitMq.run()
    }

    // This will request on an interval
    group.addTask {
        for await _ in AsyncTimerSequence(interval: .seconds(1), clock: .continuous) {
            do {
                let response = try await massTransit.request(
                    MyRequest(value: "please give me something"), MyResponse.self,
                    timeout: .seconds(15))
                await logger.info("Got response: \(response)")
            } catch {
                await logger.error("Request failed: \(error)")
            }
        }
    }

    // This will respond to requests
    group.addTask {
        let requestStream = try await massTransit.consumeWithContext(
            MyRequest.self, queueName: "RequestResponse-MyRequestConsumer")
        for await request in requestStream {
            let message = request.message
            await logger.info("Got request: \(message)")

            let response = MyResponse(value: message.value)
            await logger.info("Sending response: \(response)")
            try await request.respond(response)
        }
    }
}
