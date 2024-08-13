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
logger.logLevel = .debug
let rabbitMq = try SimpleRabbitMqConnector("amqp://guest:guest@localhost/%2F", logger: logger)
let massTransit = MassTransit(rabbitMq, logger: logger)

try await withThrowingDiscardingTaskGroup { group in
    // Supervise RabbitMq connection
    group.addTask {
        try await rabbitMq.run()
    }

    for await _ in AsyncTimerSequence(interval: .seconds(1), clock: .continuous) {
        do {
            let response = try await massTransit.request(
                MyRequest(value: "please give me something"), MyResponse.self,
                exchangeName: "masstransit_request_response.Contracts:MyRequest",
                timeout: .seconds(15))
            logger.info("Got response: \(response)")
        } catch {
            logger.error("Request failed: \(error)")
        }
    }

    group.cancelAll()
}
