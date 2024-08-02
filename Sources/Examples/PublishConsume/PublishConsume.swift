import AsyncAlgorithms
import Foundation
import Logging
import MassTransit
import RabbitMq

struct RabbitMqConnector: Connectable {
    let connection: Connection

    init(_ connectionUrl: String) throws {
        self.connection = try Connection(connectionUrl)
    }

    func getConnection() async -> RabbitMq.Connection? {
        return connection
    }

    func run() async throws {
        try await connection.run(reconnectionInterval: .seconds(15))
    }
}

struct MyTestEvent: Codable {
    let id: UUID
    let name: String
}

let logger = Logger(label: "PublishConsume")
let rabbitMqConnector = try RabbitMqConnector("amqp://guest:guest@localhost/%2F")
let massTransit = MassTransit(rabbitMqConnector, logger: logger)

let connectTask = Task {
    try await rabbitMqConnector.run()
}
let consumeTask = Task {
    let events = try await massTransit.consume(MyTestEvent.self)
    for await event in events {
        logger.info("Consumed event: \(event)")
    }
}

// Start publishing
for await _ in AsyncTimerSequence(interval: .seconds(1), clock: .continuous) {
    let event = MyTestEvent(
        id: UUID(),
        name: "My Event!"
    )
    logger.info("Publishing event: \(event)")
    try await massTransit.publish(event)
}

consumeTask.cancel()
connectTask.cancel()
