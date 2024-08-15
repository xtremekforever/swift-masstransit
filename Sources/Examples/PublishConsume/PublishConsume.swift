import AsyncAlgorithms
import Foundation
import Logging
import MassTransit

struct MyTestEvent: MassTransitMessage {
    let id: UUID
    let name: String
}

let logger = Logger(label: "PublishConsume")
let rabbitMq = try SimpleRabbitMqConnector("amqp://guest:guest@localhost/%2F", logger: logger)
let massTransit = MassTransit(rabbitMq, logger: logger)

try await withThrowingDiscardingTaskGroup { group in
    // Supervise RabbitMq connection
    group.addTask {
        try await rabbitMq.run()
    }
    // Consume events
    group.addTask {
        let events = try await massTransit.consume(MyTestEvent.self)
        for await event in events {
            logger.info("Consumed event: \(event)")
        }
    }
    // Publish on an interval
    group.addTask {
        for await _ in AsyncTimerSequence(interval: .seconds(1), clock: .continuous) {
            let event = MyTestEvent(
                id: UUID(),
                name: "My Event!"
            )
            logger.info("Publishing event: \(event)")
            try await massTransit.publish(event)
        }
    }
}
