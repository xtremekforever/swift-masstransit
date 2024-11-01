import AsyncAlgorithms
import Foundation
import Logging
import MassTransit
import RabbitMq

struct MyTestEvent: MassTransitMessage {
    let id: UUID
    let name: String
}

let logger = Logger(label: "PublishConsume")
let rabbitMq = RetryingConnection("amqp://guest:guest@localhost/%2F", logger: logger)
let massTransit = MassTransit(rabbitMq, logger: logger)

try await withThrowingDiscardingTaskGroup { group in
    // Supervise RabbitMq connection
    group.addTask {
        await rabbitMq.run()
    }
    // Publish on an interval
    group.addTask {
        let timerSequence = AsyncTimerSequence(interval: .seconds(1), clock: .continuous)
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
