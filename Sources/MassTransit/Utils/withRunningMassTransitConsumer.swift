import Logging
import RabbitMq

public func withRunningMassTransitConsumer(
    using connection: Connection,
    consumerName: String,
    routingKey: String = "",
    configuration: MassTransitConsumerConfiguration = .init(),
    retryInterval: Duration = MassTransit.defaultRetryInterval,
    body: @escaping @Sendable (MassTransitConsumer) async throws -> Void
) async throws {
    let consumer = MassTransitConsumer(
        using: connection, queueName: consumerName, exchangeName: consumerName,
        routingKey: routingKey, configuration: configuration, retryInterval: retryInterval,
        logger: connection.logger
    )

    try await withThrowingDiscardingTaskGroup { group in
        group.addTask {
            do {
                try await consumer.run()
            } catch is CancellationError {
                // Ignore
            } catch {
                throw error
            }
        }

        try await body(consumer)

        group.cancelAll()
    }
}
