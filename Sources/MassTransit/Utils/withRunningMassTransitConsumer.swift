import Logging
import RabbitMq

/// Run a `body` with a running `MassTransitConsumer` on a given `Connection`.
///
/// This is useful for easily testing a `MassTransitConsumer` that is constructed with a
/// consumer name and configuration, that gets terminated when the `body` exits.
///
/// - Parameters:
///   - connection: The `Connection` to use for the `MassTransitConsumer`.
///   - consumerName: The name of the consumer to use. This will be used for the `queueName` and `exchangeName` of the consumr.
///   - routingKey: Optional routing key to use for this consumer.
///   - configuration: Consumer configuration to use when constructing the consumer.
///   - retryInterval: Retry interval to use for retrying consume and bind operations.
///   - body: The body to run. When the body exits, the consumer is terminated along with streams.
/// - Throws: Any error thrown by the `MassTransitConsumer` or `body`.
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

    try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask {
            do {
                try await consumer.run()
            } catch is CancellationError {
                // Ignore
            } catch {
                throw error
            }
        }

        group.addTask {
            try await body(consumer)
        }

        // Exit task group once one task exits
        try await group.next()
        group.cancelAll()
    }
}
