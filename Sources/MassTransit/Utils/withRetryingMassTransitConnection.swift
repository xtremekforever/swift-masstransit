import Logging
import RabbitMq

public func withRetryingMassTransitConnection(
    connectionString: String = "amqp://guest:guest@localhost/%2F",
    connectionConfiguration: ConnectionConfiguration = .init(),
    reconnectionInterval: Duration = MassTransit.defaultRetryInterval,
    logger: Logger,
    body: @escaping @Sendable (RetryingConnection, MassTransit) async throws -> Void
) async throws {
    let connection = RetryingConnection(
        connectionString, configuration: connectionConfiguration,
        reconnectionInterval: reconnectionInterval, logger: logger
    )
    let massTransit = MassTransit(connection, logger: logger)

    try await withThrowingDiscardingTaskGroup { group in
        group.addTask {
            await connection.run()
        }

        try await body(connection, massTransit)

        group.cancelAll()
    }
}
