import Logging
import RabbitMq

/// Run a `body` with a MassTransit connection in scope on a `RetryingConnection`.
///
/// This helper makes it easy to connect to a RabbitMQ broker using a `RetryingConnection`
/// and create a `MassTransit` instance that can be used in the passed `body`.
///
/// - Parameters:
///   - connectionString: Connection string to use to configure the `RetryingConnection`.
///   - connectionConfiguration: The configuration for the `RetryingConnection`.
///   - reconnectionInterval: The reconnection interval to use for retrying the connection.
///   - logger: Logger to use for the `RetryingConnection` and `MassTransit` instance.
///   - body: The body to run. When the body exits, the connection is closed.
/// - Throws: Any error thrown by the `RetryingConnection` or `body`.
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

    try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask {
            await connection.run()
        }
        group.addTask {
            try await body(connection, massTransit)
        }

        // Exit task group once one task exits
        try await group.next()
        group.cancelAll()
    }
}
