import Logging
import RabbitMq

/// Run a `body` with a MassTransit connection in scope.
///
/// This helper makes it easy to connect to a RabbitMQ broker using a `BasicConnection`
/// and create a `MassTransit` instance that can be used in the passed `body`. When the
/// `body` exits, the connection is closed.
///
/// - Parameters:
///   - connectionString: Connection string to use to configure the `BasicConnection`.
///   - connectionConfiguration: The configuration for the `BasicConnection`.
///   - connect: Whether or not to actually connect to the broker.
///     This can be used to test failure scenarios or delay connecting to the broker until later.
///   - logger: Logger to use for the `BasicConnection` and `MassTransit` instance.
///   - body: The body to run. When the body exits, the connection is closed.
/// - Throws: Any error thrown by the `BasicConnection` or `body`.
public func withMassTransitConnection(
    connectionString: String = "amqp://guest:guest@localhost/%2F",
    connectionConfiguration: ConnectionConfiguration = .init(),
    connectionPollingInterval: Duration = defaultConnectionPollingInterval,
    connect: Bool = true,
    logger: Logger,
    body: @escaping @Sendable (BasicConnection, MassTransit) async throws -> Void
) async throws {
    try await withBasicConnection(
        connectionString, configuration: connectionConfiguration,
        connectionPollingInterval: connectionPollingInterval,
        connect: connect, logger: logger
    ) { connection in
        let massTransit = MassTransit(connection, logger: logger)
        try await body(connection, massTransit)
    }
}
