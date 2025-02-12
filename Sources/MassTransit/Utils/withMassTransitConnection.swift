import Logging
import RabbitMq

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
