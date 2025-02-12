import Logging
import RabbitMq

public func withMassTransitConnection(
    connectionString: String = "amqp://guest:guest@localhost/%2F",
    connectionConfiguration: ConnectionConfiguration = .init(),
    connect: Bool = true,
    logger: Logger,
    body: @escaping @Sendable (Connection, MassTransit) async throws -> Void
) async throws {
    let rabbitMq = BasicConnection(
        connectionString, configuration: connectionConfiguration, logger: logger
    )
    let massTransit = MassTransit(rabbitMq, logger: logger)
    do {
        if connect {
            try await rabbitMq.connect()
        }
        try await body(rabbitMq, massTransit)
    } catch {
        // Close RabbitMq connection and rethrow error
        await rabbitMq.close()
        throw error
    }

    // Close when done
    await rabbitMq.close()
}
