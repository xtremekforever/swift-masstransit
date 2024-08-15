import Logging
import RabbitMq

// Simple connector for RabbitMq designed to work with MassTransit wrappers
public struct SimpleRabbitMqConnector: Connectable {
    let connection: Connection

    public init(_ connectionUrl: String, logger: Logger = Logger(label: "\(SimpleRabbitMqConnector.self)")) throws {
        self.connection = try Connection(connectionUrl, logger: logger)
    }

    public func getConnection() async -> RabbitMq.Connection? {
        return connection
    }

    public func run(reconnectionInterval: Duration = .seconds(15)) async throws {
        try await connection.run(reconnectionInterval: reconnectionInterval)
    }
}
