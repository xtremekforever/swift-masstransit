import AMQPClient
import Testing

@testable import MassTransit

// NOTE: These tests require the RabbitMQ broker running from the Docker Compose project
@Suite(.timeLimit(.minutes(1)))
struct MassTransitTests {
    private let logger = createTestLogger()

    // Auto-delete exchanges
    let publisherConfiguration = MassTransitPublisherConfiguration(
        exchangeOptions: .init(type: .fanout, autoDelete: true)
    )

    @Test
    func sendMessageSucceeds() async throws {
        try await withMassTransitConnection(logger: logger) { _, massTransit in
            try await massTransit.send(
                TestMessage(value: "A test message"), exchangeName: #function, configuration: publisherConfiguration
            )
        }
    }

    @Test
    func sendMessageFails() async throws {
        try await withMassTransitConnection(connect: false, logger: logger) { _, massTransit in
            await #expect(throws: AMQPConnectionError.self) {
                try await massTransit.send(
                    TestMessage(value: "A test message"), exchangeName: #function, configuration: publisherConfiguration
                )
            }
        }
    }
}
