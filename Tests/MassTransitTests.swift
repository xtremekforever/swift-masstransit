import AMQPClient
import Testing

@testable import MassTransit

// NOTE: These tests require the RabbitMQ broker running from the Docker Compose project
@Suite(.timeLimit(.minutes(1)))
struct MassTransitTests {
    private let logger = createTestLogger()

    @Test
    func sendMessageSucceeds() async throws {
        try await withMassTransitConnection(logger: logger) { _, massTransit in
            try await massTransit.send(
                TestMessage(value: "A test message"), exchangeName: #function,
                configuration: testPublisherConfiguration
            )
        }
    }

    @Test
    func sendMessageFails() async throws {
        try await withMassTransitConnection(connect: false, logger: logger) { _, massTransit in
            await #expect(throws: AMQPConnectionError.self) {
                try await massTransit.send(
                    TestMessage(value: "A test message"), exchangeName: #function,
                    configuration: testPublisherConfiguration
                )
            }
        }
    }
}
