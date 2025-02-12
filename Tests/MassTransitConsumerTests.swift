import AMQPClient
import Testing

@testable import MassTransit

extension MassTransitConsumer {
    // Add publish method for message type
}

@Suite(.timeLimit(.minutes(1)))
struct MassTransitConsumerTests {
    private let logger = createTestLogger()

    @Test
    func consumesSingleMessageType() async throws {
        // Arrange
        try await withMassTransitConnection(logger: logger) { connection, massTransit in
            let messageExchange = "\(#function)-\(TestMessage.self)"
            let message = TestMessage(value: "A test message")

            try await withRunningMassTransitConsumer(using: connection, consumerName: #function) { consumer in
                let stream = try await consumer.consume(TestMessage.self, messageExchange: messageExchange)

                // Act
                try await massTransit.publish(message, exchangeName: messageExchange)
                let consumedMessage = try #require(await stream.firstElement())

                // Assert
                #expect(consumedMessage == message)
            }
        }
    }
}
