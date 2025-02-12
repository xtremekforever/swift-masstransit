import AMQPClient
import RabbitMq
import Testing

@testable import MassTransit

@Suite(.timeLimit(.minutes(1)))
struct MassTransitConsumerTests {
    private let logger = createTestLogger()

    @Test(arguments: [TestMessage(value: "A test message")])
    func consumesSingleMessageType(message: TestMessage) async throws {
        // Arrange
        try await withMassTransitConnection(logger: logger) { connection, massTransit in
            try await withRunningMassTransitConsumer(using: connection, consumerName: #function) { consumer in
                // Act + Assert
                try await handleTestConsumeWithPublish(
                    message, on: consumer, publishWith: massTransit
                )
            }
        }
    }

    @Test(arguments: [(TestMessage(value: "First message type"), MessageType2(), MessageType3())])
    func consumesMultipleMessageTypes(
        message1: TestMessage, message2: MessageType2, message3: MessageType3
    ) async throws {
        // Arrange
        try await withMassTransitConnection(logger: logger) { connection, massTransit in
            try await withRunningMassTransitConsumer(using: connection, consumerName: #function) { consumer in
                // We run all tasks in concurrently since we want to prove they will all work at the same time
                try await withThrowingDiscardingTaskGroup { group in
                    // Act + Assert
                    group.addTask {
                        try await handleTestConsumeWithPublish(
                            message1, on: consumer, publishWith: massTransit
                        )
                    }
                    group.addTask {
                        try await handleTestConsumeWithPublish(
                            message2, on: consumer, publishWith: massTransit
                        )
                    }
                    group.addTask {
                        try await handleTestConsumeWithPublish(
                            message3, on: consumer, publishWith: massTransit
                        )
                    }
                }
            }
        }
    }

    @Test
    func recoversAfterConnectionFailure() async throws {
        // Arrange
        try await withRetryingMassTransitConnection(logger: logger) { connection, massTransit in
            try await withRunningMassTransitConsumer(
                using: connection, consumerName: #function, retryInterval: .milliseconds(10)
            ) { consumer in
                let messageExchange = "\(#function)-\(TestMessage.self)"
                let stream = try await consumer.consume(TestMessage.self, messageExchange: messageExchange)
                let originalUrl = await connection.configuredUrl

                // Ensure that consumer works first
                let testMessage1 = TestMessage(value: "first message")
                try await massTransit.publish(testMessage1, exchangeName: messageExchange)
                let consumedMessage1 = try #require(await stream.firstElement())
                #expect(consumedMessage1 == testMessage1)

                // Disconnect from broker
                await connection.reconfigure(with: "amqp://invalid")
                await consumer.waitForConsumerReadyState(ready: false, timeout: .seconds(10))

                // Reconnect to broker, wait for connection to be ready
                await connection.reconfigure(with: originalUrl)
                await consumer.waitForConsumerReadyState(ready: true, timeout: .seconds(10))

                // Ensure that consumer on the original stream is working again
                let testMessage2 = TestMessage(value: "second message")
                try await massTransit.publish(testMessage2, exchangeName: messageExchange)
                let consumedMessage2 = try #require(await stream.firstElement())
                #expect(consumedMessage2 == testMessage2)
            }
        }
    }

    private func handleTestConsumeWithPublish<T: MassTransitMessage & Equatable>(
        _ message: T, on consumer: MassTransitConsumer, publishWith massTransit: MassTransit
    ) async throws {
        let messageExchange = "\(#function)-\(message.self)"
        let stream = try await consumer.consume(T.self, messageExchange: messageExchange)

        // Act
        try await massTransit.publish(message, exchangeName: messageExchange)

        // Assert
        let consumedMessage = try #require(await stream.firstElement())
        #expect(consumedMessage == message)
    }
}

extension MassTransitConsumer {
    func waitForConsumerReadyState(ready: Bool, timeout: Duration) async {
        let start = ContinuousClock().now
        while !Task.isCancelledOrShuttingDown {
            if isConsumerReady == ready {
                break
            }

            if ContinuousClock().now - start >= timeout {
                break
            }

            await gracefulCancellableDelay(connection.connectionPollingInterval)
        }
    }
}
