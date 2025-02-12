import AMQPClient
import RabbitMq
import Testing

@testable import MassTransit

@Suite(.timeLimit(.minutes(1)))
struct MassTransitConsumerTests {
    private let logger = createTestLogger()

    private func withConfiguredMassTransitConsumer(
        consumerName: String = #function,
        retryInterval: Duration = MassTransit.defaultRetryInterval,
        body: @escaping @Sendable (BasicConnection, MassTransit, MassTransitConsumer) async throws -> Void
    ) async throws {
        try await withMassTransitConnection(logger: logger) { connection, massTransit in
            try await withRunningMassTransitConsumer(
                using: connection, consumerName: consumerName,
                configuration: .init(exchangeOptions: .init(type: .fanout, autoDelete: true)),
                retryInterval: retryInterval
            ) { consumer in
                try await body(connection, massTransit, consumer)
            }
        }
    }

    @Test(arguments: [TestMessage(value: "A test message")])
    func consumesSingleMessageType(message: TestMessage) async throws {
        // Arrange
        try await withConfiguredMassTransitConsumer(consumerName: #function) { _, massTransit, consumer in
            // Act + Assert
            try await consumer.handleTestConsumeWithPublish(
                message, exchangeName: #function, publishWith: massTransit
            )
        }
    }

    @Test(arguments: [(TestMessage(value: "First message type"), MessageType2(), MessageType3())])
    func consumesMultipleMessageTypes(
        message1: TestMessage, message2: MessageType2, message3: MessageType3
    ) async throws {
        // Arrange
        try await withConfiguredMassTransitConsumer(consumerName: #function) { _, massTransit, consumer in
            // We run all tasks in concurrently since we want to prove they will all work at the same time
            try await withThrowingDiscardingTaskGroup { group in
                // Act + Assert
                group.addTask {
                    try await consumer.handleTestConsumeWithPublish(
                        message1, exchangeName: #function, publishWith: massTransit
                    )
                }
                group.addTask {
                    try await consumer.handleTestConsumeWithPublish(
                        message2, exchangeName: #function, publishWith: massTransit
                    )
                }
                group.addTask {
                    try await consumer.handleTestConsumeWithPublish(
                        message3, exchangeName: #function, publishWith: massTransit
                    )
                }
            }
        }
    }

    @Test
    func recoversAfterConnectionFailure() async throws {
        // Arrange
        try await withConfiguredMassTransitConsumer(
            consumerName: #function, retryInterval: .milliseconds(10)
        ) { connection, massTransit, consumer in
            let messageExchange = "\(#function)-\(TestMessage.self)"
            let exchangeOptions = ExchangeOptions(type: .direct, durable: false, autoDelete: true)
            let stream = try await consumer.consume(
                TestMessage.self, messageExchange: messageExchange, exchangeOptions: exchangeOptions
            )

            // Ensure that consumer works first
            let testMessage1 = TestMessage(value: "first message")
            try await massTransit.publish(
                testMessage1, exchangeName: messageExchange, configuration: .init(exchangeOptions: exchangeOptions)
            )
            let consumedMessage1 = try #require(await stream.firstElement())
            #expect(consumedMessage1 == testMessage1)

            // Disconnect from broker
            await connection.close()
            await consumer.waitForConsumerReadyState(ready: false, timeout: .seconds(10))

            // Reconnect to broker, wait for consumer to be ready
            try await connection.connect()
            await consumer.waitForConsumerReadyState(ready: true, timeout: .seconds(10))

            // Ensure that consumer on the original stream is working again
            let testMessage2 = TestMessage(value: "second message")
            try await massTransit.publish(
                testMessage2, exchangeName: messageExchange, configuration: .init(exchangeOptions: exchangeOptions)
            )
            let consumedMessage2 = try #require(await stream.firstElement())
            #expect(consumedMessage2 == testMessage2)
        }
    }
}

extension MassTransitConsumer {
    func handleTestConsumeWithPublish<T: MassTransitMessage & Equatable>(
        _ message: T, exchangeName: String = #function,
        exchangeOptions: ExchangeOptions = .init(type: .direct, durable: false, autoDelete: true),
        publishWith massTransit: MassTransit
    ) async throws {
        let messageExchange = "\(exchangeName).\(T.self)"
        let stream = try await consume(
            T.self, messageExchange: messageExchange, exchangeOptions: exchangeOptions
        )

        // Act
        try await massTransit.publish(
            message, exchangeName: messageExchange, configuration: .init(exchangeOptions: exchangeOptions)
        )

        // Assert
        let consumedMessage = try #require(await stream.firstElement())
        #expect(consumedMessage == message)
    }

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
