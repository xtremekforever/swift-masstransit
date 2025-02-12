import AMQPClient
import RabbitMq
import Testing

@testable import MassTransit

@Suite(.timeLimit(.minutes(1)))
struct MassTransitConsumerTests {
    private let logger = createTestLogger()

    private func withConfiguredMassTransitConsumer(
        connect: Bool = true,
        consumerName: String = #function,
        retryInterval: Duration = MassTransit.defaultRetryInterval,
        body: @escaping @Sendable (BasicConnection, MassTransit, MassTransitConsumer) async throws -> Void
    ) async throws {
        try await withMassTransitConnection(connect: connect, logger: logger) { connection, massTransit in
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

    @Test(arguments: [(TestMessage(value: "A request message"), MessageType2())])
    func consumesRequestAndResponds(request: TestMessage, response: MessageType2) async throws {
        // Arrange
        try await withConfiguredMassTransitConsumer(consumerName: #function) { _, massTransit, consumer in
            let requestStream = try await consumer.consumeWithContext(
                TestMessage.self, exchangeOptions: .responseDefaults
            )

            // Act + Assert
            try await withThrowingDiscardingTaskGroup { group in
                group.addTask {
                    let requestContext = try #require(await requestStream.firstElement())
                    try await requestContext.respond(response)
                }

                // Send a request, assert the response
                let requestResponse = try await massTransit.request(
                    request, MessageType2.self,
                    configuration: .init(exchangeOptions: .responseDefaults)
                )
                #expect(requestResponse == response)
            }
        }
    }

    @Test
    func recoversAfterInitialConnectionFailure() async throws {
        // Arrange
        try await withConfiguredMassTransitConsumer(
            connect: false, retryInterval: .milliseconds(250)
        ) { connection, massTransit, consumer in
            let message = TestMessage(value: "test publish after connection failure")
            let messageExchange = "\(#function).\(TestMessage.self)"
            let exchangeOptions = ExchangeOptions(type: .direct, durable: false, autoDelete: true)
            let stream = try await consumer.consume(
                TestMessage.self, messageExchange: messageExchange,
                exchangeOptions: exchangeOptions
            )

            // Make sure the consumer is not ready
            #expect(await !consumer.isConsumerReady)

            // Connect to broker, wait for connection
            try await connection.connect()
            await consumer.waitForConsumerReadyState(ready: true, timeout: .seconds(10))

            // Publish a test message, verify it arrives on consumer stream
            try await consumer.publishAndVerify(
                message, using: messageExchange, publishWith: massTransit, verifyOn: stream
            )
        }
    }

    @Test
    func rebindsConsumersAfterConnectionFailure() async throws {
        // Arrange
        try await withConfiguredMassTransitConsumer(
            consumerName: #function, retryInterval: .milliseconds(250)
        ) { connection, massTransit, consumer in
            let messageExchange = "\(#function)-\(TestMessage.self)"
            let exchangeOptions = ExchangeOptions(type: .direct, durable: false, autoDelete: true)
            let stream = try await consumer.consume(
                TestMessage.self, messageExchange: messageExchange, exchangeOptions: exchangeOptions
            )

            // Ensure that consumer works first
            try await consumer.publishAndVerify(
                TestMessage(value: "first message"), using: messageExchange,
                publishWith: massTransit, verifyOn: stream
            )

            // Disconnect from broker
            await connection.close()
            await consumer.waitForConsumerReadyState(ready: false, timeout: .seconds(10))

            // Reconnect to broker, wait for consumer to be ready
            try await connection.connect()
            await consumer.waitForConsumerReadyState(ready: true, timeout: .seconds(10))

            // Ensure that consumer on the original stream is working again
            try await consumer.publishAndVerify(
                TestMessage(value: "second message"), using: messageExchange,
                publishWith: massTransit, verifyOn: stream
            )
        }
    }
}

extension MassTransitConsumer {
    func publishAndVerify<T: MassTransitMessage & Equatable>(
        _ message: T, using messageExchange: String,
        exchangeOptions: ExchangeOptions = .init(type: .direct, durable: false, autoDelete: true),
        publishWith massTransit: MassTransit,
        verifyOn stream: AnyAsyncSequence<T>
    ) async throws {
        try await massTransit.publish(
            message, exchangeName: messageExchange, configuration: .init(exchangeOptions: exchangeOptions)
        )
        let consumedMessage = try #require(await stream.firstElement())
        #expect(consumedMessage == message)
    }

    func handleTestConsumeWithPublish<T: MassTransitMessage & Equatable>(
        _ message: T, exchangeName: String = #function,
        exchangeOptions: ExchangeOptions = .init(type: .direct, durable: false, autoDelete: true),
        publishWith massTransit: MassTransit
    ) async throws {
        let messageExchange = "\(exchangeName).\(T.self)"
        let stream = try await consume(
            T.self, messageExchange: messageExchange, exchangeOptions: exchangeOptions
        )

        // Act + Assert
        try await publishAndVerify(message, using: messageExchange, publishWith: massTransit, verifyOn: stream)
    }
}
