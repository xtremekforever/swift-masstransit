import Logging
import NIOCore
import RabbitMq
import Tracing

protocol MessageConsumerHandler {
    func handleMessage(buffer: ByteBuffer) throws
}

public actor MassTransitConsumer {
    let connection: Connection
    let queueName: String
    let exchangeName: String
    let configuration: MassTransitConsumerConfiguration
    let routingKey: String
    let retryInterval: Duration
    let logger: Logger

    private var consumers: [String: MessageConsumerHandler] = [:]

    private struct MessageHandler<T: MassTransitMessage>: MessageConsumerHandler {
        let queueName: String
        let continuation: AsyncStream<T>.Continuation
        let logger: Logger

        func handleMessage(buffer: ByteBuffer) throws {
            let wrapper = try MassTransitWrapper(T.self, from: buffer)
            logger.debug("Consumed message \(wrapper.message) from queue \(queueName)")
            continuation.yield(wrapper.message)
        }
    }

    public init(
        using connection: Connection,
        queueName: String,
        exchangeName: String,
        routingKey: String = "",
        configuration: MassTransitConsumerConfiguration = .init(),
        retryInterval: Duration = MassTransit.defaultRetryInterval,
        logger: Logger = .init(label: .init(describing: MassTransitConsumer.self))
    ) {
        self.connection = connection
        self.queueName = queueName
        self.exchangeName = exchangeName
        self.routingKey = routingKey
        self.configuration = configuration
        self.retryInterval = retryInterval
        self.logger = logger
    }

    private func removeConsumer(messageType: String) {
        logger.debug("Removing consumer \(messageType) for \(queueName)...")
        consumers.removeValue(forKey: messageType)
    }

    public func consume<T: MassTransitMessage>(
        _: T.Type,
        messageExchange: String = String(describing: T.self),
        exchangeOptions: ExchangeOptions = .massTransitDefaults,
        routingKey: String = "",
        bindingOptions: BindingOptions = .init(),
        customMessageType: String? = nil
    ) async throws -> AsyncStream<T> {
        // Determine message type
        let messageType = customMessageType ?? messageExchange

        // If we need to use a messageExchange, we want to declare and bind it here
        if !messageExchange.isEmpty {
            while await !connection.isConnected && !Task.isCancelledOrShuttingDown {
                await connection.waitForConnection(timeout: retryInterval)
            }
            let channel = try await connection.getChannel()

            // Bind messageExchange to consumer exchange
            try await channel?.exchangeDeclare(messageExchange, exchangeOptions, logger)

            // Bind messageExchange to consumer exchange
            try await channel?.exchangeBind(
                exchangeName, messageExchange, routingKey, bindingOptions, logger
            )
        }

        // Create a stream and message handler
        let urn = urn(from: messageType)
        logger.info("Consuming messages of type \(messageType) on queue \(queueName)...")
        let (stream, continuation) = AsyncStream.makeStream(of: T.self)
        consumers[urn] = MessageHandler(queueName: queueName, continuation: continuation, logger: logger)

        // Handle termination
        continuation.onTermination = { _ in
            Task { await self.removeConsumer(messageType: urn) }
        }

        return stream
    }

    public func run() async throws {
        logger.info("Starting consumer on queue \(queueName)...")
        let consumer = configuration.createConsumer(using: connection, queueName, exchangeName, routingKey)
        let consumeStream = try await consumer.retryingConsumeBuffer(retryInterval: retryInterval)

        // Consume messages from the consumer
        for await buffer in consumeStream {
            process(buffer)
        }
    }

    private func process(_ buffer: ByteBuffer) {
        withSpan("\(queueName) consume", ofKind: .consumer) { span in
            logger.trace("Consumed buffer from \(queueName): \(String(buffer: buffer))")
            var handled = false

            do {
                // We parse the wrapper only to see what the messageTypes are
                let wrapper = try MassTransitWrapper(Wrapper.self, from: buffer)

                // Looking for matching consumers
                for messageType in wrapper.messageType {
                    guard let handler = consumers[messageType] else {
                        continue
                    }

                    // If there is a matching type, try to process it
                    logger.trace("Message type associated for queue \(queueName): \(messageType)")
                    try handler.handleMessage(buffer: buffer)
                    handled = true
                }

                // If the message is not handled, print an error
                if !handled {
                    logger.error(
                        "Message of type(s) \(wrapper.messageType) from queue \(queueName) is missing a consumer!"
                    )

                    // TODO: This message should then be routed to a different error or unhandled queue
                }
            } catch {
                logger.error("Error in message consumed from \(queueName): \(error)")

                // TODO: We should route this to an error queue
            }
        }
    }
}
