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
        let continuation: AsyncStream<T>.Continuation
        let logger: Logger
        func handleMessage(buffer: NIOCore.ByteBuffer) throws {
            // Return the message
            let message = try MassTransitWrapper(T.self, from: buffer).message
            logger.debug("Consumed message: \(message)")

            continuation.yield(message)
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

    public func consume<T: MassTransitMessage>(
        _: T.Type,
        messageExchange: String = String(describing: T.self),
        customMessageType: String? = nil
    ) async throws -> AsyncStream<T> {
        // Determine message type
        let messageType = customMessageType ?? messageExchange

        while await !connection.isConnected && !Task.isCancelledOrShuttingDown {
            await connection.waitForConnection(timeout: retryInterval)
        }

        // Bind messageExchange to consumer exchange
        logger.debug("Binding exchange \(messageExchange) to \(exchangeName) with routing key: \(routingKey)")
        try await connection.getChannel()?.exchangeBind(
            destination: exchangeName, source: messageExchange, routingKey: routingKey
        )

        // Create a stream and message handler
        let urn = urn(from: messageType)
        logger.debug("Registering messageType \(urn) to consume from \(queueName)...")
        let (stream, continuation) = AsyncStream.makeStream(of: T.self)
        consumers[urn] = MessageHandler(continuation: continuation, logger: logger)

        return stream
    }

    public func run() async throws {
        let consumer = configuration.createConsumer(using: connection, queueName, exchangeName, routingKey)
        let consumeStream = try await consumer.retryingConsumeBuffer(retryInterval: retryInterval)

        logger.info("Starting consumer on queue \(queueName)...")

        // Consume messages from the consumer
        for await buffer in consumeStream {
            logger.trace("Consumed buffer: \(String(buffer: buffer))")

            try withSpan("\(queueName) consume", ofKind: .consumer) { span in
                // We parse the wrapper only to see what the messageTypes are
                let wrapper = try MassTransitWrapper(EmptyMessage.self, from: buffer)
                logger.trace("Wrapper received: \(wrapper)")

                // Looking for matching consumers
                var handled = false
                for messageType in wrapper.messageType {
                    guard let handler = consumers[messageType] else {
                        continue
                    }

                    // If there is a matching type, try to process it
                    logger.debug("Message type associated for queue \(queueName): \(messageType)")
                    try handler.handleMessage(buffer: buffer)
                    handled = true
                }

                // If the message is not handled, print an error
                if !handled {
                    logger.debug(
                        "Message of type(s) \(wrapper.messageType) from queue \(queueName) is missing a consumer!"
                    )

                    // TODO: This message should then be routed to a different error or unhandled queue
                }
            }
        }
    }
}
