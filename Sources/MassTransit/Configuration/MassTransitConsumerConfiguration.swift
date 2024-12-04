import RabbitMq

public struct MassTransitConsumerConfiguration: Sendable {
    public var queueOptions: QueueOptions
    public var exchangeOptions: ExchangeOptions
    public var consumerOptions: ConsumerOptions

    public init(
        queueOptions: QueueOptions = .massTransitDefaults,
        exchangeOptions: ExchangeOptions = .massTransitDefaults,
        consumerOptions: ConsumerOptions = .init()
    ) {
        self.queueOptions = queueOptions
        self.exchangeOptions = exchangeOptions
        self.consumerOptions = consumerOptions
    }
}

extension MassTransitConsumerConfiguration {
    func createConsumer(using connection: Connection, queueName: String, exchangeName: String) -> Consumer {
        .init(
            connection, queueName, exchangeName,
            exchangeOptions: exchangeOptions,
            queueOptions: queueOptions,
            consumerOptions: consumerOptions
        )
    }
}