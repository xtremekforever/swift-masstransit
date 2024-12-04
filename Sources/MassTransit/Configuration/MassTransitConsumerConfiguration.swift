import RabbitMq

public struct MassTransitConsumerConfiguration: Sendable {
    public var queueOptions: QueueOptions
    public var exchangeOptions: ExchangeOptions
    public var consumerOptions: ConsumerOptions

    public init(
        queueOptions: QueueOptions = .massTransitDefaults,
        exchangeOptions: ExchangeOptions = .massTransitDefaults,
        consumerOptions: ConsumerOptions = .massTransitDefaults
    ) {
        self.queueOptions = queueOptions
        self.exchangeOptions = exchangeOptions
        self.consumerOptions = consumerOptions
    }
}

extension MassTransitConsumerConfiguration {
    func createConsumer(
        using connection: Connection, _ queueName: String, _ exchangeName: String, _ routingKey: String
    ) -> Consumer {
        .init(
            connection, queueName, exchangeName, routingKey,
            exchangeOptions: exchangeOptions,
            queueOptions: queueOptions,
            consumerOptions: consumerOptions
        )
    }
}
