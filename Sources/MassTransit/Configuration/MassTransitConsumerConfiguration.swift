import RabbitMq

/// Configuration for a MassTransit Consumer
///
/// This will wrap the options provided by the RabbitMq library for configuring
/// the consumer that is used under the hood.
public struct MassTransitConsumerConfiguration: Sendable {
    /// Options for the queue that is defined (optionally) and used for the consumer.
    public var queueOptions: QueueOptions
    /// Options for the exchange that is defined (optionally) and bound to the queue.
    public var exchangeOptions: ExchangeOptions
    /// Options for the consumer on the broker.
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
