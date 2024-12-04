import RabbitMq

public struct MassTransitPublisherConfiguration: Sendable {
    public var exchangeOptions: ExchangeOptions
    public var publisherOptions: PublisherOptions

    public init(
        exchangeOptions: ExchangeOptions = .massTransitDefaults,
        publisherOptions: PublisherOptions = .init()
    ) {
        self.exchangeOptions = exchangeOptions
        self.publisherOptions = publisherOptions
    }
}

extension MassTransitPublisherConfiguration {
    func createPublisher(using connection: Connection, _ exchangeName: String) -> Publisher {
        .init(
            connection, exchangeName,
            exchangeOptions: exchangeOptions,
            publisherOptions: publisherOptions
        )
    }
}
