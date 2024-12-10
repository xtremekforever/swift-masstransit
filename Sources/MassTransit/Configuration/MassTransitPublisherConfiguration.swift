import RabbitMq

/// Configuration for a MassTransit Publisher
///
/// This will wrap the options provided by the RabbitMq library for configuring
/// the publisher that is used under the hood.
public struct MassTransitPublisherConfiguration: Sendable {
    /// Options for the exchange that is defined (optionally) and published to.
    public var exchangeOptions: ExchangeOptions
    /// Options for the publisher on the broker.
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
