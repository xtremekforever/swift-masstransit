import RabbitMq

extension ExchangeOptions {
    public static var massTransitDefaults: ExchangeOptions {
        .init(type: .fanout, durable: true)
    }

    public static var responseDefaults: ExchangeOptions {
        .init(type: .fanout, autoDelete: true)
    }
}
