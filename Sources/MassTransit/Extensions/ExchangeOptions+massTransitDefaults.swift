import RabbitMq

extension ExchangeOptions {
    public static var massTransitDefaults: ExchangeOptions {
        .init(type: .fanout, durable: true)
    }
}
