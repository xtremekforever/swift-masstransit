import RabbitMq

extension ConsumerOptions {
    public static var massTransitDefaults: ConsumerOptions {
        .init(autoAck: true)
    }
}
