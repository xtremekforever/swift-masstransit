import RabbitMq

extension QueueOptions {
    public static var massTransitDefaults: QueueOptions {
        .init(autoDelete: true, durable: true)
    }
}
