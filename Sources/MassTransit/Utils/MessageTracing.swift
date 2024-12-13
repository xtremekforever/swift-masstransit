import Tracing

enum PublishKind: String {
    case send = "Send"
    case publish = "Publish"
    case request = "Request"
    case respond = "Respond"
}

func withPublishSpan(
    _ messageId: String,
    _ messageType: String,
    _ publishKind: PublishKind,
    _ exchangeName: String,
    _ routingKey: String,
    _ body: () async throws -> Void
) async throws {
    try await withSpan("\(messageType) \(publishKind)", ofKind: .producer) { span in
        span.attributes.messaging.messageID = messageId
        span.attributes.messaging.destination = exchangeName
        span.attributes.messaging.system = "rabbitmq"
        span.attributes.messaging.rabbitMQ.routingKey = routingKey
        try await body()
    }
}

enum ConsumeKind: String {
    case response = "Response"
    case consume = "Consume"
}

func withConsumeSpan<T>(
    _ queueName: String,
    _ consumeKind: ConsumeKind,
    _ routingKey: String,
    _ body: (any Span) -> T
) -> T {
    return withSpan("\(queueName) \(consumeKind)", ofKind: .consumer) { span in
        span.attributes.messaging.system = "rabbitmq"
        span.attributes.messaging.rabbitMQ.routingKey = routingKey
        return body(span)
    }
}
