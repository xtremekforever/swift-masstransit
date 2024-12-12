import Foundation
import Logging
import RabbitMq
import Tracing

// This is used from `consumeWithContext` for providing the Response mechanism
public struct RequestContext<T: MassTransitMessage>: Sendable {
    internal let connection: Connection
    internal let requestId: String?
    internal let responseAddress: String?
    internal let logger: Logger
    public let message: T
}

extension RequestContext {
    public func respond<TResponse: MassTransitMessage>(
        _ value: TResponse,
        routingKey: String = "",
        configuration: MassTransitPublisherConfiguration = .init(
            exchangeOptions: .init(type: .fanout, autoDelete: true)
        ),
        customMessageType: String? = nil,
        retryInterval: Duration = MassTransit.defaultRetryInterval
    ) async throws {
        guard let responseAddress = responseAddress,
            let responseUrl = URL(string: responseAddress),
            let responseExchange = responseUrl.pathComponents.last
        else {
            throw MassTransitError.invalidContext
        }
        let messageType = customMessageType ?? String(describing: TResponse.self)

        // Create MassTransitWrapper to send the response
        let response = MassTransitWrapper(
            messageId: UUID().uuidString,
            requestId: requestId,
            destinationAddress: responseAddress,
            messageType: ["urn:message:\(messageType)"],
            message: value
        )
        logger.trace("Wrapper for response: \(response)")

        // Encode to JSON
        let messageJson = try response.jsonEncode()
        logger.trace("Message JSON to respond: \(String(buffer: messageJson))")

        // Publisher is used to send the response
        let publisher = configuration.createPublisher(using: connection, responseExchange)
        try await withSpan("\(messageType) response", ofKind: .producer) { span in
            span.attributes.messaging.messageID = response.messageId
            span.attributes.messaging.destination = responseExchange
            span.attributes.messaging.rabbitMQ.routingKey = routingKey
            span.attributes.messaging.system = "rabbitmq"
            try await publisher.retryingPublish(messageJson, routingKey: routingKey, retryInterval: retryInterval)
        }
    }
}
