import Foundation
import Logging
import RabbitMq
import Tracing

/// This is used from `consumeWithContext` for providing the Response mechanism
public struct RequestContext<T: MassTransitMessage>: Sendable {
    internal let connection: Connection
    internal let requestId: String?
    internal let responseAddress: String?
    internal let logger: Logger
    public let message: T
}

extension RequestContext {
    /// Publish a response to this request context.
    ///
    /// This method will publish the response method that is used to the exchange that is provided
    /// in the `responseAddress`. This is to support the MassTransit request/response mechanism.
    ///
    /// - Parameters:
    ///   - value: The response MassTransitMessage to publish.
    ///   - messageType: The message type string to include in the MassTransit wrapper. Defaults to the name of the message type.
    ///   - routingKey: Optional routing key to use for publishing the response.
    ///   - configuration: Configuration to use for the publisher. Exchange defaults to `autoDelete = true`.
    ///   - retryInterval: The retry interval to use for publishing the response.
    /// - Throws:
    ///     - `MassTransitError.invalidContext` if the `responseAddress` is invalid.
    ///     - JSON encoding error if `value` cannot be encoded.
    ///     - `CancellationError()` if the task is cancelled during publish.
    public func respond<TResponse: MassTransitMessage>(
        _ value: TResponse,
        messageType: String = String(describing: TResponse.self),
        routingKey: String = "",
        configuration: MassTransitPublisherConfiguration = .init(
            exchangeOptions: .responseDefaults
        ),
        retryInterval: Duration = MassTransit.defaultRetryInterval
    ) async throws {
        guard let responseAddress = responseAddress,
            let responseUrl = URL(string: responseAddress),
            let responseExchange = responseUrl.pathComponents.last
        else {
            throw MassTransitError.invalidContext(responseAddress: responseAddress)
        }

        // Create MassTransitWrapper to send the response
        var response = MassTransitWrapper<T>.create(using: value, messageType: messageType)
        response.requestId = requestId
        response.destinationAddress = responseAddress
        logger.trace("Wrapper for response to \(responseExchange): \(response)")

        // Encode to JSON
        let messageJson = try response.jsonEncode()
        logger.trace("Message JSON to respond: \(String(buffer: messageJson))")

        // Publisher is used to send the response
        let publisher = configuration.createPublisher(using: connection, responseExchange)
        logger.debug("Publishing response of type \(messageType) on exchange \(responseExchange)...")
        try await withPublishSpan(response.messageId, messageType, .respond, responseExchange, routingKey) {
            try await publisher.retryingPublish(messageJson, routingKey: routingKey, retryInterval: retryInterval)
        }
    }
}
