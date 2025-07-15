import Foundation
import RabbitMq

extension MassTransit {

    /// Perform a request using a specific message type, expecting another message type as a response.
    ///
    /// This method implements the most basic subset of the C# MassTransit library's Request/Response
    /// functionality by publishing a request, then awaiting a response on a specific exchange.
    ///
    /// - Parameters:
    ///   - value: The `MassTransitMessage`-conforming request to publish.
    ///   - _: The type of the `MassTransitMessage`-conforming response we expect.
    ///   - exchangeName: The name of the exchange to publish the request to.
    ///   - routingKey: Optional routing key to use for the request.
    ///   - timeout: Timeout for the response to the request to arrive.
    ///   - configuration: Configuration to use for the publisher, which contains exchange and publisher options.
    ///   - customMessageType: Custom message type to use. This will not affect the exchange name,
    ///     only the message type "urn" that is sent in the MassTransit message wrapper.
    /// - Throws:
    ///     - 'MassTransitError.timeout' if no response is received within the timeout.
    ///     - `CancellationError()` if the task is cancelled while publishing, consuming, or waiting for a response.
    /// - Returns: Message of type `TResponse` if it is received successfully.
    public func request<T: MassTransitMessage, TResponse: MassTransitMessage>(
        _ value: T,
        _: TResponse.Type,
        exchangeName: String = String(describing: T.self),
        routingKey: String = "",
        timeout: Duration = defaultRequestTimeout,
        configuration: MassTransitPublisherConfiguration = .init(),
        customMessageType: String? = nil
    ) async throws -> TResponse {
        // Use task group to timeout request
        return try await withThrowingTaskGroup(of: TResponse.self) { group in
            group.addTask {
                return try await performRequest(
                    value, TResponse.self, exchangeName, routingKey, configuration, customMessageType
                )
            }
            group.addTask {
                await gracefulCancellableDelay(timeout)
                throw MassTransitError.timeout
            }

            let result = try await group.next()
            group.cancelAll()
            return result!
        }
    }

    private func performRequest<T: MassTransitMessage, TResponse: MassTransitMessage>(
        _ value: T,
        _: TResponse.Type,
        _ exchangeName: String,
        _ routingKey: String,
        _ configuration: MassTransitPublisherConfiguration,
        _ customMessageType: String?
    ) async throws -> TResponse {
        // Publisher is used to send the request
        let publisher = configuration.createPublisher(using: connection, exchangeName)

        // Consumer is used to get a response with a custom requestName and address provided
        let requestName = "\(ProcessInfo.processInfo.hostName)_\(getModuleName(self))_bus_\(randomString(length: 26))"
        let address = "\(await connection.getConnectionAddress())/\(requestName)?temporary=true"
        let consumer = Consumer(
            connection, requestName, requestName, routingKey,
            exchangeOptions: ExchangeOptions(type: .fanout, durable: false, autoDelete: true),
            queueOptions: QueueOptions(autoDelete: true, durable: false, args: ["x-expires": .int32(60000)]),
            consumerOptions: ConsumerOptions(autoAck: true)
        )

        let messageType = customMessageType ?? exchangeName

        // Create MassTransitWrapper to send the request
        var request = MassTransitWrapper<T>.create(using: value, messageType: messageType)
        request.requestId = UUID().uuidString
        request.sourceAddress = address
        request.responseAddress = address
        logger.trace(
            "Wrapper for request",
            metadata: ["request": .string("\(request)"), "responseExchange": .string(exchangeName)]
        )

        // Start consuming before publishing request so we can get the response
        let responseStream = try await consumer.consumeBuffer()

        // Send the request with a regular publish
        let requestJson = try request.jsonEncode()
        logger.trace("Request JSON", metadata: ["requestJson": .string(String(buffer: requestJson))])

        logger.debug(
            "Sending request message...",
            metadata: [
                "messageType": .string(messageType),
                "exchangeName": .string(exchangeName),
                "routingKey": .string(routingKey),
            ]
        )
        try await withPublishSpan(request.messageId, messageType, .request, exchangeName, routingKey) {
            try await publisher.publish(requestJson, routingKey: routingKey)
        }

        logger.debug(
            "Waiting for response...",
            metadata: [
                "requestType": .string("\(TResponse.self)"),
                "queue": .string(requestName),
            ]
        )
        for await response in responseStream {
            let wrapper = try processConsumeBuffer(response, as: TResponse.self, .response, requestName, routingKey)
            if let wrapper {
                logger.trace(
                    "Consumed response from queue",
                    metadata: [
                        "response": .string("\(wrapper.message)"),
                        "queue": .string(requestName),
                    ]
                )
                return wrapper.message
            }
        }
        throw MassTransitError.timeout
    }
}
