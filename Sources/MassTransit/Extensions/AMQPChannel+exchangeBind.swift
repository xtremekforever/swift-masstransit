import AMQPClient
import Logging
import RabbitMq

extension AMQPChannel {
    public func exchangeBind(
        _ destination: String,
        _ source: String,
        _ routingKey: String,
        _ bindingOptions: BindingOptions,
        _ logger: Logger
    ) async throws {
        logger.trace(
            "Binding exchange...",
            metadata: [
                "destination": .string(destination),
                "source": .string(source),
                "routingKey": .string(routingKey),
                "bindingOptions": .string("\(bindingOptions)"),
            ]
        )
        try await exchangeBind(
            destination: destination,
            source: source,
            routingKey: routingKey,
            args: bindingOptions.args
        )
    }
}
