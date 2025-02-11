import AMQPClient
import Logging
import RabbitMq

func withRetryingConnectionBody(
    _ connection: Connection,
    operationName: String,
    retryInterval: Duration = MassTransit.defaultRetryInterval,
    exitOnSuccess: Bool = false,
    body: @escaping @Sendable () async throws -> Void
) async throws {
    var firstAttempt = true
    let firstAttemptStart = ContinuousClock().now

    // Wait for connection, timeout after retryInterval
    await connection.waitForConnection(timeout: retryInterval)

    while !Task.isCancelledOrShuttingDown {
        do {
            connection.logger.trace("Starting body for operation \"\(operationName)\"...")
            try await body()
            if exitOnSuccess {
                return
            }
        } catch AMQPConnectionError.connectionClosed(let replyCode, let replyText) {
            if !firstAttempt {
                let error = AMQPConnectionError.connectionClosed(replyCode: replyCode, replyText: replyText)
                connection.logger.error(
                    "Connection closed while \(operationName): \(error)"
                )
            }

            // Wait for connection, timeout after retryInterval
            await connection.waitForConnection(timeout: retryInterval)

            firstAttempt = false
            continue
        } catch {
            // If this is our first attempt to connect, keep trying until we reach the timeout
            if firstAttempt && ContinuousClock().now - firstAttemptStart < retryInterval {
                await gracefulCancellableDelay(connection.connectionPollingInterval)
                continue
            }

            connection.logger.error("Error \(operationName): \(error)")
            firstAttempt = false
        }

        // Retry here
        connection.logger.trace("Will retry operation \"\(operationName)\" in \(retryInterval)...")
        try await Task.sleep(for: retryInterval)
    }
}
