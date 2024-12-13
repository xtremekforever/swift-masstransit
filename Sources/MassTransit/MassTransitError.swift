import NIO

public enum MassTransitError: Error {
    case timeout
    case brokerTimeout
    case decodingError(data: String, type: MassTransitMessage.Type)
    case encodingError(message: MassTransitMessage)
    case invalidContext(responseAddress: String?)
}
