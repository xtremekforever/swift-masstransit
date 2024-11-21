import Foundation
import NIOCore
import NIOFoundationCompat

public typealias MassTransitMessage = Codable & Sendable

struct MassTransitWrapper<T: MassTransitMessage>: MassTransitMessage {
    var messageId: String
    var requestId: String?
    var sourceAddress: String?
    var destinationAddress: String?
    var responseAddress: String?
    var messageType: [String]
    var message: T
}

extension MassTransitWrapper {
    init(_: T.Type, from buffer: ByteBuffer) throws {
        // Decode from JSON
        let decoder = JSONDecoder()
        guard let wrapper = try? decoder.decode(Self.self, from: buffer)
        else {
            throw MassTransitError.parsingError
        }
        self = wrapper
    }

    func jsonEncode() throws -> ByteBuffer {
        // Encode to JSON
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .withoutEscapingSlashes]
        guard let json = try? encoder.encodeAsByteBuffer(self, allocator: .init())
        else {
            throw MassTransitError.parsingError
        }

        return json
    }

    static func create<TMessage: MassTransitMessage>(from value: TMessage, using exchangeName: String)
        -> MassTransitWrapper<
            TMessage
        >
    {
        return MassTransitWrapper<TMessage>(
            messageId: UUID().uuidString,
            messageType: ["urn:message:\(exchangeName)"],
            message: value
        )
    }
}
