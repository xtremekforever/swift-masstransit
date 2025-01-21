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

func urn(from messageType: String) -> String {
    "urn:message:\(messageType)"
}

extension MassTransitWrapper {
    init(_: T.Type, from buffer: ByteBuffer) throws {
        // Decode from JSON
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        guard let wrapper = try? decoder.decode(Self.self, from: buffer)
        else {
            throw MassTransitError.decodingError(data: String(buffer: buffer), type: T.self)
        }
        self = wrapper
    }

    func jsonEncode() throws -> ByteBuffer {
        // Encode to JSON
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.prettyPrinted, .withoutEscapingSlashes, .sortedKeys]
        guard let json = try? encoder.encodeAsByteBuffer(self, allocator: .init())
        else {
            throw MassTransitError.encodingError(message: message)
        }

        return json
    }

    static func create<TMessage: MassTransitMessage>(
        using value: TMessage, messageType: String
    ) -> MassTransitWrapper<TMessage> {
        assert(!messageType.isEmpty)

        return .init(
            messageId: UUID().uuidString,
            messageType: [urn(from: messageType)],
            message: value
        )
    }
}

/// Useful for parsing just the MassTransitWrapper while ignoring message content.
struct Wrapper: MassTransitMessage {}
