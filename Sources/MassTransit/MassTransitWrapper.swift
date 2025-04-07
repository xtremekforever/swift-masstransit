import FoundationEssentials
import IkigaJSON
import NIOCore

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
        var decoder = IkigaJSONDecoder()
        decoder.settings.dateDecodingStrategy = .iso8601
        guard let wrapper = try? decoder.decode(Self.self, from: buffer)
        else {
            throw MassTransitError.decodingError(data: String(buffer: buffer), type: T.self)
        }
        self = wrapper
    }

    func jsonEncode() throws -> ByteBuffer {
        // Encode to JSON
        var encoder = IkigaJSONEncoder()
        encoder.settings.dateEncodingStrategy = .iso8601
        do {
            var json = ByteBuffer()
            try encoder.encodeAndWrite(self, into: &json)
            return json
        } catch {
            throw MassTransitError.encodingError(message: message)
        }
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
