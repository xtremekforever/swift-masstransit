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
        decoder.dateDecodingStrategy = .iso8601
        guard let wrapper = try? decoder.decode(Self.self, from: buffer)
        else {
            throw MassTransitError.parsingError
        }
        self = wrapper
    }

    func jsonEncode() throws -> ByteBuffer {
        // Encode to JSON
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.prettyPrinted, .withoutEscapingSlashes]
        guard let json = try? encoder.encodeAsByteBuffer(self, allocator: .init())
        else {
            throw MassTransitError.parsingError
        }

        return json
    }

    static func create<TMessage: MassTransitMessage>(
        using value: TMessage, urn: String
    ) -> MassTransitWrapper<TMessage> {
        assert(!urn.isEmpty)

        return .init(
            messageId: UUID().uuidString,
            messageType: ["urn:message:\(urn)"],
            message: value
        )
    }
}
