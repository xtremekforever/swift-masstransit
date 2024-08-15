import Foundation

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
    init(_: T.Type, from jsonString: String) throws {
        // Decode from JSON
        let decoder = JSONDecoder()
        guard let data = jsonString.data(using: .utf8),
            let wrapper = try? decoder.decode(Self.self, from: data)
        else {
            throw MassTransitError.parsingError
        }
        self = wrapper
    }

    func jsonEncode() throws -> String {
        // Encode to JSON
        let encoder = JSONEncoder()
        guard let json = try? encoder.encode(self),
            let jsonString = String(data: json, encoding: .utf8)
        else {
            throw MassTransitError.parsingError
        }

        return jsonString
    }
}
