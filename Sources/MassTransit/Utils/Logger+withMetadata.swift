import Logging

extension Logger.MetadataValue {
    var isEmpty: Bool {
        switch self {
        case .string(let str):
            return str.isEmpty
        case .stringConvertible(let strConvertible):
            return strConvertible.description.isEmpty
        case .dictionary(let dict):
            return dict.isEmpty
        case .array(let arr):
            return arr.isEmpty
        }
    }
}
extension Logger {
    func withMetadata(_ metadata: [String: Logger.MetadataValue], includeKeysForEmptyValues: Bool = false) -> Logger {
        var logger = self
        for (key, value) in metadata {
            if !includeKeysForEmptyValues && value.isEmpty {
                continue
            }

            logger[metadataKey: key] = value
        }
        return logger
    }
}
