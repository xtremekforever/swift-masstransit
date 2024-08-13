import Foundation
import RabbitMq

extension Connection {
    func getConnectionAddress() -> String {
        guard let configuredUrl = URL(string: url),
            let scheme = configuredUrl.scheme,
            let host = configuredUrl.host
        else {
            return "unknown://invalid"
        }

        var type = ""
        switch scheme {
        case "amqp":
            type = "rabbitmq"
        case "amqps":
            type = "rabbitmqs"
        default:
            type = "unknown"
        }
        return "\(type)://\(host)"
    }
}
