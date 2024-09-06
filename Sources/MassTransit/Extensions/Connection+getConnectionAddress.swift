import Foundation
import RabbitMq

extension Connection {
    func getConnectionAddress() async -> String {
        guard let configuredUrl = URL(string: await configuredUrl),
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
