import FoundationEssentials
import MassTransit

struct TestMessage: MassTransitMessage, Equatable {
    var value: String
}

struct MessageType2: MassTransitMessage, Equatable {
    var id: UUID = UUID()
    var value: String = "A value"
}

struct MessageType3: MassTransitMessage, Equatable {
    var boolean: Bool = false
    var integer: Int = 0
}
