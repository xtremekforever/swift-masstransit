import MassTransit

struct TestMessage: MassTransitMessage, Equatable {
    var value: String
}
