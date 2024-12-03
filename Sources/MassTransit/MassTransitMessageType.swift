extension String {
    public static func massTransitMessageType(namespace: String, type: String) -> String {
        assert(!namespace.isEmpty)
        assert(!type.isEmpty)

        return "\(namespace):\(type)"
    }

    public static func massTransitMessageType<T: MassTransitMessage>(namespace: String, type: T.Type) -> String {
        .massTransitMessageType(namespace: namespace, type: String(describing: T.self))
    }
}
