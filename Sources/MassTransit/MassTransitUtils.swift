import AsyncAlgorithms

func randomString(length: Int) -> String {
    let letters = "abcdefghijklmnopqrstuvwxyz0123456789"
    var randomString = ""
    for _ in 0..<length {
        let randomIndex = Int.random(in: 0..<letters.count)
        let letter = letters[letters.index(letters.startIndex, offsetBy: randomIndex)]
        randomString += String(letter)
    }
    return randomString
}

func getModuleName<T>(_ module: T) -> String {
    return String(String(reflecting: T.self).prefix { $0 != "." }).replacingOccurrences(of: "_", with: "")
}

func gracefulCancellableDelay(timeout: Duration) async throws {
    for await _ in AsyncTimerSequence(interval: timeout, clock: .continuous).cancelOnGracefulShutdown() {
        break
    }
}
