import Tracing

func randomString(length: Int) -> String {
    let letters = "abcdefghijklmnopqrstuvwxyz0123456789"
    var randomString = ""
    for _ in 0..<length {
        let letter = letters.randomElement()!
        randomString += String(letter)
    }
    return randomString
}

func getModuleName<T>(_ module: T) -> String {
    return String(String(reflecting: T.self).prefix { $0 != "." }).replacingOccurrences(of: "_", with: "")
}
