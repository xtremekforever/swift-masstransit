import Logging
import NIOCore

extension ByteBuffer {
    func logJsonAsTrace(using logger: Logger) {
        if logger.logLevel > .trace {
            return
        }

        logger.trace("JSON buffer", metadata: ["messageJson": .string(String(buffer: self))])
    }
}
