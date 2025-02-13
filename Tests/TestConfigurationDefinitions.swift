import MassTransit
import RabbitMq

// Always auto-delete exchanges for tests

let testMessageTypeExchangeOptions = ExchangeOptions(type: .direct, autoDelete: true)
let testExchangeOptions = ExchangeOptions(type: .fanout, autoDelete: true)
let testConsumerConfiguration = MassTransitConsumerConfiguration(exchangeOptions: testExchangeOptions)
let testPublisherConfiguration = MassTransitPublisherConfiguration(exchangeOptions: testExchangeOptions)
