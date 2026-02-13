## 0.10.0 (in development)

### Enhancements

 * `Connection.withChannel` can be used for short-lived channels, [a pattern that is discouraged](https://www.rabbitmq.com/docs/channels#high-channel-churn)
   but can be useful in integration tests

### Bug Fixes

 * NIO pipeline handler ordering caused a crash on TLS connections when opening a channel
 * Heartbeat monitor could not observe inbound frames (any traffic counts for a heartbeat)


## 0.9.0 (Dec 29, 2025)

#### Initial Release

This library, heavily inspired by a few existing AMQP 0-9-1 clients (the original Bunny, Pika, amqprs, the .NET RabbitMQ client 7.x)
is now mature enough to be publicly released.

It targets Swift 6.x and uses modern Swift's concurrency features.

In addition, this is the 2nd AMQP 0-9-1 client — after .NET client 7.x — to support
automatic publisher confirm tracking and acknowledgement.
