# RediMQ

RediMQ is a library to enable and simplfy using REDIS (v6) as a Message Queue. The library
is built on the [go-redis/redis] module (v8 and above) for working with the REDIS backend.
RediMQ Supports two types of queues:

1. Ungrouped Message Topic (umts) - Unordered queue having multiple consumer groups 
    and each consumer group with multiple consumers. This can be done using the [Topic] struct.
2. Grouped Message Topic (gmts) - Queue which maintains order at a message group level. 
    The Queue allows multiple consumer groups each having multiple competing consumers. This can 
    be done using the [GroupedMessageTopic] struct.

The library leverages the REDIS Streams internally to provide the message queue features.

### pkg.go.dev documentation
https://pkg.go.dev/github.com/webbytes/redimq