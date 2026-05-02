# Watermill Kafka Franz

A [Watermill](https://watermill.io) Pub/Sub implementation for Apache Kafka using the modern [franz-go](https://github.com/twmb/franz-go) client library.

## Features

- Full Watermill Pub/Sub interface compliance
- Consumer groups with automatic partition rebalancing
- At-least-once delivery semantics
- Context-based cancellation and graceful shutdown
- Configurable marshaling strategies
- SASL authentication and TLS support
- Idiomatic Go with functional configuration
- High performance (lock-free internals)

## Installation

```bash
go get github.com/dubyte/watermill-kafka-franz
```

## Quick Start

### Publisher

```go
package main

import (
    "context"
    "log"
    
    "github.com/dubyte/watermill-kafka-franz/pkg/kafka"
    "github.com/ThreeDotsLabs/watermill/message"
)

func main() {
    publisher, err := kafka.NewPublisher(kafka.PublisherConfig{
        Brokers: []string{"localhost:9092"},
    }, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer publisher.Close()
    
    msg := message.NewMessage("id-1", []byte("hello world"))
    err = publisher.Publish("my-topic", msg)
    if err != nil {
        log.Fatal(err)
    }
}
```

### Subscriber

```go
subscriber, err := kafka.NewSubscriber(kafka.SubscriberConfig{
    Brokers:       []string{"localhost:9092"},
    ConsumerGroup: "my-consumer-group",
}, nil)
if err != nil {
    log.Fatal(err)
}
defer subscriber.Close()

messages, err := subscriber.Subscribe(context.Background(), "my-topic")
if err != nil {
    log.Fatal(err)
}

for msg := range messages {
    log.Printf("Received: %s", msg.Payload)
    msg.Ack()
}
```

## Configuration

### Publisher Options

```go
config := kafka.PublisherConfig{
    Brokers:               []string{"localhost:9092"},
    Marshaler:             kafka.DefaultMarshaler{},
    MaxBufferedRecords:    10000,
    ProduceRequestTimeout: 10 * time.Second,
    BatchMaxBytes:         1 << 20, // 1MB
    Compression:           []kgo.CompressionCodec{kgo.SnappyCompression()},
    ClientID:              "my-app",
}
```

### Subscriber Options

```go
config := kafka.SubscriberConfig{
    Brokers:                []string{"localhost:9092"},
    Unmarshaler:            kafka.DefaultMarshaler{},
    ConsumerGroup:          "my-group",
    AutoOffsetReset:        "earliest", // or "latest", "none"
    HeartbeatInterval:      3 * time.Second,
    SessionTimeout:         45 * time.Second,
    AutoCommitInterval:     5 * time.Second,
    DisableAutoCommit:      false,
    NackResendSleep:        100 * time.Millisecond,
    FetchMaxBytes:          50 << 20, // 50MB
    ClientID:               "my-app",
}
```

## Advanced Usage

### Custom Marshaler

```go
type MyMarshaler struct{}

func (m MyMarshaler) Marshal(topic string, msg *message.Message) (*kgo.Record, error) {
    // Custom serialization logic
}

func (m MyMarshaler) Unmarshal(record *kgo.Record) (*message.Message, error) {
    // Custom deserialization logic
}
```

### Context Metadata

Access Kafka metadata from message context:

```go
partition, ok := kafka.PartitionFromContext(msg.Context())
offset, ok := kafka.OffsetFromContext(msg.Context())
timestamp, ok := kafka.MessageTimestampFromContext(msg.Context())
key, ok := kafka.MessageKeyFromContext(msg.Context())
```

You can also set context values when building custom middleware or marshalers:

```go
ctx = kafka.ContextWithPartition(ctx, partition)
ctx = kafka.ContextWithOffset(ctx, offset)
ctx = kafka.ContextWithTimestamp(ctx, timestamp)
ctx = kafka.ContextWithKey(ctx, key)
```

### Authentication

#### SASL/PLAIN

```go
config.SASLMechanism = plain.Auth{
    User: "username",
    Pass: "password",
}.AsMechanism()
```

#### SASL/SCRAM

```go
config.SASLMechanism = scram.Auth{
    User: "username",
    Pass: "password",
}.AsSha256Mechanism()
```

#### TLS

```go
config.TLS = &tls.Config{
    // Your TLS configuration
}
```

## Testing

Start Kafka with Docker Compose:

```bash
docker-compose up -d
```

Run tests:

```bash
# Unit tests only
go test -short ./...

# All tests (requires Kafka)
go test ./...

# With race detector
go test -race ./...
```

## Why Franz-Go?

This library uses [franz-go](https://github.com/twmb/franz-go) instead of Sarama because:

- **Modern Go**: Uses latest Go features, cleaner API
- **Unified Client**: Single client for producing and consuming
- **Performance**: Lock-free internals, better throughput
- **Context Support**: Native context.Context support
- **Error Handling**: Better error types with `kerr` package
- **Active Development**: Well-maintained with frequent updates

## Comparison with Watermill-Kafka

| Feature | Watermill-Kafka (Sarama) | Watermill-Kafka-Franz |
|---------|-------------------------|----------------------|
| API Style | Struct-based config | Functional options + Struct |
| Client | Separate producer/consumer | Unified client |
| Context | Limited | Native support |
| Compression | Limited | Multiple codecs |
| Performance | Good | Excellent |

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
