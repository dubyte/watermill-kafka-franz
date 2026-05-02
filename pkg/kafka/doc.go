// Package kafka provides a Watermill Pub/Sub implementation for Apache Kafka
// using the franz-go client library.
//
// # Overview
//
// This package implements the Watermill message.Publisher and message.Subscriber
// interfaces using franz-go, a modern, pure-Go Kafka client with excellent
// performance and reliability characteristics.
//
// # Features
//
//   - Full Watermill Pub/Sub interface compliance
//   - Async message publishing with configurable delivery guarantees
//   - Consumer groups with automatic partition rebalancing
//   - At-least-once delivery semantics
//   - Context-based cancellation and graceful shutdown
//   - Configurable marshaling strategies
//   - Support for Kafka authentication and TLS
//
// # Basic Usage
//
// Create a publisher:
//
//	publisher, err := kafka.NewPublisher(kafka.PublisherConfig{
//	    Brokers: []string{"localhost:9092"},
//	}, nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer publisher.Close()
//
// Create a subscriber:
//
//	subscriber, err := kafka.NewSubscriber(kafka.SubscriberConfig{
//	    Brokers:       []string{"localhost:9092"},
//	    ConsumerGroup: "my-consumer-group",
//	}, nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer subscriber.Close()
//
// Publish messages:
//
//	msg := message.NewMessage(uuid.New().String(), []byte("hello"))
//	err = publisher.Publish("my-topic", msg)
//
// Subscribe and consume:
//
//	messages, err := subscriber.Subscribe(context.Background(), "my-topic")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	for msg := range messages {
//	    // Process message
//	    fmt.Printf("Received: %s\n", msg.Payload)
//	    msg.Ack()
//	}
//
// # Configuration
//
// Both PublisherConfig and SubscriberConfig accept a variadic list of
// franz-go kgo.Opt options for advanced configuration. The Config structs
// provide sensible defaults while allowing full access to franz-go's
// extensive configuration options.
//
// # Marshaling
//
// The package provides a DefaultMarshaler that handles message serialization.
// Custom marshalers can be implemented to support different serialization
// formats or to add custom headers.
//
// # Context Metadata
//
// Kafka metadata is available through the message context:
//
//	partition, ok := kafka.PartitionFromContext(msg.Context())
//	offset, ok := kafka.OffsetFromContext(msg.Context())
//	timestamp, ok := kafka.MessageTimestampFromContext(msg.Context())
//	key, ok := kafka.MessageKeyFromContext(msg.Context())
//
// # Authentication
//
// SASL/PLAIN authentication:
//
//	import "github.com/twmb/franz-go/pkg/sasl/plain"
//
//	config.SASLMechanism = plain.Auth{
//	    User: "username",
//	    Pass: "password",
//	}.AsMechanism()
//
// SASL/SCRAM authentication:
//
//	import "github.com/twmb/franz-go/pkg/sasl/scram"
//
//	config.SASLMechanism = scram.Auth{
//	    User: "username",
//	    Pass: "password",
//	}.AsSha256Mechanism()
//
// TLS configuration:
//
//	config.TLS = &tls.Config{
//	    // Your TLS configuration
//	}
//
// # Custom Marshaler
//
// Implement custom serialization:
//
//	type MyMarshaler struct{}
//
//	func (m MyMarshaler) Marshal(topic string, msg *message.Message) (*kgo.Record, error) {
//	    // Custom serialization logic
//	}
//
//	func (m MyMarshaler) Unmarshal(record *kgo.Record) (*message.Message, error) {
//	    // Custom deserialization logic
//	}
//
//	// Use custom marshaler
//	publisherConfig := kafka.PublisherConfig{
//	    Brokers:   []string{"localhost:9092"},
//	    Marshaler: MyMarshaler{},
//	}
package kafka
