package kafka

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

// PublisherConfig configures the Kafka Publisher.
type PublisherConfig struct {
	// Brokers is the list of Kafka brokers to connect to.
	Brokers []string

	// Marshaler converts Watermill messages to Kafka records.
	// Defaults to DefaultMarshaler{}.
	Marshaler Marshaler

	// MaxBufferedRecords sets the max amount of records the client will buffer.
	// Defaults to 10000.
	MaxBufferedRecords int

	// ProduceRequestTimeout is the timeout for producing messages.
	// Defaults to 10 seconds.
	ProduceRequestTimeout time.Duration

	// BatchMaxBytes is the max size of a record batch.
	// Defaults to 1MB.
	BatchMaxBytes int32

	// Compression sets the compression codec for message batches.
	// Defaults to [SnappyCompression, NoCompression].
	Compression []kgo.CompressionCodec

	// DisableIdempotentWrite disables idempotent writes.
	// Idempotent writes are enabled by default for exactly-once semantics.
	DisableIdempotentWrite bool

	// TLS configuration for secure connections.
	TLS *tls.Config

	// SASLMechanism for authentication.
	SASLMechanism sasl.Mechanism

	// ClientID is the client ID to use for Kafka connections.
	// Defaults to "watermill".
	ClientID string

	// RackID is the rack ID for rack-aware consumers.
	RackID string

	// OverwriteKgoOpts allows passing arbitrary franz-go options.
	// Use with caution - these options may override settings above.
	OverwriteKgoOpts []kgo.Opt
}

// SubscriberConfig configures the Kafka Subscriber.
type SubscriberConfig struct {
	// Brokers is the list of Kafka brokers to connect to.
	Brokers []string

	// Unmarshaler converts Kafka records to Watermill messages.
	// Defaults to DefaultMarshaler{}.
	Unmarshaler Unmarshaler

	// ConsumerGroup is the consumer group ID. If empty, consumes from all partitions.
	ConsumerGroup string

	// AutoOffsetReset sets the offset reset policy: "earliest", "latest", or "none".
	// Defaults to "latest".
	AutoOffsetReset string

	// HeartbeatInterval is the consumer group heartbeat interval.
	// Defaults to 3 seconds.
	HeartbeatInterval time.Duration

	// SessionTimeout is the consumer group session timeout.
	// Defaults to 45 seconds.
	SessionTimeout time.Duration

	// RebalanceTimeout is the consumer group rebalance timeout.
	// Defaults to 60 seconds.
	RebalanceTimeout time.Duration

	// AutoCommitInterval sets how often to auto-commit offsets.
	// Defaults to 5 seconds.
	AutoCommitInterval time.Duration

	// DisableAutoCommit disables auto-commit. When disabled, offsets are committed manually.
	DisableAutoCommit bool

	// FetchMinBytes sets the minimum bytes to fetch per request.
	// Defaults to 1.
	FetchMinBytes int32

	// FetchMaxBytes sets the maximum bytes to fetch per request.
	// Defaults to 50MB.
	FetchMaxBytes int32

	// FetchMaxPartitionBytes sets the max bytes per partition.
	// Defaults to 1MB.
	FetchMaxPartitionBytes int32

	// FetchMaxWait sets the maximum time to wait for fetch.
	// Defaults to 5 seconds.
	FetchMaxWait time.Duration

	// NackResendSleep sets how long to sleep before resending a nacked message.
	// Defaults to 0 (no sleep). Use DefaultSubscriberConfig() for a pre-filled 100ms default.
	NackResendSleep time.Duration

	// CommitTimeout is the timeout for manual offset commits when DisableAutoCommit is true.
	// Defaults to 10 seconds. Increase for high-latency clusters.
	CommitTimeout time.Duration

	// TLS configuration for secure connections.
	TLS *tls.Config

	// SASLMechanism for authentication.
	SASLMechanism sasl.Mechanism

	// ClientID is the client ID to use for Kafka connections.
	// Defaults to "watermill".
	ClientID string

	// RackID is the rack ID for rack-aware consumers.
	RackID string

	// OverwriteKgoOpts allows passing arbitrary franz-go options.
	// Use with caution - these options may override settings above.
	OverwriteKgoOpts []kgo.Opt

	// InitializeTopicPartitions is the number of partitions for topics created by SubscribeInitialize.
	// Defaults to 1.
	InitializeTopicPartitions int32

	// InitializeTopicReplicationFactor is the replication factor for topics created by SubscribeInitialize.
	// Defaults to 1.
	InitializeTopicReplicationFactor int16
}

// Validate checks that the PublisherConfig has all required fields set.
func (c PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers must not be empty")
	}
	if c.Marshaler == nil {
		return errors.New("marshaler must not be nil")
	}
	return nil
}

// Validate checks that the SubscriberConfig has all required fields set.
func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers must not be empty")
	}
	if c.Unmarshaler == nil {
		return errors.New("unmarshaler must not be nil")
	}
	return nil
}

// setPublisherDefaults applies default values to zero-value fields in PublisherConfig.
func setPublisherDefaults(config *PublisherConfig) {
	if config.Marshaler == nil {
		config.Marshaler = DefaultMarshaler{}
	}
	if config.MaxBufferedRecords == 0 {
		config.MaxBufferedRecords = 10000
	}
	if config.ProduceRequestTimeout == 0 {
		config.ProduceRequestTimeout = 10 * time.Second
	}
	if config.BatchMaxBytes == 0 {
		config.BatchMaxBytes = 1 << 20 // 1MB
	}
	if len(config.Compression) == 0 {
		config.Compression = []kgo.CompressionCodec{kgo.SnappyCompression(), kgo.NoCompression()}
	}
	if config.ClientID == "" {
		config.ClientID = "watermill"
	}
}

// DefaultPublisherConfig returns a PublisherConfig with sensible defaults.
func DefaultPublisherConfig() PublisherConfig {
	return PublisherConfig{
		MaxBufferedRecords:    10000,
		ProduceRequestTimeout: 10 * time.Second,
		BatchMaxBytes:         1 << 20, // 1MB
		Compression:           []kgo.CompressionCodec{kgo.SnappyCompression(), kgo.NoCompression()},
		ClientID:              "watermill",
	}
}

// DefaultSubscriberConfig returns a SubscriberConfig with sensible defaults.
func DefaultSubscriberConfig() SubscriberConfig {
	return SubscriberConfig{
		AutoOffsetReset:                  "latest",
		HeartbeatInterval:                3 * time.Second,
		SessionTimeout:                   45 * time.Second,
		RebalanceTimeout:                 60 * time.Second,
		AutoCommitInterval:               5 * time.Second,
		FetchMinBytes:                    1,
		FetchMaxBytes:                    50 << 20, // 50MB
		FetchMaxPartitionBytes:           1 << 20,  // 1MB
		FetchMaxWait:                     5 * time.Second,
		NackResendSleep:                  100 * time.Millisecond,
		ClientID:                         "watermill",
		InitializeTopicPartitions:        1,
		InitializeTopicReplicationFactor: 1,
		CommitTimeout:                    10 * time.Second,
	}
}
