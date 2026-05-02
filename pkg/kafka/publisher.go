package kafka

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Publisher implements message.Publisher interface using franz-go.
type Publisher struct {
	config PublisherConfig
	client *kgo.Client
	logger watermill.LoggerAdapter

	closed   bool
	closedMu sync.Mutex
}

// NewPublisher creates a new Kafka Publisher.
func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	if config.Marshaler == nil {
		config.Marshaler = DefaultMarshaler{}
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.MaxBufferedRecords(config.MaxBufferedRecords),
		kgo.ProduceRequestTimeout(config.ProduceRequestTimeout),
		kgo.ProducerBatchMaxBytes(config.BatchMaxBytes),
		kgo.ProducerBatchCompression(config.Compression...),
		kgo.ClientID(config.ClientID),
		kgo.Rack(config.RackID),
	}

	if config.DisableIdempotentWrite {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	if config.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(config.TLS))
	}

	if config.SASLMechanism != nil {
		opts = append(opts, kgo.SASL(config.SASLMechanism))
	}

	// Allow overriding with custom opts
	opts = append(opts, config.OverwriteKgoOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create kafka client")
	}

	return &Publisher{
		config: config,
		client: client,
		logger: logger,
	}, nil
}

// Publish implements message.Publisher.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	p.closedMu.Lock()
	if p.closed {
		p.closedMu.Unlock()
		return errors.New("publisher closed")
	}
	p.closedMu.Unlock()

	if len(msgs) == 0 {
		return nil
	}

	records := make([]*kgo.Record, len(msgs))
	for i, msg := range msgs {
		record, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		// Set context for cancellation/timeout
		// Note: We don't use msg.Context() here because it may be cancelled
		// The record context is used for request-scoped values, not for cancellation
		record.Context = context.Background()
		records[i] = record
	}

	// Use background context for publishing
	// Message contexts may be cancelled and shouldn't affect publishing
	ctx := context.Background()

	// Synchronous production
	result := p.client.ProduceSync(ctx, records...)
	if err := result.FirstErr(); err != nil {
		return errors.Wrap(err, "cannot produce messages")
	}

	// Log success with partition/offset info from first record
	if len(result) > 0 {
		rec := result[0].Record
		if rec != nil {
			p.logger.Trace("Published message to Kafka", watermill.LogFields{
				"topic":     topic,
				"partition": rec.Partition,
				"offset":    rec.Offset,
			})
		}
	}

	return nil
}

// Close implements message.Publisher.
func (p *Publisher) Close() error {
	p.closedMu.Lock()
	defer p.closedMu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	p.client.Close()
	return nil
}

// Compile-time interface check
var _ message.Publisher = (*Publisher)(nil)
