package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Subscriber implements message.Subscriber interface using franz-go.
type Subscriber struct {
	config SubscriberConfig
	client *kgo.Client
	logger watermill.LoggerAdapter

	closing       chan struct{}
	subscribersWg sync.WaitGroup
	closed        uint32 // atomic
}

// NewSubscriber creates a new Kafka Subscriber.
func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config = setSubscriberDefaults(config)

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.FetchMinBytes(config.FetchMinBytes),
		kgo.FetchMaxBytes(config.FetchMaxBytes),
		kgo.FetchMaxPartitionBytes(config.FetchMaxPartitionBytes),
		kgo.FetchMaxWait(config.FetchMaxWait),
		kgo.ClientID(config.ClientID),
		kgo.Rack(config.RackID),
		kgo.AllowAutoTopicCreation(),
	}

	// Consumer group configuration
	if config.ConsumerGroup != "" {
		opts = append(opts,
			kgo.ConsumerGroup(config.ConsumerGroup),
			kgo.HeartbeatInterval(config.HeartbeatInterval),
			kgo.SessionTimeout(config.SessionTimeout),
			kgo.RebalanceTimeout(config.RebalanceTimeout),
		)

		// Auto-commit configuration
		if !config.DisableAutoCommit {
			opts = append(opts,
				kgo.AutoCommitMarks(),
				kgo.AutoCommitInterval(config.AutoCommitInterval),
			)
		}
	}

	// Offset reset policy (applies to both consumer group and direct consumption)
	switch config.AutoOffsetReset {
	case "earliest":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	case "latest":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	case "none":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtCommitted()))
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

	return &Subscriber{
		config:  config,
		client:  client,
		logger:  logger,
		closing: make(chan struct{}),
	}, nil
}

func setSubscriberDefaults(config SubscriberConfig) SubscriberConfig {
	if config.Unmarshaler == nil {
		config.Unmarshaler = DefaultMarshaler{}
	}
	if config.AutoOffsetReset == "" {
		config.AutoOffsetReset = "latest"
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 3 * time.Second
	}
	if config.SessionTimeout == 0 {
		config.SessionTimeout = 45 * time.Second
	}
	if config.RebalanceTimeout == 0 {
		config.RebalanceTimeout = 60 * time.Second
	}
	if config.AutoCommitInterval == 0 {
		config.AutoCommitInterval = 5 * time.Second
	}
	if config.FetchMaxBytes == 0 {
		config.FetchMaxBytes = 50 << 20 // 50MB
	}
	if config.FetchMaxPartitionBytes == 0 {
		config.FetchMaxPartitionBytes = 1 << 20 // 1MB
	}
	if config.FetchMaxWait == 0 {
		config.FetchMaxWait = 5 * time.Second
	}
	return config
}

// Subscribe implements message.Subscriber.
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if atomic.LoadUint32(&s.closed) == 1 {
		return nil, errors.New("subscriber closed")
	}

	output := make(chan *message.Message)
	s.subscribersWg.Add(1)

	go func() {
		defer s.subscribersWg.Done()
		defer close(output)

		// Create a context that is cancelled when the subscriber is closing
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			select {
			case <-s.closing:
				cancel()
			case <-runCtx.Done():
			}
		}()

		// Add topic to consumption
		s.client.AddConsumeTopics(topic)

		// Wait for consumer to be ready (especially important for consumer groups)
		// Do an initial poll with timeout to allow group rebalancing
		readyCtx, readyCancel := context.WithTimeout(runCtx, 10*time.Second)
		fetches := s.client.PollFetches(readyCtx)
		readyCancel()

		// Log any initial errors but don't fail - consumer may still be joining
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				if err.Err != context.DeadlineExceeded && err.Err != context.Canceled {
					s.logger.Debug("Initial poll error (expected during startup)", watermill.LogFields{
						"error":     err.Err.Error(),
						"topic":     err.Topic,
						"partition": err.Partition,
					})
				}
			}
		}

		// Small delay to ensure consumer is fully ready
		time.Sleep(100 * time.Millisecond)

		for {
			// Poll for records
			fetches := s.client.PollFetches(runCtx)

			// Check if we should exit
			if fetches.IsClientClosed() {
				select {
				case <-s.closing:
					// Subscriber is closing, exit gracefully
					return
				default:
					// Client closed but subscriber not - this can happen during startup
					// or reconnection. Wait a bit and continue polling to allow recovery.
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}

			if runCtx.Err() != nil {
				return
			}

			// Handle errors - log them but continue polling to allow recovery
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					// Skip context canceled errors (normal shutdown)
					if err.Err == context.Canceled {
						continue
					}
					// Log all errors but don't exit - franz-go handles retries internally
					s.logger.Debug("Fetch error", watermill.LogFields{
						"error":     err.Err.Error(),
						"topic":     err.Topic,
						"partition": err.Partition,
					})
				}
				// Continue polling - franz-go handles reconnection
				continue
			}

			// Process records
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				if record == nil {
					continue
				}

				msg, err := s.config.Unmarshaler.Unmarshal(record)
				if err != nil {
					s.logger.Error("Cannot unmarshal message", err, nil)
					continue
				}

				// Enrich context with Kafka metadata
				// Use context.Background() as base to avoid carrying over cancellation from runCtx
				recordCtx := setPartitionToCtx(context.Background(), record.Partition)
				recordCtx = setPartitionOffsetToCtx(recordCtx, record.Offset)
				recordCtx = setMessageTimestampToCtx(recordCtx, record.Timestamp)
				recordCtx = setMessageKeyToCtx(recordCtx, record.Key)

				msgCtx, cancelMsg := context.WithCancel(recordCtx)
				msg.SetContext(msgCtx)

				if err := s.handleMessage(msg, output, record, cancelMsg); err != nil {
					return
				}
			}
		}
	}()

	return output, nil
}

func (s *Subscriber) handleMessage(
	msg *message.Message,
	output chan *message.Message,
	record *kgo.Record,
	cancel context.CancelFunc,
) error {
	defer cancel()

ResendLoop:
	for {
		select {
		case output <- msg:
		case <-s.closing:
			return nil
		case <-msg.Context().Done():
			return nil
		}

		select {
		case <-msg.Acked():
			// Mark for commit
			s.client.MarkCommitRecords(record)

			// Manual commit if auto-commit disabled
			if s.config.DisableAutoCommit {
				if err := s.client.CommitRecords(msg.Context(), record); err != nil {
					s.logger.Error("Cannot commit offset", err, nil)
				}
			}
			break ResendLoop

		case <-msg.Nacked():
			// Copy and retry
			msg = msg.Copy()
			msg.SetContext(setPartitionToCtx(
				setPartitionOffsetToCtx(
					setMessageTimestampToCtx(
						setMessageKeyToCtx(context.Background(), record.Key),
						record.Timestamp,
					),
					record.Offset,
				),
				record.Partition,
			))

			if s.config.NackResendSleep > 0 {
				time.Sleep(s.config.NackResendSleep)
			}
			continue ResendLoop

		case <-s.closing:
			return nil
		case <-msg.Context().Done():
			return nil
		}
	}

	return nil
}

// Close implements message.Subscriber.
func (s *Subscriber) Close() error {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return nil
	}

	s.logger.Debug("Subscriber: closing subscriber", nil)
	close(s.closing)
	s.subscribersWg.Wait()
	s.logger.Debug("Subscriber: all subscribers finished, closing client", nil)
	s.client.Close()
	s.logger.Debug("Subscriber: client closed", nil)

	return nil
}

// SubscribeInitialize implements message.SubscribeInitializer.
// It creates the Kafka topic if it doesn't exist.
func (s *Subscriber) SubscribeInitialize(topic string) error {
	if atomic.LoadUint32(&s.closed) == 1 {
		return errors.New("subscriber closed")
	}

	// Create admin client
	adminClient := kadm.NewClient(s.client)
	// Note: Don't close adminClient here - it wraps the shared client
	// and closing it would close the underlying client

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Check if topic exists
	topics, err := adminClient.ListTopics(ctx)
	if err != nil {
		return errors.Wrap(err, "cannot list topics")
	}

	if _, exists := topics[topic]; exists {
		s.logger.Debug("Topic already exists", watermill.LogFields{"topic": topic})
		return nil
	}

	// Create topic with default config (1 partition, replication factor 1)
	resp, err := adminClient.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		return errors.Wrap(err, "cannot create topic")
	}

	if err := resp[topic].Err; err != nil {
		// Topic may already exist (race condition)
		if err == kerr.TopicAlreadyExists {
			s.logger.Debug("Topic already exists", watermill.LogFields{"topic": topic})
			return nil
		}
		return errors.Wrapf(err, "cannot create topic %s", topic)
	}

	s.logger.Info("Created Kafka topic", watermill.LogFields{"topic": topic})
	return nil
}

// Compile-time interface checks
var _ message.Subscriber = (*Subscriber)(nil)
var _ message.SubscribeInitializer = (*Subscriber)(nil)
