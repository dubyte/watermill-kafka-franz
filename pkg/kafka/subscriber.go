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
	logger watermill.LoggerAdapter

	// adminClient is used for SubscribeInitialize
	adminClient *kgo.Client

	closing       chan struct{}
	subscribersWg sync.WaitGroup
	closed        uint32 // atomic

	subClientsMu sync.Mutex
	subClients   []*kgo.Client
}

// NewSubscriber creates a new Kafka Subscriber.
func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config = setSubscriberDefaults(config)

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	// Create an admin client for SubscribeInitialize
	adminOpts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ClientID(config.ClientID + "-admin"),
	}
	if config.TLS != nil {
		adminOpts = append(adminOpts, kgo.DialTLSConfig(config.TLS))
	}
	if config.SASLMechanism != nil {
		adminOpts = append(adminOpts, kgo.SASL(config.SASLMechanism))
	}
	adminOpts = append(adminOpts, config.OverwriteKgoOpts...)

	adminClient, err := kgo.NewClient(adminOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create admin kafka client")
	}

	return &Subscriber{
		config:      config,
		logger:      logger,
		adminClient: adminClient,
		closing:     make(chan struct{}),
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

	// Create a new client for this subscription to ensure isolation.
	// Dedicated clients are used to prevent cross-topic message "stealing"
	// in concurrent polling scenarios.
	opts := s.subscriberOptions(topic)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create kafka client")
	}

	s.subClientsMu.Lock()
	s.subClients = append(s.subClients, client)
	s.subClientsMu.Unlock()

	output := make(chan *message.Message)
	s.subscribersWg.Add(1)

	go func() {
		defer s.subscribersWg.Done()
		defer close(output)
		defer client.Close()

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

		// Wait for consumer group to join and get partition assignments
		// This is critical for tests to ensure consumer is ready before messages are published
		time.Sleep(2 * time.Second)

		for {
			// Poll for records
			fetches := client.PollFetches(runCtx)

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
				// Use context.WithoutCancel to preserve values but avoid carrying over cancellation
				recordCtx := ContextWithPartition(context.WithoutCancel(runCtx), record.Partition)
				recordCtx = ContextWithOffset(recordCtx, record.Offset)
				recordCtx = ContextWithTimestamp(recordCtx, record.Timestamp)
				recordCtx = ContextWithKey(recordCtx, record.Key)

				msgCtx, cancelMsg := context.WithCancel(recordCtx)
				msg.SetContext(msgCtx)

				if err := s.handleMessage(runCtx, client, msg, output, record, cancelMsg); err != nil {
					return
				}
			}
		}
	}()

	return output, nil
}

func (s *Subscriber) subscriberOptions(topic string) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.config.Brokers...),
		kgo.FetchMinBytes(s.config.FetchMinBytes),
		kgo.FetchMaxBytes(s.config.FetchMaxBytes),
		kgo.FetchMaxPartitionBytes(s.config.FetchMaxPartitionBytes),
		kgo.FetchMaxWait(s.config.FetchMaxWait),
		kgo.ClientID(s.config.ClientID),
		kgo.Rack(s.config.RackID),
		kgo.ConsumeTopics(topic),
		kgo.AllowAutoTopicCreation(),
	}

	// Consumer group configuration
	if s.config.ConsumerGroup != "" {
		opts = append(opts,
			kgo.ConsumerGroup(s.config.ConsumerGroup),
			kgo.HeartbeatInterval(s.config.HeartbeatInterval),
			kgo.SessionTimeout(s.config.SessionTimeout),
			kgo.RebalanceTimeout(s.config.RebalanceTimeout),
		)

		// Auto-commit configuration
		if !s.config.DisableAutoCommit {
			opts = append(opts,
				kgo.AutoCommitMarks(),
				kgo.AutoCommitInterval(s.config.AutoCommitInterval),
			)
		}
	}

	// Offset reset policy
	switch s.config.AutoOffsetReset {
	case "earliest":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	case "latest":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	case "none":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtCommitted()))
	}

	if s.config.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(s.config.TLS))
	}

	if s.config.SASLMechanism != nil {
		opts = append(opts, kgo.SASL(s.config.SASLMechanism))
	}

	// Allow overriding with custom opts
	opts = append(opts, s.config.OverwriteKgoOpts...)

	return opts
}

func (s *Subscriber) handleMessage(
	ctx context.Context,
	client *kgo.Client,
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
			client.MarkCommitRecords(record)

			// Manual commit if auto-commit disabled
			if s.config.DisableAutoCommit {
				if err := client.CommitRecords(msg.Context(), record); err != nil {
					s.logger.Error("Cannot commit offset", err, nil)
				}
			}
			break ResendLoop

		case <-msg.Nacked():
			// Copy and retry
			msg = msg.Copy()
			msg.SetContext(ContextWithPartition(
				ContextWithOffset(
					ContextWithTimestamp(
						ContextWithKey(ctx, record.Key),
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

	s.subClientsMu.Lock()
	for _, client := range s.subClients {
		client.Close()
	}
	s.subClients = nil
	s.subClientsMu.Unlock()

	s.adminClient.Close()
	s.logger.Debug("Subscriber: all clients closed", nil)

	return nil
}

// SubscribeInitialize implements message.SubscribeInitializer.
// It creates the Kafka topic if it doesn't exist.
func (s *Subscriber) SubscribeInitialize(topic string) error {
	if atomic.LoadUint32(&s.closed) == 1 {
		return errors.New("subscriber closed")
	}

	// Create admin client
	adminClient := kadm.NewClient(s.adminClient)

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
