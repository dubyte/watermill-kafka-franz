package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewPublisher_ValidConfig(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)

	require.NoError(t, err)
	assert.NotNil(t, publisher)
	assert.NotNil(t, publisher.client)
	assert.NotNil(t, publisher.config.Marshaler)
	assert.Equal(t, logger, publisher.logger)

	// Clean up
	err = publisher.Close()
	assert.NoError(t, err)
}

func TestNewPublisher_NilLogger(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	publisher, err := NewPublisher(config, nil)

	require.NoError(t, err)
	assert.NotNil(t, publisher)
	assert.NotNil(t, publisher.logger)

	// Clean up
	err = publisher.Close()
	assert.NoError(t, err)
}

func TestNewPublisher_InvalidBrokers(t *testing.T) {
	config := PublisherConfig{
		Brokers: []string{},
	}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)

	assert.Error(t, err)
	assert.Nil(t, publisher)
	assert.Contains(t, err.Error(), "brokers must not be empty")
}

func TestPublisher_Publish_SingleMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)
	require.NoError(t, err)
	defer func() { _ = publisher.Close() }()

	topic := "test-topic-" + watermill.NewUUID()

	// Initialize topic to avoid race conditions with auto-creation
	sub, err := NewSubscriber(SubscriberConfig{Brokers: config.Brokers}, logger)
	require.NoError(t, err)
	err = sub.SubscribeInitialize(topic)
	require.NoError(t, err)
	_ = sub.Close()

	msg := message.NewMessage(watermill.NewUUID(), []byte("test payload"))
	msg.Metadata.Set("key1", "value1")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msg.SetContext(ctx)

	err = publisher.Publish(topic, msg)
	require.NoError(t, err)
}

func TestPublisher_Publish_MultipleMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)
	require.NoError(t, err)
	defer func() { _ = publisher.Close() }()

	topic := "test-topic-" + watermill.NewUUID()

	// Initialize topic to avoid race conditions with auto-creation
	sub, err := NewSubscriber(SubscriberConfig{Brokers: config.Brokers}, logger)
	require.NoError(t, err)
	err = sub.SubscribeInitialize(topic)
	require.NoError(t, err)
	_ = sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgs := []*message.Message{
		message.NewMessage(watermill.NewUUID(), []byte("payload 1")),
		message.NewMessage(watermill.NewUUID(), []byte("payload 2")),
		message.NewMessage(watermill.NewUUID(), []byte("payload 3")),
	}

	for _, msg := range msgs {
		msg.SetContext(ctx)
	}

	err = publisher.Publish(topic, msgs...)
	require.NoError(t, err)
}

func TestPublisher_Publish_EmptyMessages(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)
	require.NoError(t, err)
	defer func() { _ = publisher.Close() }()

	// Publishing zero messages should not error
	err = publisher.Publish("test-topic")
	assert.NoError(t, err)

	err = publisher.Publish("test-topic", []*message.Message{}...)
	assert.NoError(t, err)
}

func TestPublisher_Publish_ClosedPublisher(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)
	require.NoError(t, err)

	// Close the publisher
	err = publisher.Close()
	require.NoError(t, err)

	// Attempt to publish after close
	msg := message.NewMessage(watermill.NewUUID(), []byte("test"))
	topic := "test-topic-" + watermill.NewUUID()
	err = publisher.Publish(topic, msg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "publisher closed")
}

func TestPublisher_Close_Idempotent(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)
	require.NoError(t, err)

	// First close
	err = publisher.Close()
	assert.NoError(t, err)
	assert.True(t, publisher.closed)

	// Second close should not error
	err = publisher.Close()
	assert.NoError(t, err)
}

func TestPublisher_Publish_WithCustomMarshaler(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}
	config.Marshaler = DefaultMarshaler{}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)
	require.NoError(t, err)
	defer func() { _ = publisher.Close() }()

	assert.Equal(t, config.Marshaler, publisher.config.Marshaler)
}

func TestPublisher_Publish_MarshalError(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}
	config.Marshaler = &failingMarshaler{}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)
	require.NoError(t, err)
	defer func() { _ = publisher.Close() }()

	msg := message.NewMessage(watermill.NewUUID(), []byte("test"))
	topic := "test-topic-" + watermill.NewUUID()
	err = publisher.Publish(topic, msg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot marshal message")
}

// failingMarshaler is a test marshaler that always returns an error
type failingMarshaler struct{}

func (f *failingMarshaler) Marshal(topic string, msg *message.Message) (*kgo.Record, error) {
	return nil, errors.New("marshal failed")
}

func TestNewPublisher_OTelEnabled_Succeeds(t *testing.T) {
	t.Parallel()

	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}
	config.OTelEnabled = true

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)

	require.NoError(t, err)
	assert.NotNil(t, publisher)

	_ = publisher.Close()
}
