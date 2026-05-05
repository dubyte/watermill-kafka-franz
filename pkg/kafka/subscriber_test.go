package kafka

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeInitialize_CreatesTopic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)
	require.NoError(t, err)
	defer func() { _ = subscriber.Close() }()

	topic := "test-subscribe-initialize-" + watermill.NewUUID()

	// Initialize the topic
	err = subscriber.SubscribeInitialize(topic)
	require.NoError(t, err)

	// Verify topic was created by initializing again (should not error)
	err = subscriber.SubscribeInitialize(topic)
	require.NoError(t, err)
}

func TestSubscribeInitialize_TopicAlreadyExists(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)
	require.NoError(t, err)
	defer func() { _ = subscriber.Close() }()

	topic := "test-subscribe-initialize-existing-" + watermill.NewUUID()

	// Create topic first time
	err = subscriber.SubscribeInitialize(topic)
	require.NoError(t, err)

	// Second call should not error - topic already exists
	err = subscriber.SubscribeInitialize(topic)
	require.NoError(t, err)
}

func TestSubscribeInitialize_ClosedSubscriber(t *testing.T) {
	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)
	require.NoError(t, err)

	// Close the subscriber
	err = subscriber.Close()
	require.NoError(t, err)

	// Attempt to initialize after close
	err = subscriber.SubscribeInitialize("test-topic")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "subscriber closed")
}

func TestStop_RejectsNewSubscriptions(t *testing.T) {
	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)
	require.NoError(t, err)
	defer func() { _ = subscriber.Close() }()

	err = subscriber.Stop()
	require.NoError(t, err)

	_, err = subscriber.Subscribe(t.Context(), "test-topic")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "subscriber stopped")
}

func TestStop_Idempotent(t *testing.T) {
	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)
	require.NoError(t, err)
	defer func() { _ = subscriber.Close() }()

	require.NoError(t, subscriber.Stop())
	require.NoError(t, subscriber.Stop())
	require.NoError(t, subscriber.Stop())
}

func TestClose_AlsoStops(t *testing.T) {
	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)
	require.NoError(t, err)

	err = subscriber.Close()
	require.NoError(t, err)

	_, err = subscriber.Subscribe(t.Context(), "test-topic")
	assert.Error(t, err)
}

func TestStop_ThenClose(t *testing.T) {
	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)
	require.NoError(t, err)

	require.NoError(t, subscriber.Stop())
	require.NoError(t, subscriber.Close())

	_, err = subscriber.Subscribe(t.Context(), "test-topic")
	assert.Error(t, err)
}
