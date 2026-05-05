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

func TestNewSubscriber_OTelEnabled_SetsKotelService(t *testing.T) {
	t.Parallel()

	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}
	config.OTelEnabled = true

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)

	require.NoError(t, err)
	assert.NotNil(t, subscriber)
	assert.NotNil(t, subscriber.kotelService, "kotelService must be set when OTelEnabled=true")

	_ = subscriber.Close()
}

func TestNewSubscriber_OTelDisabled_KotelServiceNil(t *testing.T) {
	t.Parallel()

	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}
	config.OTelEnabled = false

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)

	require.NoError(t, err)
	assert.NotNil(t, subscriber)
	assert.Nil(t, subscriber.kotelService, "kotelService must be nil when OTelEnabled=false")

	_ = subscriber.Close()
}
