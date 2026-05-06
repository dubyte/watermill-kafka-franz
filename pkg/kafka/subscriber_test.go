package kafka

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unit tests for Subscriber — no broker required.
// Broker-required tests live in subscriber_integration_test.go.

func TestSubscribeInitialize_ClosedSubscriber(t *testing.T) {
	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)
	require.NoError(t, err)

	err = subscriber.Close()
	require.NoError(t, err)

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
