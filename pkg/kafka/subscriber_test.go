package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
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

func TestSkipUnmarshalErrorHandler_ReturnsNil(t *testing.T) {
	t.Parallel()

	logger := watermill.NewStdLogger(false, false)
	handler := SkipUnmarshalErrorHandler(logger)

	record := &kgo.Record{Topic: "test-topic", Partition: 0, Offset: 42}
	unmarshalErr := errors.New("invalid protobuf payload")

	result := handler(context.Background(), record, unmarshalErr)
	assert.NoError(t, result, "SkipUnmarshalErrorHandler must return nil to signal skip")
}

func TestUnmarshalErrorHandler_ErrorReturn_StopsSubscriber(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("DLQ unavailable, stop consumer")
	handler := UnmarshalErrorHandler(func(_ context.Context, _ *kgo.Record, _ error) error {
		return sentinel
	})

	record := &kgo.Record{Topic: "test-topic", Partition: 1, Offset: 7}
	unmarshalErr := errors.New("bad message")

	result := handler(context.Background(), record, unmarshalErr)
	assert.ErrorIs(t, result, sentinel, "handler returning non-nil must propagate error to stop the subscriber goroutine")
}

func TestOnUnmarshalError_DefaultIsNil(t *testing.T) {
	t.Parallel()

	cfg := DefaultSubscriberConfig()
	assert.Nil(t, cfg.OnUnmarshalError,
		"OnUnmarshalError must default to nil (fail-fast) — silent skip must be opt-in")
}
