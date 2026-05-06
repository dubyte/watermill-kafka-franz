//go:build integration

package kafka

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Watermill compliance tests for SubscribeInitialize.
// These tests verify that the subscriber correctly implements the
// message.SubscribeInitializer interface against a real Kafka broker.

func TestSubscribeInitialize_CreatesTopic(t *testing.T) {
	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)
	require.NoError(t, err)
	defer func() { _ = subscriber.Close() }()

	topic := "test-subscribe-initialize-" + watermill.NewUUID()

	err = subscriber.SubscribeInitialize(topic)
	require.NoError(t, err)

	// Idempotent — second call must not error.
	err = subscriber.SubscribeInitialize(topic)
	require.NoError(t, err)
}

func TestSubscribeInitialize_TopicAlreadyExists(t *testing.T) {
	config := DefaultSubscriberConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	subscriber, err := NewSubscriber(config, logger)
	require.NoError(t, err)
	defer func() { _ = subscriber.Close() }()

	topic := "test-subscribe-initialize-existing-" + watermill.NewUUID()

	err = subscriber.SubscribeInitialize(topic)
	require.NoError(t, err)

	err = subscriber.SubscribeInitialize(topic)
	assert.NoError(t, err, "second call must be idempotent")
}
