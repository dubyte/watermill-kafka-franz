//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

// Watermill compliance tests for Publisher.
// These tests verify that the publisher correctly implements the
// message.Publisher interface against a real Kafka broker.

func TestPublisher_Publish_SingleMessage(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)
	require.NoError(t, err)
	defer func() { _ = publisher.Close() }()

	topic := "test-topic-" + watermill.NewUUID()

	sub, err := NewSubscriber(SubscriberConfig{Brokers: config.Brokers}, logger)
	require.NoError(t, err)
	require.NoError(t, sub.SubscribeInitialize(topic))
	_ = sub.Close()

	msg := message.NewMessage(watermill.NewUUID(), []byte("test payload"))
	msg.Metadata.Set("key1", "value1")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msg.SetContext(ctx)

	require.NoError(t, publisher.Publish(topic, msg))
}

func TestPublisher_Publish_MultipleMessages(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	logger := watermill.NewStdLogger(false, false)
	publisher, err := NewPublisher(config, logger)
	require.NoError(t, err)
	defer func() { _ = publisher.Close() }()

	topic := "test-topic-" + watermill.NewUUID()

	sub, err := NewSubscriber(SubscriberConfig{Brokers: config.Brokers}, logger)
	require.NoError(t, err)
	require.NoError(t, sub.SubscribeInitialize(topic))
	_ = sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgs := []*message.Message{
		message.NewMessage(watermill.NewUUID(), []byte("payload 1")),
		message.NewMessage(watermill.NewUUID(), []byte("payload 2")),
		message.NewMessage(watermill.NewUUID(), []byte("payload 3")),
	}
	for _, m := range msgs {
		m.SetContext(ctx)
	}

	require.NoError(t, publisher.Publish(topic, msgs...))
}
