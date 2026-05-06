//go:build integration

package kafka

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

// Watermill compliance tests — full PubSub contract.
// tests.TestPubSub is the canonical test suite maintained by the watermill
// project that every Publisher+Subscriber pair must pass to claim compliance
// with the watermill messaging contract.

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, "test-"+t.Name())
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(false, false)

	publisherConfig := DefaultPublisherConfig()
	publisherConfig.Brokers = []string{"127.0.0.1:9092"}
	publisher, err := NewPublisher(publisherConfig, logger)
	if err != nil {
		t.Fatalf("failed to create publisher: %v", err)
	}

	subscriberConfig := DefaultSubscriberConfig()
	subscriberConfig.Brokers = []string{"127.0.0.1:9092"}
	subscriberConfig.ConsumerGroup = consumerGroup
	subscriberConfig.AutoOffsetReset = "earliest"

	subscriber, err := NewSubscriber(subscriberConfig, logger)
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	return publisher, subscriber
}

func createNoGroupPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, "")
}

func TestPubSub(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPubSub_ordered(t *testing.T) {
	t.Parallel()

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestNoGroupSubscriber(t *testing.T) {
	t.Parallel()

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                   false,
			ExactlyOnceDelivery:              false,
			GuaranteedOrder:                  false,
			Persistent:                       true,
			NewSubscriberReceivesOldMessages: true,
		},
		createNoGroupPubSub,
		nil,
	)
}
