package kafka

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func kafkaBrokers() []string {
	return []string{"127.0.0.1:9092"}
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, "test")
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(false, false)

	publisherConfig := DefaultPublisherConfig()
	publisherConfig.Brokers = kafkaBrokers()
	publisher, err := NewPublisher(publisherConfig, logger)
	if err != nil {
		t.Fatalf("failed to create publisher: %v", err)
	}

	subscriberConfig := DefaultSubscriberConfig()
	subscriberConfig.Brokers = kafkaBrokers()
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
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPubSub_ordered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

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
	if testing.Short() {
		t.Skip("skipping long tests")
	}

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
