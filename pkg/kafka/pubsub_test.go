package kafka

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func kafkaBrokers() []string {
	return []string{"localhost:9092"}
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, "")
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

func TestPubSub(t *testing.T) {
features := tests.Features{
ConsumerGroups:                      true,
ExactlyOnceDelivery:                 false,
GuaranteedOrder:                     true,
GuaranteedOrderWithSingleSubscriber: true,
Persistent:                          true,
		NewSubscriberReceivesOldMessages:    true,
		ContextPreserved:                    true,
RestartServiceCommand:               []string{"docker", "compose", "restart", "kafka"},
}

	tests.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}
