package kafka

import (
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Unit tests for Publisher — no broker required.
// Broker-required tests live in publisher_integration_test.go.

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

	assert.NoError(t, publisher.Close())
}

func TestNewPublisher_NilLogger(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	publisher, err := NewPublisher(config, nil)

	require.NoError(t, err)
	assert.NotNil(t, publisher)
	assert.NotNil(t, publisher.logger)

	assert.NoError(t, publisher.Close())
}

func TestNewPublisher_InvalidBrokers(t *testing.T) {
	config := PublisherConfig{
		Brokers: []string{},
	}

	publisher, err := NewPublisher(config, watermill.NewStdLogger(false, false))

	assert.Error(t, err)
	assert.Nil(t, publisher)
	assert.Contains(t, err.Error(), "brokers must not be empty")
}

func TestPublisher_Publish_EmptyMessages(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	publisher, err := NewPublisher(config, watermill.NewStdLogger(false, false))
	require.NoError(t, err)
	defer func() { _ = publisher.Close() }()

	assert.NoError(t, publisher.Publish("test-topic"))
	assert.NoError(t, publisher.Publish("test-topic", []*message.Message{}...))
}

func TestPublisher_Publish_ClosedPublisher(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	publisher, err := NewPublisher(config, watermill.NewStdLogger(false, false))
	require.NoError(t, err)
	require.NoError(t, publisher.Close())

	msg := message.NewMessage(watermill.NewUUID(), []byte("test"))
	err = publisher.Publish("test-topic-"+watermill.NewUUID(), msg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "publisher closed")
}

func TestPublisher_Close_Idempotent(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}

	publisher, err := NewPublisher(config, watermill.NewStdLogger(false, false))
	require.NoError(t, err)

	assert.NoError(t, publisher.Close())
	assert.True(t, publisher.closed)
	assert.NoError(t, publisher.Close())
}

func TestPublisher_Publish_WithCustomMarshaler(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}
	config.Marshaler = DefaultMarshaler{}

	publisher, err := NewPublisher(config, watermill.NewStdLogger(false, false))
	require.NoError(t, err)
	defer func() { _ = publisher.Close() }()

	assert.Equal(t, config.Marshaler, publisher.config.Marshaler)
}

func TestPublisher_Publish_MarshalError(t *testing.T) {
	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}
	config.Marshaler = &failingMarshaler{}

	publisher, err := NewPublisher(config, watermill.NewStdLogger(false, false))
	require.NoError(t, err)
	defer func() { _ = publisher.Close() }()

	msg := message.NewMessage(watermill.NewUUID(), []byte("test"))
	err = publisher.Publish("test-topic-"+watermill.NewUUID(), msg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot marshal message")
}

func TestNewPublisher_OTelEnabled_Succeeds(t *testing.T) {
	t.Parallel()

	config := DefaultPublisherConfig()
	config.Brokers = []string{"127.0.0.1:9092"}
	config.OTelEnabled = true

	publisher, err := NewPublisher(config, watermill.NewStdLogger(false, false))

	require.NoError(t, err)
	assert.NotNil(t, publisher)

	_ = publisher.Close()
}

// failingMarshaler always returns an error — used to test marshal-error handling.
type failingMarshaler struct{}

func (f *failingMarshaler) Marshal(_ string, _ *message.Message) (*kgo.Record, error) {
	return nil, errors.New("marshal failed")
}
