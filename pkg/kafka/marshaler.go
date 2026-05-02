package kafka

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// UUIDHeaderKey is the reserved header key for storing message UUID.
const UUIDHeaderKey = "_watermill_message_uuid"

// Marshaler converts Watermill messages to Kafka records.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*kgo.Record, error)
}

// Unmarshaler converts Kafka records to Watermill messages.
type Unmarshaler interface {
	Unmarshal(record *kgo.Record) (*message.Message, error)
}

// DefaultMarshaler is the default implementation of Marshaler and Unmarshaler.
type DefaultMarshaler struct{}

// Marshal converts a Watermill message to a Kafka record.
func (DefaultMarshaler) Marshal(topic string, msg *message.Message) (*kgo.Record, error) {
	// Reject reserved header key
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved for message UUID", UUIDHeaderKey)
	}

	// Build headers: UUID header + metadata headers
	headers := make([]kgo.RecordHeader, 0, len(msg.Metadata)+1)
	headers = append(headers, kgo.RecordHeader{
		Key:   UUIDHeaderKey,
		Value: []byte(msg.UUID),
	})

	for key, value := range msg.Metadata {
		headers = append(headers, kgo.RecordHeader{
			Key:   key,
			Value: []byte(value),
		})
	}

	return &kgo.Record{
		Topic:   topic,
		Value:   msg.Payload,
		Headers: headers,
	}, nil
}

// Unmarshal converts a Kafka record to a Watermill message.
func (DefaultMarshaler) Unmarshal(record *kgo.Record) (*message.Message, error) {
	return unmarshalRecord(record)
}

// PartitionedMarshaler is a marshaler that uses message UUID as the Kafka key
// to ensure messages with the same key are routed to the same partition.
// This enables ordered delivery within each partition.
type PartitionedMarshaler struct{}

// Marshal converts a Watermill message to a Kafka record with UUID as key.
func (PartitionedMarshaler) Marshal(topic string, msg *message.Message) (*kgo.Record, error) {
	// Reject reserved header key
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved for message UUID", UUIDHeaderKey)
	}

	// Build headers: UUID header + metadata headers
	headers := make([]kgo.RecordHeader, 0, len(msg.Metadata)+1)
	headers = append(headers, kgo.RecordHeader{
		Key:   UUIDHeaderKey,
		Value: []byte(msg.UUID),
	})

	for key, value := range msg.Metadata {
		headers = append(headers, kgo.RecordHeader{
			Key:   key,
			Value: []byte(value),
		})
	}

	return &kgo.Record{
		Topic:   topic,
		Key:     []byte(msg.UUID), // Use UUID as key for partitioning
		Value:   msg.Payload,
		Headers: headers,
	}, nil
}

// Unmarshal converts a Kafka record to a Watermill message.
func (PartitionedMarshaler) Unmarshal(record *kgo.Record) (*message.Message, error) {
	return unmarshalRecord(record)
}

// unmarshalRecord is the shared implementation for unmarshaling Kafka records
// into Watermill messages. It extracts the UUID from the reserved header and
// maps remaining headers to message metadata.
func unmarshalRecord(record *kgo.Record) (*message.Message, error) {
	var messageID string
	metadata := make(message.Metadata, len(record.Headers))

	for _, header := range record.Headers {
		if header.Key == UUIDHeaderKey {
			messageID = string(header.Value)
		} else {
			metadata.Set(header.Key, string(header.Value))
		}
	}

	msg := message.NewMessage(messageID, record.Value)
	msg.Metadata = metadata

	return msg, nil
}
