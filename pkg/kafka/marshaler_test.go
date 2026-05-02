package kafka

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDefaultMarshaler_Marshal_ReservedHeader(t *testing.T) {
	m := DefaultMarshaler{}

	msg := message.NewMessage("test-uuid", []byte("payload"))
	msg.Metadata.Set(UUIDHeaderKey, "should-fail")

	_, err := m.Marshal("test-topic", msg)
	if err == nil {
		t.Fatal("expected error for reserved header key, got nil")
	}
	if !strings.Contains(err.Error(), "reserved for message UUID") {
		t.Errorf("expected error message to contain 'reserved for message UUID', got: %v", err)
	}
}

func TestDefaultMarshaler_Marshal_BasicMessage(t *testing.T) {
	m := DefaultMarshaler{}

	msg := message.NewMessage("test-uuid", []byte("payload"))

	record, err := m.Marshal("test-topic", msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record == nil {
		t.Fatal("expected record, got nil")
	}

	if record.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got '%s'", record.Topic)
	}
	if !bytes.Equal(record.Value, []byte("payload")) {
		t.Errorf("expected payload 'payload', got '%s'", record.Value)
	}
	if len(record.Headers) != 1 {
		t.Errorf("expected 1 header, got %d", len(record.Headers))
	}
	if record.Headers[0].Key != UUIDHeaderKey {
		t.Errorf("expected header key '%s', got '%s'", UUIDHeaderKey, record.Headers[0].Key)
	}
	if !bytes.Equal(record.Headers[0].Value, []byte("test-uuid")) {
		t.Errorf("expected header value 'test-uuid', got '%s'", record.Headers[0].Value)
	}
}

func TestDefaultMarshaler_Marshal_WithMetadata(t *testing.T) {
	m := DefaultMarshaler{}

	msg := message.NewMessage("test-uuid", []byte("payload"))
	msg.Metadata.Set("key1", "value1")
	msg.Metadata.Set("key2", "value2")

	record, err := m.Marshal("test-topic", msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record == nil {
		t.Fatal("expected record, got nil")
	}

	// Should have UUID header + 2 metadata headers
	if len(record.Headers) != 3 {
		t.Errorf("expected 3 headers, got %d", len(record.Headers))
	}

	// Check UUID header exists
	foundUUID := false
	metadataCount := 0
	for _, h := range record.Headers {
		if h.Key == UUIDHeaderKey {
			foundUUID = true
			if !bytes.Equal(h.Value, []byte("test-uuid")) {
				t.Errorf("expected UUID value 'test-uuid', got '%s'", h.Value)
			}
		} else {
			metadataCount++
		}
	}
	if !foundUUID {
		t.Error("UUID header not found")
	}
	if metadataCount != 2 {
		t.Errorf("expected 2 metadata headers, got %d", metadataCount)
	}
}

func TestDefaultMarshaler_Unmarshal_EmptyHeaders(t *testing.T) {
	m := DefaultMarshaler{}

	record := &kgo.Record{
		Topic:   "test-topic",
		Value:   []byte("payload"),
		Headers: []kgo.RecordHeader{},
	}

	msg, err := m.Unmarshal(record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message, got nil")
	}

	// Without UUID header, message ID should be empty
	if msg.UUID != "" {
		t.Errorf("expected empty UUID, got '%s'", msg.UUID)
	}
	if !bytes.Equal(msg.Payload, []byte("payload")) {
		t.Errorf("expected payload 'payload', got '%s'", msg.Payload)
	}
	if len(msg.Metadata) != 0 {
		t.Errorf("expected empty metadata, got %d items", len(msg.Metadata))
	}
}

func TestDefaultMarshaler_Unmarshal_WithUUID(t *testing.T) {
	m := DefaultMarshaler{}

	record := &kgo.Record{
		Topic: "test-topic",
		Value: []byte("payload"),
		Headers: []kgo.RecordHeader{
			{Key: UUIDHeaderKey, Value: []byte("test-uuid")},
			{Key: "custom-key", Value: []byte("custom-value")},
		},
	}

	msg, err := m.Unmarshal(record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message, got nil")
	}

	if msg.UUID != "test-uuid" {
		t.Errorf("expected UUID 'test-uuid', got '%s'", msg.UUID)
	}
	if !bytes.Equal(msg.Payload, []byte("payload")) {
		t.Errorf("expected payload 'payload', got '%s'", msg.Payload)
	}
	if msg.Metadata.Get("custom-key") != "custom-value" {
		t.Errorf("expected metadata 'custom-key'='custom-value', got '%s'", msg.Metadata.Get("custom-key"))
	}
}

func TestDefaultMarshaler_RoundTrip(t *testing.T) {
	m := DefaultMarshaler{}

	// Create original message
	original := message.NewMessage("roundtrip-uuid", []byte("roundtrip-payload"))
	original.Metadata.Set("meta1", "value1")
	original.Metadata.Set("meta2", "value2")

	// Marshal
	record, err := m.Marshal("test-topic", original)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	// Unmarshal
	restored, err := m.Unmarshal(record)
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	// Verify round-trip integrity
	if restored.UUID != original.UUID {
		t.Errorf("UUID mismatch: expected '%s', got '%s'", original.UUID, restored.UUID)
	}
	if !bytes.Equal(restored.Payload, original.Payload) {
		t.Errorf("payload mismatch: expected '%s', got '%s'", original.Payload, restored.Payload)
	}
	if restored.Metadata.Get("meta1") != original.Metadata.Get("meta1") {
		t.Errorf("meta1 mismatch: expected '%s', got '%s'", original.Metadata.Get("meta1"), restored.Metadata.Get("meta1"))
	}
	if restored.Metadata.Get("meta2") != original.Metadata.Get("meta2") {
		t.Errorf("meta2 mismatch: expected '%s', got '%s'", original.Metadata.Get("meta2"), restored.Metadata.Get("meta2"))
	}
}
