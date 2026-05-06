//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dubyte/watermill-kafka-franz/pkg/kafka"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// shortUUID returns the first 8 characters of a new watermill UUID.
// redpandaAddr, toxiproxyAPI, and toxiproxyKafka are declared in testmain_test.go.
func shortUUID() string {
	return watermill.NewUUID()[:8]
}

// topicSanitizer replaces any character that is not lowercase alphanumeric or
// a dash with a dash.
var topicSanitizer = regexp.MustCompile(`[^a-z0-9-]+`)

// uniqueTopic returns a Kafka-safe topic name that is unique per test.
func uniqueTopic(t *testing.T) string {
	t.Helper()
	raw := "test-" + strings.ToLower(t.Name()) + "-" + shortUUID()
	sanitized := topicSanitizer.ReplaceAllString(raw, "-")
	// Trim any leading/trailing dashes produced by sanitization.
	sanitized = strings.Trim(sanitized, "-")
	return sanitized
}

// defaultSubscriberConfig returns a SubscriberConfig pointing at redpandaAddr
// with earliest offset reset and the given consumer group.
func defaultSubscriberConfig(consumerGroup string) kafka.SubscriberConfig {
	cfg := kafka.DefaultSubscriberConfig()
	cfg.Brokers = []string{redpandaAddr}
	cfg.ConsumerGroup = consumerGroup
	cfg.AutoOffsetReset = "earliest"
	return cfg
}

// newSubscriber creates a Subscriber from config and registers a cleanup that
// closes it when the test ends.
func newSubscriber(t *testing.T, config kafka.SubscriberConfig) *kafka.Subscriber {
	t.Helper()
	logger := watermill.NewStdLogger(false, false)
	sub, err := kafka.NewSubscriber(config, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

// newPublisher creates a Publisher pointing at redpandaAddr and registers a
// cleanup that closes it when the test ends.
func newPublisher(t *testing.T) *kafka.Publisher {
	t.Helper()
	cfg := kafka.DefaultPublisherConfig()
	cfg.Brokers = []string{redpandaAddr}
	cfg.DisableIdempotentWrite = true
	// AllowAutoTopicCreation lets the first produce on a new topic succeed
	// instead of returning UNKNOWN_TOPIC_OR_PARTITION while the broker creates it.
	cfg.OverwriteKgoOpts = []kgo.Opt{kgo.AllowAutoTopicCreation()}
	logger := watermill.NewStdLogger(false, false)
	pub, err := kafka.NewPublisher(cfg, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

// publishMessages publishes count messages to topic using pub and returns their
// UUIDs in order.
func publishMessages(t *testing.T, pub *kafka.Publisher, topic string, count int) []string {
	t.Helper()
	uuids := make([]string, count)
	for i := range count {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("payload-%d", i)))
		require.NoError(t, pub.Publish(topic, msg))
		uuids[i] = msg.UUID
	}
	return uuids
}

// publishBadMessage publishes a raw kgo.Record with no UUID header and the
// sentinel value []byte("POISON") that poisonPillUnmarshaler recognises.
func publishBadMessage(t *testing.T, topic string, partition int32) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(redpandaAddr))
	require.NoError(t, err)
	defer client.Close()

	record := &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Value:     []byte("POISON"),
		// Intentionally no _watermill_message_uuid header.
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, client.ProduceSync(ctx, record).FirstErr())
}

// collectMessages reads up to count messages from ch, acking each one, and
// returns a slice of the received *message.Message pointers.
// It returns early if timeout elapses.
func collectMessages(t *testing.T, ch <-chan *message.Message, count int, timeout time.Duration) []*message.Message {
	t.Helper()
	collected := make([]*message.Message, 0, count)
	deadline := time.After(timeout)
	for len(collected) < count {
		select {
		case msg, ok := <-ch:
			if !ok {
				return collected
			}
			msg.Ack()
			collected = append(collected, msg)
		case <-deadline:
			return collected
		}
	}
	return collected
}

// collectMessagesNoAck reads up to count messages from ch WITHOUT acking them.
// It returns early if timeout elapses.
func collectMessagesNoAck(t *testing.T, ch <-chan *message.Message, count int, timeout time.Duration) []*message.Message {
	t.Helper()
	collected := make([]*message.Message, 0, count)
	deadline := time.After(timeout)
	for len(collected) < count {
		select {
		case msg, ok := <-ch:
			if !ok {
				return collected
			}
			collected = append(collected, msg)
		case <-deadline:
			return collected
		}
	}
	return collected
}

// ackAll acks every message in msgs.
func ackAll(msgs []*message.Message) {
	for _, m := range msgs {
		m.Ack()
	}
}

// ---------------------------------------------------------------------------
// poisonPillUnmarshaler
// ---------------------------------------------------------------------------

// poisonPillUnmarshaler returns an error when the record value equals
// []byte("POISON") and the record carries no UUID header.
// All other records are delegated to kafka.DefaultMarshaler{}.
type poisonPillUnmarshaler struct{}

func (poisonPillUnmarshaler) Unmarshal(record *kgo.Record) (*message.Message, error) {
	if bytes.Equal(record.Value, []byte("POISON")) {
		for _, h := range record.Headers {
			if h.Key == kafka.UUIDHeaderKey {
				// UUID header present — treat as a normal (non-poison) message.
				goto normal
			}
		}
		return nil, fmt.Errorf("poison pill at topic=%s partition=%d offset=%d",
			record.Topic, record.Partition, record.Offset)
	}
normal:
	return kafka.DefaultMarshaler{}.Unmarshal(record)
}

// publishRawPoisonPill publishes a raw record with value "POISON" and no UUID
// header, which will cause poisonPillUnmarshaler to return an error.
func publishRawPoisonPill(t *testing.T, topic string) {
	t.Helper()
	publishBadMessage(t, topic, 0)
}

// ---------------------------------------------------------------------------
// capturingLogger
// ---------------------------------------------------------------------------

// logEntry stores a single captured log entry.
type logEntry struct {
	level  string
	msg    string
	fields watermill.LogFields
	err    error
}

// capturingLogger implements watermill.LoggerAdapter and accumulates all
// entries so that tests can assert on them.
type capturingLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

func (l *capturingLogger) Error(msg string, err error, fields watermill.LogFields) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, logEntry{level: "error", msg: msg, fields: fields, err: err})
}

func (l *capturingLogger) Info(msg string, fields watermill.LogFields) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, logEntry{level: "info", msg: msg, fields: fields})
}

func (l *capturingLogger) Debug(msg string, fields watermill.LogFields) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, logEntry{level: "debug", msg: msg, fields: fields})
}

func (l *capturingLogger) Trace(msg string, fields watermill.LogFields) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, logEntry{level: "trace", msg: msg, fields: fields})
}

// With returns a new capturingLogger that prepends baseFields to every entry.
func (l *capturingLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return &prefixedCapturingLogger{parent: l, base: fields}
}

// prefixedCapturingLogger wraps capturingLogger with additional base fields.
type prefixedCapturingLogger struct {
	parent *capturingLogger
	base   watermill.LogFields
}

func (p *prefixedCapturingLogger) merge(fields watermill.LogFields) watermill.LogFields {
	merged := make(watermill.LogFields, len(p.base)+len(fields))
	for k, v := range p.base {
		merged[k] = v
	}
	for k, v := range fields {
		merged[k] = v
	}
	return merged
}

func (p *prefixedCapturingLogger) Error(msg string, err error, fields watermill.LogFields) {
	p.parent.Error(msg, err, p.merge(fields))
}

func (p *prefixedCapturingLogger) Info(msg string, fields watermill.LogFields) {
	p.parent.Info(msg, p.merge(fields))
}

func (p *prefixedCapturingLogger) Debug(msg string, fields watermill.LogFields) {
	p.parent.Debug(msg, p.merge(fields))
}

func (p *prefixedCapturingLogger) Trace(msg string, fields watermill.LogFields) {
	p.parent.Trace(msg, p.merge(fields))
}

func (p *prefixedCapturingLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return &prefixedCapturingLogger{parent: p.parent, base: p.merge(fields)}
}

// HasDebugEntry returns true if any debug-level entry has a field with the
// given name whose fmt.Sprintf("%v") representation equals value.
func (l *capturingLogger) HasDebugEntry(field, value string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		if e.level != "debug" {
			continue
		}
		if v, ok := e.fields[field]; ok {
			if fmt.Sprintf("%v", v) == value {
				return true
			}
		}
	}
	return false
}

// DebugEntriesWithField returns the LogFields of all debug entries that
// contain the given field name.
func (l *capturingLogger) DebugEntriesWithField(field string) []watermill.LogFields {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []watermill.LogFields
	for _, e := range l.entries {
		if e.level != "debug" {
			continue
		}
		if _, ok := e.fields[field]; ok {
			out = append(out, e.fields)
		}
	}
	return out
}

// errorEntries returns all entries logged at error level.
func (l *capturingLogger) errorEntries() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []logEntry
	for _, e := range l.entries {
		if e.level == "error" {
			out = append(out, e)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// capturedMessages – thread-safe UUID accumulator
// ---------------------------------------------------------------------------

// capturedMessages is a thread-safe container for received message UUIDs,
// useful for at-least-once verification across goroutines.
type capturedMessages struct {
	mu   sync.Mutex
	uids []string
}

// Add appends a UUID.
func (c *capturedMessages) Add(uuid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uids = append(c.uids, uuid)
}

// UUIDs returns a copy of all captured UUIDs.
func (c *capturedMessages) UUIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.uids))
	copy(out, c.uids)
	return out
}

// Len returns the number of captured UUIDs.
func (c *capturedMessages) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.uids)
}

// ---------------------------------------------------------------------------
// createTopicWithPartitions — admin helper
// ---------------------------------------------------------------------------

// createTopicWithPartitions creates a Kafka topic with the specified number of
// partitions and replication factor 1.  It is idempotent.
func createTopicWithPartitions(t *testing.T, topic string, partitions int32) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(redpandaAddr))
	require.NoError(t, err)
	defer client.Close()

	adm := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
	require.NoError(t, err)
	if terr := resp[topic].Err; terr != nil {
		require.Contains(t, terr.Error(), "TOPIC_ALREADY_EXISTS",
			"unexpected error creating topic %s", topic)
	}
}

// drainAndAck starts a goroutine that reads from ch, acks every message, and
// forwards the UUID to the returned channel.  The goroutine exits when ch is
// closed.
func drainAndAck(ch <-chan *message.Message) <-chan string {
	out := make(chan string, 256)
	go func() {
		defer close(out)
		for msg := range ch {
			out <- msg.UUID
			msg.Ack()
		}
	}()
	return out
}

// deleteGroupOffsets deletes the committed offsets for a consumer group on
// partition 0 of the given topic.  This simulates an offset-out-of-range
// scenario where the broker has expired the offsets.
//
// Note: kadm.TopicsSet requires an explicit partition list — a nil slice means
// "no partitions" and would silently delete nothing.
func deleteGroupOffsets(t *testing.T, group, topic string) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(redpandaAddr))
	require.NoError(t, err)
	defer client.Close()

	adm := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err = adm.DeleteOffsets(ctx, group, kadm.TopicsSet{topic: {0: {}}})
	require.NoError(t, err)
}
