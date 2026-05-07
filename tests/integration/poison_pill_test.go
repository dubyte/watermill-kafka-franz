//go:build integration

package integration_test

// Tests in this file verify the subscriber's behaviour when an Unmarshaler
// returns an error (a "poison pill" record).
//
// Before fix #2 (MarkCommitRecords on unmarshal error) the subscriber calls
// `continue` without marking the bad offset, pinning the consumer group at the
// poison pill's offset forever and stalling all subsequent messages in that
// partition.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/dubyte/watermill-kafka-franz/pkg/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubscriber_PoisonPill_DoesNotBlockSubsequentMessages verifies that a
// record that fails to unmarshal does not permanently stall the partition.
//
// TDD: This test FAILS before fix #2 (MarkCommitRecords on unmarshal error).
//
// Message layout (single partition):
//
//	offset 0,1,2 — valid
//	offset 3     — POISON (unmarshal error)
//	offset 4,5,6 — valid
//
// With fix #2: the poison pill at offset 3 is skipped and offsets 4-6 arrive.
// Without the fix: the subscriber stops at offset 3 and offsets 4-6 are never
// delivered.
func TestSubscriber_PoisonPill_DoesNotBlockSubsequentMessages(t *testing.T) {
	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	pub := newPublisher(t)

	// Publish three valid messages (offsets 0,1,2).
	firstUUIDs := publishMessages(t, pub, topic, 3)

	// Publish the poison pill at offset 3.
	publishBadMessage(t, topic, 0)

	// Publish three more valid messages (offsets 4,5,6).
	secondUUIDs := publishMessages(t, pub, topic, 3)

	cfg := defaultSubscriberConfig("test-poison-pill-1-" + watermill.NewShortUUID())
	cfg.Unmarshaler = poisonPillUnmarshaler{}
	cfg.AutoCommitInterval = 500 * time.Millisecond

	sub := newSubscriber(t, cfg)

	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// Receive and ack the first three valid messages.
	firstBatch := collectMessages(t, ch, 3, 15*time.Second)
	require.Len(t, firstBatch, 3, "expected first three valid messages (offsets 0-2)")
	assert.ElementsMatch(t, firstUUIDs, uuidsOf(firstBatch))

	// After fix #2 the poison pill at offset 3 is skipped and messages at
	// offsets 4-6 are delivered.  Before the fix this times out.
	secondBatch := collectMessages(t, ch, 3, 10*time.Second)
	assert.Len(t, secondBatch, 3,
		"expected second batch of valid messages (offsets 4-6) after poison pill at offset 3 is skipped")
	assert.ElementsMatch(t, secondUUIDs, uuidsOf(secondBatch))
}

// TestSubscriber_PoisonPill_PartitionDoesNotStallAfterRebalance verifies that
// after a consumer group rebalance the poison pill offset is skipped by the new
// consumer, which then receives the subsequent valid message.
//
// TDD: This test FAILS before fix #2.
//
// Message layout:
//
//	offset 0 — valid
//	offset 1 — POISON
//	offset 2 — valid
//
// Subscribe1 receives offset 0, acks it, and is closed after auto-commit
// flushes.  Subscribe2 re-subscribes to the same group.  With fix #2 it
// receives offset 2 (poison at 1 skipped).  Without the fix it stalls on the
// poison pill at offset 1 and never delivers offset 2.
func TestSubscriber_PoisonPill_PartitionDoesNotStallAfterRebalance(t *testing.T) {
	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	pub := newPublisher(t)
	group := "test-poison-rebalance-" + watermill.NewShortUUID()

	// Publish: valid(0), POISON(1), valid(2).
	firstValid := publishMessages(t, pub, topic, 1)
	publishBadMessage(t, topic, 0)
	thirdValid := publishMessages(t, pub, topic, 1)

	// --- Subscribe1: consume and ack the first valid message ---
	cfg1 := defaultSubscriberConfig(group)
	cfg1.Unmarshaler = poisonPillUnmarshaler{}
	cfg1.AutoCommitInterval = 200 * time.Millisecond

	sub1 := newSubscriber(t, cfg1)

	ch1, err := sub1.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	received1 := collectMessages(t, ch1, 1, 15*time.Second)
	require.Equal(t, firstValid, uuidsOf(received1),
		"Subscribe1 should receive the first valid message")

	// Allow auto-commit to flush the acknowledged offset.
	time.Sleep(500 * time.Millisecond)

	// Close sub1 so it leaves the consumer group cleanly.
	require.NoError(t, sub1.Close())
	// Drain ch1 to completion so the background goroutine exits.
	for range ch1 {
	}

	// --- Subscribe2: re-join same group, expect offset 2 ---
	cfg2 := defaultSubscriberConfig(group)
	cfg2.Unmarshaler = poisonPillUnmarshaler{}
	cfg2.AutoCommitInterval = 200 * time.Millisecond

	sub2 := newSubscriber(t, cfg2)

	ch2, err := sub2.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// With fix #2 the poison pill at offset 1 is skipped and offset 2 arrives.
	// Without the fix this times out.
	received2 := collectMessages(t, ch2, 1, 10*time.Second)
	assert.Equal(t, thirdValid, uuidsOf(received2),
		"Subscribe2 should receive the valid message at offset 2 (poison pill at offset 1 skipped)")
}

// TestSubscriber_PoisonPill_ErrorIsLogged verifies that a poison pill record
// produces at least one error-level log entry containing an informative keyword.
//
// This test also requires fix #2 to pass: it gates on the valid message after
// the poison pill being delivered.  Before the fix that delivery never happens.
func TestSubscriber_PoisonPill_ErrorIsLogged(t *testing.T) {
	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	logger := &capturingLogger{}

	cfg := defaultSubscriberConfig("test-poison-logged-" + watermill.NewShortUUID())
	cfg.Unmarshaler = poisonPillUnmarshaler{}
	cfg.AutoCommitInterval = 300 * time.Millisecond

	sub, err := kafka.NewSubscriber(cfg, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	pub := newPublisher(t)

	// Poison pill first, then a valid message.
	publishBadMessage(t, topic, 0)
	validUUIDs := publishMessages(t, pub, topic, 1)

	// Wait for the valid message — only succeeds with fix #2 applied because the
	// poison pill must have been skipped first.
	received := collectMessages(t, ch, 1, 15*time.Second)
	require.Equal(t, validUUIDs, uuidsOf(received),
		"valid message should be delivered after the poison pill is skipped")

	// Verify that an error was logged for the poison pill.
	errors := logger.errorEntries()
	require.NotEmpty(t, errors, "expected at least one error log entry for the poison pill")

	// At least one error entry must mention "unmarshal" or "skip".
	found := false
	for _, e := range errors {
		lower := strings.ToLower(e.msg)
		if strings.Contains(lower, "unmarshal") || strings.Contains(lower, "skip") {
			found = true
			break
		}
		if e.err != nil {
			lowerErr := strings.ToLower(e.err.Error())
			if strings.Contains(lowerErr, "unmarshal") || strings.Contains(lowerErr, "poison") {
				found = true
				break
			}
		}
	}
	assert.True(t, found,
		"expected an error log entry mentioning 'unmarshal' or 'skip'; got: %+v", errors)
}
