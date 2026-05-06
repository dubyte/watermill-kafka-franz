//go:build integration

package integration_test

// Tests in this file verify the Subscriber's at-least-once delivery guarantees:
// all messages delivered, Nack causes redelivery, manual commits, and offset
// commitment behaviour.

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubscriber_AtLeastOnce_AllMessagesDelivered publishes 50 messages,
// subscribes with autoOffsetReset=earliest, acks all of them, and verifies
// that every UUID is received exactly (at-least-once: no missing UUID).
func TestSubscriber_AtLeastOnce_AllMessagesDelivered(t *testing.T) {
	t.Parallel()

	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	pub := newPublisher(t)
	sentUUIDs := publishMessages(t, pub, topic, 50)

	cfg := defaultSubscriberConfig("test-alo-all-" + watermill.NewShortUUID())
	sub := newSubscriber(t, cfg)

	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	msgs := collectMessages(t, ch, 50, 30*time.Second)
	require.Len(t, msgs, 50, "expected all 50 messages to be delivered")

	receivedUUIDs := make([]string, len(msgs))
	for i, m := range msgs {
		receivedUUIDs[i] = m.UUID
	}
	assert.ElementsMatch(t, sentUUIDs, receivedUUIDs,
		"all 50 published UUIDs must be present in received messages")
}

// TestSubscriber_AtLeastOnce_NackCausesRedelivery publishes a single message,
// Nacks it on first delivery, then receives and Acks it on the second delivery.
// Verifies that the same UUID appears twice and the message is eventually committed.
func TestSubscriber_AtLeastOnce_NackCausesRedelivery(t *testing.T) {
	t.Parallel()

	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	pub := newPublisher(t)
	sent := publishMessages(t, pub, topic, 1)

	cfg := defaultSubscriberConfig("test-alo-nack-" + watermill.NewShortUUID())
	sub := newSubscriber(t, cfg)

	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// First delivery: Nack.
	var firstUUID string
	select {
	case msg, ok := <-ch:
		require.True(t, ok, "channel must be open")
		firstUUID = msg.UUID
		msg.Nack()
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for first delivery")
	}
	assert.Equal(t, sent[0], firstUUID, "first delivery UUID must match the published UUID")

	// Second delivery: Ack.
	var secondUUID string
	select {
	case msg, ok := <-ch:
		require.True(t, ok, "channel must still be open after Nack")
		secondUUID = msg.UUID
		msg.Ack()
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for redelivery after Nack")
	}
	assert.Equal(t, sent[0], secondUUID,
		"redelivery UUID must match the published UUID")
	assert.Equal(t, firstUUID, secondUUID,
		"the same message must be redelivered after a Nack")
}

// TestSubscriber_AtLeastOnce_NackWithSleep verifies that NackResendSleep
// introduces a measurable delay before the message is redelivered.
func TestSubscriber_AtLeastOnce_NackWithSleep(t *testing.T) {
	t.Parallel()

	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	pub := newPublisher(t)
	publishMessages(t, pub, topic, 1)

	const nackSleep = 200 * time.Millisecond

	cfg := defaultSubscriberConfig("test-alo-nacksleep-" + watermill.NewShortUUID())
	cfg.NackResendSleep = nackSleep
	sub := newSubscriber(t, cfg)

	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// First delivery: record the time and Nack.
	var nackTime time.Time
	select {
	case msg, ok := <-ch:
		require.True(t, ok)
		nackTime = time.Now()
		msg.Nack()
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for first delivery")
	}

	// Redelivery: measure the elapsed time since the Nack.
	select {
	case msg, ok := <-ch:
		require.True(t, ok)
		elapsed := time.Since(nackTime)
		msg.Ack()
		assert.GreaterOrEqual(t, elapsed, nackSleep,
			"redelivery should not happen before NackResendSleep (%s) elapses; elapsed=%s",
			nackSleep, elapsed)
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for redelivery after Nack")
	}
}

// TestSubscriber_AtLeastOnce_MultiplePartitions publishes 30 messages to a
// topic with 3 partitions, subscribes without a consumer group (assigns all
// partitions), acks all, and verifies all 30 UUIDs are received.
func TestSubscriber_AtLeastOnce_MultiplePartitions(t *testing.T) {
	t.Parallel()

	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 3)

	pub := newPublisher(t)
	sentUUIDs := publishMessages(t, pub, topic, 30)

	// No consumer group: subscriber assigns all partitions directly.
	cfg := defaultSubscriberConfig("")
	sub := newSubscriber(t, cfg)

	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	msgs := collectMessages(t, ch, 30, 30*time.Second)
	require.Len(t, msgs, 30, "expected all 30 messages across 3 partitions")

	receivedUUIDs := make([]string, len(msgs))
	for i, m := range msgs {
		receivedUUIDs[i] = m.UUID
	}
	assert.ElementsMatch(t, sentUUIDs, receivedUUIDs,
		"all 30 UUIDs must be received from the 3-partition topic")
}

// TestSubscriber_AtLeastOnce_OffsetCommittedAfterAck verifies that offsets are
// committed after Ack so that, after Close() and re-subscribe with the same
// consumer group, already-acked messages are NOT redelivered.
//
// Timeline:
//  1. Publish 3 messages.
//  2. Subscribe; receive+ack msg1 only.
//  3. Wait 6 s (> default AutoCommitInterval of 5 s) for the offset to flush.
//  4. Close subscriber.
//  5. Re-subscribe with same group; earliest reset.
//  6. Assert msg1 is NOT redelivered; msg2 and msg3 ARE delivered.
func TestSubscriber_AtLeastOnce_OffsetCommittedAfterAck(t *testing.T) {
	// Not parallel: depends on timing (auto-commit interval).

	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	pub := newPublisher(t)
	sent := publishMessages(t, pub, topic, 3)

	group := "test-alo-commit-" + watermill.NewShortUUID()

	// --- Phase 1: subscribe and ack only msg1 ---
	cfg1 := defaultSubscriberConfig(group)
	cfg1.AutoCommitInterval = 1 * time.Second // shorter than default to speed up test

	sub1 := newSubscriber(t, cfg1)

	ch1, err := sub1.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	var msg1 *message.Message
	select {
	case m, ok := <-ch1:
		require.True(t, ok)
		msg1 = m
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for msg1")
	}
	assert.Equal(t, sent[0], msg1.UUID)
	msg1.Ack()

	// Wait for auto-commit to flush the acked offset (> AutoCommitInterval).
	time.Sleep(cfg1.AutoCommitInterval*2 + 500*time.Millisecond)

	// Close sub1 cleanly.
	require.NoError(t, sub1.Close())
	for range ch1 {
	} // drain to let the goroutine exit

	// --- Phase 2: re-subscribe with same group ---
	cfg2 := defaultSubscriberConfig(group)
	sub2 := newSubscriber(t, cfg2)

	ch2, err := sub2.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// Collect exactly 2 messages (msg2 and msg3). msg1 must NOT appear.
	msgs := collectMessages(t, ch2, 2, 20*time.Second)
	require.Len(t, msgs, 2, "expected exactly msg2 and msg3 from re-subscribe")

	receivedUUIDs := make([]string, len(msgs))
	for i, m := range msgs {
		receivedUUIDs[i] = m.UUID
	}
	assert.ElementsMatch(t, sent[1:], receivedUUIDs,
		"only msg2 and msg3 should be redelivered; msg1 offset was committed")
	assert.NotContains(t, receivedUUIDs, sent[0],
		"msg1 must NOT be redelivered because its offset was committed")
}

// TestSubscriber_AtLeastOnce_NoDuplicatesOnCleanStop publishes 20 messages,
// subscribes and acks all 20, calls Stop() then Close(), then re-subscribes
// with the same consumer group and verifies no messages are redelivered.
func TestSubscriber_AtLeastOnce_NoDuplicatesOnCleanStop(t *testing.T) {
	t.Parallel()

	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	pub := newPublisher(t)
	publishMessages(t, pub, topic, 20)

	group := "test-alo-nodup-stop-" + watermill.NewShortUUID()

	// --- Phase 1: subscribe, ack all 20, then Stop+Close ---
	cfg1 := defaultSubscriberConfig(group)
	cfg1.AutoCommitInterval = 500 * time.Millisecond

	sub1 := newSubscriber(t, cfg1)
	ch1, err := sub1.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	msgs1 := collectMessages(t, ch1, 20, 30*time.Second)
	require.Len(t, msgs1, 20, "expected all 20 messages in phase 1")

	// Wait for auto-commit to flush the marks.
	time.Sleep(2 * time.Second)

	require.NoError(t, sub1.Stop())
	require.NoError(t, sub1.Close())
	for range ch1 {
	}

	// --- Phase 2: re-subscribe; no messages should be redelivered ---
	cfg2 := defaultSubscriberConfig(group)
	sub2 := newSubscriber(t, cfg2)

	ch2, err := sub2.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// Give a generous window; any message arriving here is a duplicate.
	redelivered := collectMessages(t, ch2, 1, 8*time.Second)
	assert.Empty(t, redelivered,
		"no messages should be redelivered after a clean Stop()+Close() with committed offsets")
}

// TestSubscriber_DisableAutoCommit_ManualCommit verifies that when
// DisableAutoCommit=true offsets are committed on Ack, so re-subscribing with
// the same group does not replay already-processed messages.
func TestSubscriber_DisableAutoCommit_ManualCommit(t *testing.T) {
	t.Parallel()

	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	pub := newPublisher(t)
	sent := publishMessages(t, pub, topic, 3)

	group := "test-alo-manual-commit-" + watermill.NewShortUUID()

	// --- Phase 1: subscribe with DisableAutoCommit, ack all 3 ---
	cfg1 := defaultSubscriberConfig(group)
	cfg1.DisableAutoCommit = true

	sub1 := newSubscriber(t, cfg1)
	ch1, err := sub1.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	msgs1 := collectMessages(t, ch1, 3, 20*time.Second)
	require.Len(t, msgs1, 3, "expected all 3 messages in phase 1")

	uuids1 := make([]string, len(msgs1))
	for i, m := range msgs1 {
		uuids1[i] = m.UUID
	}
	assert.ElementsMatch(t, sent, uuids1)

	require.NoError(t, sub1.Close())
	for range ch1 {
	}

	// --- Phase 2: re-subscribe; offsets committed manually, no redelivery ---
	cfg2 := defaultSubscriberConfig(group)
	sub2 := newSubscriber(t, cfg2)

	ch2, err := sub2.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	redelivered := collectMessages(t, ch2, 1, 8*time.Second)
	assert.Empty(t, redelivered,
		"no messages should be redelivered: offsets were committed manually via DisableAutoCommit")
}
