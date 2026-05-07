//go:build integration

package integration_test

// Tests in this file cover consumer group rebalancing, offset-out-of-range
// recovery, and the subClients slice growth regression (Bug #3).

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/dubyte/watermill-kafka-franz/pkg/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubscriber_Rebalance_AllMessagesDeliveredWithTwoConsumers starts two
// subscribers in the same consumer group against a 4-partition topic, publishes
// 40 messages, and verifies that every one of the 40 unique UUIDs is delivered
// across the two consumers.  Duplicates during the rebalance window are
// acceptable (at-least-once semantics).
func TestSubscriber_Rebalance_AllMessagesDeliveredWithTwoConsumers(t *testing.T) {
	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 4)

	group := "test-rebalance-two-" + watermill.NewShortUUID()
	pub := newPublisher(t)
	sentUUIDs := publishMessages(t, pub, topic, 40)

	makeSub := func() (<-chan string, func()) {
		cfg := defaultSubscriberConfig(group)
		cfg.AutoCommitInterval = 500 * time.Millisecond
		sub := newSubscriber(t, cfg)
		ch, err := sub.Subscribe(context.Background(), topic)
		require.NoError(t, err)
		uuidCh := drainAndAck(ch)
		return uuidCh, func() { _ = sub.Close() }
	}

	ch1, stop1 := makeSub()
	ch2, stop2 := makeSub()
	defer stop1()
	defer stop2()

	// Collect UUIDs from both subscribers until all 40 are seen (or timeout).
	seen := &capturedMessages{}
	deadline := time.After(30 * time.Second)

	for seen.Len() < len(sentUUIDs) {
		select {
		case uid, ok := <-ch1:
			if ok {
				seen.Add(uid)
			}
		case uid, ok := <-ch2:
			if ok {
				seen.Add(uid)
			}
		case <-deadline:
			t.Fatalf("timed out: received %d/%d UUIDs", seen.Len(), len(sentUUIDs))
		}
	}

	for _, uid := range sentUUIDs {
		assert.Contains(t, seen.UUIDs(), uid, "UUID %s was never delivered", uid)
	}
}

// TestSubscriber_Rebalance_NoDuplicatesAfterStableGroup verifies that after two
// subscribers in a stable consumer group have committed all offsets and been
// closed, a fresh subscriber in the same group receives no messages.
func TestSubscriber_Rebalance_NoDuplicatesAfterStableGroup(t *testing.T) {
	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 2)

	group := "test-rebalance-nodup-" + watermill.NewShortUUID()
	pub := newPublisher(t)
	publishMessages(t, pub, topic, 20)

	// --- Phase 1: consume all 20 messages with two subscribers ---
	makeSub := func() *kafka.Subscriber {
		cfg := defaultSubscriberConfig(group)
		cfg.AutoCommitInterval = 500 * time.Millisecond
		return newSubscriber(t, cfg)
	}

	sub1, sub2 := makeSub(), makeSub()

	ch1, err := sub1.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	ch2, err := sub2.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	uuidCh1 := drainAndAck(ch1)
	uuidCh2 := drainAndAck(ch2)

	collected := &capturedMessages{}
	deadline := time.After(30 * time.Second)
	for collected.Len() < 20 {
		select {
		case uid, ok := <-uuidCh1:
			if ok {
				collected.Add(uid)
			}
		case uid, ok := <-uuidCh2:
			if ok {
				collected.Add(uid)
			}
		case <-deadline:
			t.Fatalf("timed out collecting 20 messages in phase 1: got %d", collected.Len())
		}
	}

	// Allow auto-commit to flush all acknowledged offsets.
	time.Sleep(2 * time.Second)

	require.NoError(t, sub1.Close())
	require.NoError(t, sub2.Close())

	// Drain the UUID channels so the drainAndAck goroutines can exit.
	for range uuidCh1 {
	}
	for range uuidCh2 {
	}

	// --- Phase 2: fresh subscriber in same group should see nothing ---
	cfg3 := defaultSubscriberConfig(group)
	cfg3.AutoCommitInterval = 500 * time.Millisecond
	sub3 := newSubscriber(t, cfg3)

	ch3, err := sub3.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// 6 s window to catch any accidental redelivery.
	redelivered := collectMessages(t, ch3, 1, 6*time.Second)
	assert.Empty(t, redelivered,
		"expected no messages redelivered to a fresh subscriber after all offsets were committed")
}

// TestSubscriber_RollingDeployment_InconsistentGroupProtocol simulates rapid
// consumer group membership churn — start A, stop A, start B, stop B, start C —
// and verifies that subscriber C always self-heals and delivers all messages.
//
// Exploratory: simulates the #1 error seen in production (interac-controller).
// franz-go's cooperative-sticky rebalance protocol is hardcoded, so a true
// INCONSISTENT_GROUP_PROTOCOL cannot be injected via the public API.  Instead
// this test exercises rapid Start/Stop cycling to verify the auto-heal property:
// no matter how chaotic the group membership, a healthy subscriber always
// recovers.
func TestSubscriber_RollingDeployment_InconsistentGroupProtocol(t *testing.T) {
	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 2)

	group := "test-rolling-deploy-" + watermill.NewShortUUID()
	pub := newPublisher(t)
	sentUUIDs := publishMessages(t, pub, topic, 5)

	makeSub := func() *kafka.Subscriber {
		cfg := defaultSubscriberConfig(group)
		cfg.AutoCommitInterval = 500 * time.Millisecond
		// Shorten group-management timeouts so rebalances settle faster during churn.
		cfg.HeartbeatInterval = 1 * time.Second
		cfg.SessionTimeout = 6 * time.Second
		cfg.RebalanceTimeout = 10 * time.Second
		return newSubscriber(t, cfg)
	}

	// Rapid churn: start A, close A, start B, close B — all within ~1 s.
	subA := makeSub()
	chA, err := subA.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, subA.Close())
	for range chA {
	}

	subB := makeSub()
	chB, err := subB.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, subB.Close())
	for range chB {
	}

	// Subscriber C must self-heal after the churn and receive all 5 messages.
	subC := makeSub()
	chC, err := subC.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	received := collectMessages(t, chC, 5, 30*time.Second)
	assert.Len(t, received, 5,
		"subscriber C should receive all messages after group membership churn")

	gotUUIDs := make([]string, len(received))
	for i, m := range received {
		gotUUIDs[i] = m.UUID
	}
	assert.ElementsMatch(t, sentUUIDs, gotUUIDs)
}

// TestSubscriber_OffsetOutOfRange_Recovers verifies that when the committed
// offsets for a consumer group are deleted (simulating broker-side retention
// expiry), a fresh subscriber with autoOffsetReset=earliest re-reads from the
// beginning of the log.
//
// Tests OFFSET_OUT_OF_RANGE recovery — production scenario when retention
// expires under stalled consumer.
func TestSubscriber_OffsetOutOfRange_Recovers(t *testing.T) {
	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	group := "test-oor-" + watermill.NewShortUUID()
	pub := newPublisher(t)
	sentUUIDs := publishMessages(t, pub, topic, 3)

	// --- Phase 1: subscribe, ack offset 0, wait for auto-commit, then close ---
	cfg1 := defaultSubscriberConfig(group)
	cfg1.AutoCommitInterval = 300 * time.Millisecond

	sub1 := newSubscriber(t, cfg1)
	ch1, err := sub1.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	firstBatch := collectMessages(t, ch1, 1, 15*time.Second)
	require.Len(t, firstBatch, 1, "expected exactly one message in first batch")
	assert.Equal(t, sentUUIDs[0], firstBatch[0].UUID)

	// Wait for auto-commit to persist the acked offset.
	time.Sleep(600 * time.Millisecond)
	require.NoError(t, sub1.Close())
	for range ch1 {
	}

	// --- Simulate offset-out-of-range: delete the committed offsets ---
	deleteGroupOffsets(t, group, topic)

	// --- Phase 2: re-subscribe; earliest reset should replay from offset 0 ---
	cfg2 := defaultSubscriberConfig(group)
	cfg2.AutoCommitInterval = 500 * time.Millisecond

	sub2 := newSubscriber(t, cfg2)
	ch2, err := sub2.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	recovered := collectMessages(t, ch2, len(sentUUIDs), 15*time.Second)
	assert.Len(t, recovered, len(sentUUIDs),
		"subscriber should recover all messages from offset 0 after committed offset was deleted")

	recoveredUUIDs := make([]string, len(recovered))
	for i, m := range recovered {
		recoveredUUIDs[i] = m.UUID
	}
	assert.ElementsMatch(t, sentUUIDs, recoveredUUIDs)
}

// TestSubscriber_SliceGrowth_OnContextCancelledResubscribe verifies that
// repeated Subscribe+cancel cycles do not wedge Close() behind an accumulated
// slice of dead kgo.Clients (Bug #3: subClients slice grew unboundedly and was
// iterated in Close()).
//
// The authoritative verification of Bug #3 is code review: the fix removes
// each completed subscription from the slice when its goroutine exits, so the
// slice stays bounded regardless of how many cycles run. Heap-growth assertions
// for this are unreliable (GC timing is advisory and runtime.MemStats is
// coarse), so this test relies on the direct goroutine-exit timing signal:
// after 20 rapid Subscribe+cancel cycles, Close() must return within 2s. A
// regression would accumulate dead clients whose Close() calls serialise in
// the cleanup loop and push the total well past the deadline.
func TestSubscriber_SliceGrowth_OnContextCancelledResubscribe(t *testing.T) {
	topic := uniqueTopic(t)
	createTopicWithPartitions(t, topic, 1)

	sub := newSubscriber(t, defaultSubscriberConfig("test-slice-growth-"+watermill.NewShortUUID()))

	const cycles = 20

	var wg sync.WaitGroup
	for range cycles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			ch, err := sub.Subscribe(ctx, topic)
			if err != nil {
				// Subscriber may already be closed; treat as a no-op.
				cancel()
				return
			}
			cancel()
			for range ch {
			}
		}()
	}
	wg.Wait()

	// Close must finish within 2 s even after many rapid subscribe/cancel
	// cycles. A regression of Bug #3 would serialise Close() of every dead
	// client in the cleanup loop and exceed this deadline.
	closeDone := make(chan struct{})
	go func() {
		_ = sub.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// good
	case <-time.After(2 * time.Second):
		t.Error("Close() took longer than 2 s after 20 subscribe/cancel cycles — possible subClients slice accumulation (Bug #3)")
	}
}

