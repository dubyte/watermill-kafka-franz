//go:build integration

package integration_test

// Tests in this file verify the Subscriber lifecycle: Stop, Close, idempotency,
// SubscribeInitialize, and concurrent access behaviour.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dubyte/watermill-kafka-franz/pkg/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubscriber_StopAllowsInflightToAck verifies that:
//  1. After Stop() the in-flight message can still be Acked.
//  2. Subscribe() after Stop() returns an error containing "stopped".
//  3. Close() after Stop() completes within 5 s.
func TestSubscriber_StopAllowsInflightToAck(t *testing.T) {
	t.Parallel()

	topic := uniqueTopic(t)
	pub := newPublisher(t)
	publishMessages(t, pub, topic, 5)

	cfg := defaultSubscriberConfig("test-stop-inflight-" + watermill.NewShortUUID())
	// Manage lifecycle manually so we control the exact Stop/Close order.
	logger := watermill.NewStdLogger(false, false)
	sub, err := kafka.NewSubscriber(cfg, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// Wait for at least one message.
	var inflight *message.Message
	select {
	case m, ok := <-ch:
		require.True(t, ok, "channel should be open")
		inflight = m
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for first message")
	}

	// Stop the subscriber — the in-flight message should still be ackable.
	require.NoError(t, sub.Stop())

	// Ack must succeed after Stop.
	inflight.Ack()

	// Subscribe after Stop must return an error mentioning "stopped".
	_, err = sub.Subscribe(context.Background(), topic)
	require.Error(t, err, "Subscribe() after Stop() must return an error")
	assert.Contains(t, err.Error(), "stopped")

	// Close must complete within 5 s.
	done := make(chan error, 1)
	go func() { done <- sub.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return within 5s after Stop()")
	}
}

// TestSubscriber_CloseSubscribeRace verifies that concurrent Subscribe() and
// Close() calls do not cause a data race and that all channels returned by
// Subscribe() are eventually closed after Close() returns.
//
// TDD: This test should FAIL (race detected) before fix #1 is applied.
// Run with: go test -race -tags=integration ./tests/integration/ -run TestSubscriber_CloseSubscribeRace
func TestSubscriber_CloseSubscribeRace(t *testing.T) {
	t.Parallel()

	topic := uniqueTopic(t)
	pub := newPublisher(t)
	publishMessages(t, pub, topic, 20)

	cfg := defaultSubscriberConfig("test-close-race-" + watermill.NewShortUUID())
	logger := watermill.NewStdLogger(false, false)
	sub, err := kafka.NewSubscriber(cfg, logger)
	require.NoError(t, err)

	const numGoroutines = 50
	var (
		mu       sync.Mutex
		channels []<-chan *message.Message
		wg       sync.WaitGroup
	)

	// Launch goroutines that all call Subscribe concurrently.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, subErr := sub.Subscribe(context.Background(), topic)
			if subErr != nil {
				// Subscriber may already be closing; normal under race.
				return
			}
			mu.Lock()
			channels = append(channels, ch)
			mu.Unlock()
		}()
	}

	// Concurrently call Close from the main goroutine with a small head start
	// to maximise overlap with the Subscribe goroutines.
	closeErr := make(chan error, 1)
	go func() {
		time.Sleep(5 * time.Millisecond)
		closeErr <- sub.Close()
	}()

	wg.Wait()
	require.NoError(t, <-closeErr, "Close() must not return an error")

	// After Close() returns, every channel obtained via Subscribe() must be
	// closed within 5 s, proving all subscriber goroutines exited cleanly.
	mu.Lock()
	captured := make([]<-chan *message.Message, len(channels))
	copy(captured, channels)
	mu.Unlock()

	for i, ch := range captured {
		timeout := time.After(5 * time.Second)
		// Drain the channel until it is closed.
		drained := false
		for !drained {
			select {
			case _, open := <-ch:
				if !open {
					drained = true
				}
				// If open, keep draining (ack if needed — subscriber closed so no need).
			case <-timeout:
				t.Errorf("channel %d was not closed within 5s of Close() returning", i)
				drained = true // exit the inner loop; the test already failed
			}
		}
	}

	// Close must be idempotent — a second call must return nil, not panic.
	require.NoError(t, sub.Close(), "second Close() call must not error")
}

// TestSubscriber_StopThenClose_NoDeadlock verifies that Stop() followed by
// Close() does not deadlock even when a message is in-flight (not yet acked).
func TestSubscriber_StopThenClose_NoDeadlock(t *testing.T) {
	t.Parallel()

	topic := uniqueTopic(t)
	pub := newPublisher(t)
	publishMessages(t, pub, topic, 3)

	cfg := defaultSubscriberConfig("test-stop-close-nodl-" + watermill.NewShortUUID())
	logger := watermill.NewStdLogger(false, false)
	sub, err := kafka.NewSubscriber(cfg, logger)
	require.NoError(t, err)

	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// Receive a message but deliberately do NOT ack it — it remains in-flight.
	select {
	case _, ok := <-ch:
		require.True(t, ok)
		// Intentionally not acking.
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for in-flight message")
	}

	// Stop() must return immediately.
	stopDone := make(chan struct{})
	go func() {
		require.NoError(t, sub.Stop())
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s")
	}

	// Close() must return within 10 s; in-flight message is dropped on Close.
	closeDone := make(chan error, 1)
	go func() { closeDone <- sub.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close() hung for 10s after Stop() with an in-flight message")
	}
}

// TestSubscriber_MultipleCloseIdempotent verifies that calling Close()
// concurrently from multiple goroutines does not panic and every call returns nil.
func TestSubscriber_MultipleCloseIdempotent(t *testing.T) {
	t.Parallel()

	cfg := defaultSubscriberConfig("")
	logger := watermill.NewStdLogger(false, false)
	sub, err := kafka.NewSubscriber(cfg, logger)
	require.NoError(t, err)

	const n = 5
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = sub.Close()
		}()
	}
	wg.Wait()

	for i, e := range errs {
		assert.NoError(t, e, "Close() call %d should not error", i)
	}
}

// TestSubscriber_SubscribeAfterClose_Rejected verifies that Subscribe() after
// Close() returns an error containing "closed".
func TestSubscriber_SubscribeAfterClose_Rejected(t *testing.T) {
	t.Parallel()

	cfg := defaultSubscriberConfig("")
	logger := watermill.NewStdLogger(false, false)
	sub, err := kafka.NewSubscriber(cfg, logger)
	require.NoError(t, err)

	require.NoError(t, sub.Close())

	_, err = sub.Subscribe(context.Background(), uniqueTopic(t))
	require.Error(t, err, "Subscribe() after Close() must return an error")
	assert.Contains(t, err.Error(), "closed")
}

// TestSubscriber_SubscribeInitialize_CreatesAndIdempotent verifies that
// SubscribeInitialize creates the topic, is idempotent on a second call, and
// that the topic is actually usable for message delivery.
func TestSubscriber_SubscribeInitialize_CreatesAndIdempotent(t *testing.T) {
	t.Parallel()

	topic := uniqueTopic(t)

	cfg := defaultSubscriberConfig("test-init-" + watermill.NewShortUUID())
	sub := newSubscriber(t, cfg)

	// First call: creates the topic.
	require.NoError(t, sub.SubscribeInitialize(topic),
		"first SubscribeInitialize must not error")

	// Second call: topic already exists — must be idempotent.
	require.NoError(t, sub.SubscribeInitialize(topic),
		"second SubscribeInitialize must be idempotent")

	// Subscribe and publish to prove the topic is real and delivers messages.
	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	pub := newPublisher(t)
	sent := publishMessages(t, pub, topic, 1)

	msgs := collectMessages(t, ch, 1, 20*time.Second)
	require.Len(t, msgs, 1, "expected exactly one message from the initialized topic")
	assert.Equal(t, sent[0], msgs[0].UUID)
}
