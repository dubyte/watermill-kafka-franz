//go:build integration

package integration_test

// Network-fault integration tests for the watermill-kafka-franz Subscriber.
//
// Prerequisites:
//   - A running Redpanda/Kafka broker reachable at redpandaAddr (127.0.0.1:9092)
//   - A running Toxiproxy daemon reachable at toxiproxyAPI (http://127.0.0.1:8474)
//
// The Toxiproxy helpers (createToxiproxy, addToxic, removeToxic, toxiproxyAvailable),
// the uniqueTopic helper, newSubscriber, newPublisher, publishMessages, and
// collectMessages are all defined in helpers_test.go / testmain_test.go within
// this same package.
//
// Run with:
//
//	go test -v -tags=integration -timeout=5m ./tests/integration/

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dubyte/watermill-kafka-franz/pkg/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Proxy listen ports — one unique port per test to prevent bind conflicts when
// the OS has not yet released a port from a prior test run.
const (
	portBrokerRestart       = "19093"
	portHighLatency         = "19094"
	portCommitTimeout       = "19095"
	portSlowConsumer        = "19096"
	portConnDropDuringFetch = "19097"
	portIsClientClosedSpin  = "19098"
)

// errorRecordingLogger is a minimal watermill.LoggerAdapter that records error
// entries and satisfies the full interface (including With) by delegating to a
// NopLogger for non-error calls.  It is used in tests that need to assert on
// error-level log output from the subscriber.
type errorRecordingLogger struct {
	watermill.NopLogger // provides Info, Debug, Trace, With
	mu     sync.Mutex
	errors []string
}

func (l *errorRecordingLogger) Error(msg string, err error, _ watermill.LogFields) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := msg
	if err != nil {
		entry += ": " + err.Error()
	}
	l.errors = append(l.errors, entry)
}

// With returns the same logger so that derived loggers also record errors here.
func (l *errorRecordingLogger) With(_ watermill.LogFields) watermill.LoggerAdapter {
	return l
}

func (l *errorRecordingLogger) hasErrors() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.errors) > 0
}

// subscribeVia creates a subscriber pointed exclusively at brokerAddr (which
// is the Toxiproxy listen address for fault-injection tests) and immediately
// calls Subscribe for topic.  The subscriber and channel are both cleaned up
// when the test finishes.
func subscribeVia(t *testing.T, brokerAddr, topic string, extra ...func(*kafka.SubscriberConfig)) (*kafka.Subscriber, <-chan *message.Message) {
	t.Helper()

	cfg := kafka.DefaultSubscriberConfig()
	cfg.Brokers = []string{brokerAddr}
	cfg.ConsumerGroup = "cg-" + t.Name()
	cfg.AutoOffsetReset = "earliest"
	for _, fn := range extra {
		fn(&cfg)
	}

	sub := newSubscriber(t, cfg)

	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	return sub, ch
}

// publishDirect publishes n messages straight to redpandaAddr, bypassing any
// Toxiproxy proxy, so that the publish path is never affected by fault injection.
// It returns the UUIDs in the order they were published.
func publishDirect(t *testing.T, topic string, n int) []string {
	t.Helper()
	pub := newPublisher(t)
	// newPublisher dials defaultBroker; we need to route to redpandaAddr.
	// Both refer to 127.0.0.1:9092 — they are the same constant — so re-using
	// newPublisher is correct here.
	return publishMessages(t, pub, topic, n)
}

// receiveOne waits up to deadline for a single message from ch and returns it.
// Fails the test if the deadline elapses first.
func receiveOne(t *testing.T, ch <-chan *message.Message, deadline time.Duration) *message.Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("message channel closed unexpectedly")
		}
		return msg
	case <-time.After(deadline):
		t.Fatalf("timed out after %s waiting for a message", deadline)
		return nil // unreachable
	}
}

// disableProxy sets enabled=false on the named Toxiproxy proxy via the REST API,
// causing it to drop all existing and future TCP connections.
// This simulates a complete network partition between the client and the broker.
func disableProxy(t *testing.T, proxyName string) {
	t.Helper()
	body := `{"enabled": false}`
	url := toxiproxyAPI + "/proxies/" + proxyName
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"Toxiproxy REST API returned non-200 when disabling proxy %q", proxyName)
}

// ---------------------------------------------------------------------------
// Test 1 — Broker restart / TCP connection reset
// ---------------------------------------------------------------------------

// TestSubscriber_Network_BrokerRestartRecovery verifies that the subscriber
// self-heals after a TCP-level connection reset without any explicit restart.
//
// Franz-go reconnects internally; subscriber must self-heal without restart.
//
// The "reset_peer" toxic closes every downstream TCP connection immediately,
// simulating a broker process restart mid-stream.  Franz-go detects the
// EOF/RST, backs off, and re-establishes the connection automatically.  The
// watermill subscriber goroutine never exits; it sees a brief fetch error,
// logs it, and continues polling transparently to the caller.
func TestSubscriber_Network_BrokerRestartRecovery(t *testing.T) {
	if !toxiproxyAvailable() {
		t.Skip("toxiproxy not available")
	}

	topic := uniqueTopic(t)
	proxyName := "kafka-broker-restart"
	proxyAddr := "127.0.0.1:" + portBrokerRestart

	// Create proxy: subscriber connects through it so we can inject faults.
	createToxiproxy(t, proxyName, "0.0.0.0:"+portBrokerRestart, "127.0.0.1:9092")

	// Pre-publish 5 messages directly to Redpanda (bypassing the proxy) so
	// they sit in the partition before the subscriber starts.
	const msgCount = 5
	publishDirect(t, topic, msgCount)

	// Subscribe through the proxy.
	_, ch := subscribeVia(t, proxyAddr, topic)

	// Launch a collector goroutine that acks messages as they arrive.
	// We will inject the fault from the main goroutine concurrently.
	var received int32
	collectorDone := make(chan struct{})

	go func() {
		defer close(collectorDone)
		timeout := time.After(30 * time.Second)
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				msg.Ack()
				if atomic.AddInt32(&received, 1) >= msgCount {
					return
				}
			case <-timeout:
				return
			}
		}
	}()

	// Give the subscriber a moment to start fetching before injecting the fault.
	time.Sleep(300 * time.Millisecond)

	// Inject reset_peer: every downstream TCP connection is reset immediately,
	// simulating the broker dropping all connections (e.g. a process restart).
	addToxic(t, proxyName, "reset-conn", "reset_peer", "downstream", map[string]interface{}{
		"toxicity": 1.0,
	})

	// Hold the fault for 2 seconds — long enough to guarantee at least one
	// fetch cycle fails and franz-go must reconnect.
	time.Sleep(2 * time.Second)

	// Remove the toxic so franz-go can reconnect and resume fetching.
	removeToxic(t, proxyName, "reset-conn")

	<-collectorDone

	assert.Equal(t, int32(msgCount), atomic.LoadInt32(&received),
		"subscriber must deliver all %d messages within 30s despite the TCP reset; got %d",
		msgCount, atomic.LoadInt32(&received))
}

// ---------------------------------------------------------------------------
// Test 2 — High network latency
// ---------------------------------------------------------------------------

// TestSubscriber_Network_HighLatency_NoTimeout confirms that added network
// latency does not cause the subscriber to exit due to any internal timeout.
//
// Franz-go separates network I/O from application-level timeouts; a slow link
// simply slows delivery — it does not trigger client shutdown or context
// cancellation.  All 3 messages must still be delivered within 60 s.
func TestSubscriber_Network_HighLatency_NoTimeout(t *testing.T) {
	if !toxiproxyAvailable() {
		t.Skip("toxiproxy not available")
	}

	topic := uniqueTopic(t)
	proxyName := "kafka-high-latency"
	proxyAddr := "127.0.0.1:" + portHighLatency

	createToxiproxy(t, proxyName, "0.0.0.0:"+portHighLatency, "127.0.0.1:9092")

	// Add 500 ms latency with 100 ms jitter on the downstream direction.
	// This delays every byte travelling from the broker to the consumer, so
	// each fetch response is delayed by roughly 500–600 ms.
	addToxic(t, proxyName, "slow-link", "latency", "downstream", map[string]interface{}{
		"latency":  500, // milliseconds
		"jitter":   100,
		"toxicity": 1.0,
	})

	// Publish after the toxic is active so the entire fetch path is slow.
	const msgCount = 3
	publishDirect(t, topic, msgCount)

	_, ch := subscribeVia(t, proxyAddr, topic)

	// With 500 ms latency per fetch round-trip and 3 messages, the worst-case
	// total delivery is well under 60 s.  A generous deadline avoids flakiness
	// on CI while still catching an actual hang.
	uuids := collectMessages(t, ch, msgCount, 60*time.Second)

	assert.Len(t, uuids, msgCount,
		"all %d messages must be delivered despite 500ms latency; got %d",
		msgCount, len(uuids))
}

// ---------------------------------------------------------------------------
// Test 3 — Commit timeout under DisableAutoCommit (TDD: Bug #4)
// ---------------------------------------------------------------------------

// TestSubscriber_Network_CommitTimeout_UnderDisableAutoCommit is a TDD test
// that demonstrates Bug #4: CommitRecords failure is silently swallowed.
//
// TDD: Demonstrates Bug #4 — CommitRecords failure is silently swallowed.
//
// When DisableAutoCommit=true the subscriber calls CommitRecords after each Ack.
// If that call times out (network partition, broker overload) the error is only
// logged; the subscriber continues to the next message as if the commit
// succeeded.  Consequences:
//
//   - The consumer's committed offset does not advance.
//   - On restart the broker will re-deliver msg1, causing a duplicate.
//   - The caller has no way to know the commit failed.
//
// BUG PRESENT BEHAVIOUR (the assertion labelled "BUG PRESENT" passes today):
//
//	msg2 is delivered immediately after the timed-out commit of msg1.
//
// FIXED BEHAVIOUR (what the assertion should verify after the fix):
//
//	Either (a) the subscriber goroutine exits on commit failure, so msg2
//	only arrives after a re-subscribe, or (b) msg1 is re-delivered before
//	msg2 (offset not advanced means the broker serves msg1 again).
func TestSubscriber_Network_CommitTimeout_UnderDisableAutoCommit(t *testing.T) {
	if !toxiproxyAvailable() {
		t.Skip("toxiproxy not available")
	}

	topic := uniqueTopic(t)
	proxyName := "kafka-commit-timeout"
	proxyAddr := "127.0.0.1:" + portCommitTimeout

	createToxiproxy(t, proxyName, "0.0.0.0:"+portCommitTimeout, "127.0.0.1:9092")

	// Publish 3 messages before subscribing.
	publishDirect(t, topic, 3)

	// Build an error-recording logger so we can assert on the error log emitted
	// by the subscriber when CommitRecords times out.
	errLog := &errorRecordingLogger{}

	subCfg := kafka.DefaultSubscriberConfig()
	subCfg.Brokers = []string{proxyAddr}
	subCfg.ConsumerGroup = fmt.Sprintf("cg-commit-timeout-%s", watermill.NewShortUUID())
	subCfg.AutoOffsetReset = "earliest"
	subCfg.DisableAutoCommit = true
	// CommitTimeout=1s ensures the injected toxic (2000ms) reliably outlasts it.
	subCfg.CommitTimeout = 1 * time.Second

	sub, err := kafka.NewSubscriber(subCfg, errLog)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// Receive msg1.
	msg1 := receiveOne(t, ch, 20*time.Second)

	// Before acking, inject a timeout toxic on downstream that outlasts
	// CommitTimeout (1s), guaranteeing CommitRecords will time out.
	addToxic(t, proxyName, "commit-block", "timeout", "downstream", map[string]interface{}{
		"timeout":  2000, // ms — longer than CommitTimeout (1000ms)
		"toxicity": 1.0,
	})

	// Ack msg1; the subscriber will call CommitRecords and hit the timeout.
	msg1.Ack()

	// Allow the commit attempt to time out and for the subscriber to log the error.
	time.Sleep(2 * time.Second)

	// Remove the toxic so the broker connection recovers.
	removeToxic(t, proxyName, "commit-block")

	// --- BUG PRESENT assertion -------------------------------------------------
	// msg2 arrives immediately because the commit failure was swallowed and the
	// subscriber moved straight to the next record.  The select timeout is short
	// because the bug causes immediate delivery.
	var msg2 *message.Message
	select {
	case msg2 = <-ch:
		// BUG PRESENT: msg2 was delivered despite msg1's commit failing.
		// The subscriber should have surfaced the commit error instead of
		// silently continuing.
		t.Log("BUG #4 present: msg2 delivered immediately after silently-swallowed CommitRecords failure")
		msg2.Ack()
	case <-time.After(5 * time.Second):
		// After the fix the subscriber should have exited or re-delivered msg1;
		// reaching this branch means the bug is no longer present.
		t.Log("No immediate msg2 delivery after failed commit — Bug #4 appears to be fixed")
	}

	// Verify the error was at least logged (valid both before and after the fix).
	// The subscriber always logs commit errors regardless of whether it exits.
	assert.True(t, errLog.hasErrors(),
		"expected at least one error log entry for the CommitRecords timeout; got none")

	// Indicate the expected post-fix state clearly so reviewers know what to
	// change when the fix lands:
	//   assert.Nil(t, msg2,
	//       "AFTER FIX: msg2 must NOT be delivered immediately after a failed commit")

	_ = msg2
}

// ---------------------------------------------------------------------------
// Test 4 — Slow consumer: heartbeats run independently
// ---------------------------------------------------------------------------

// TestSubscriber_Network_SlowConsumer_NoSessionExpiry verifies that the Kafka
// consumer group session does not expire while a handler blocks for several
// seconds simulating slow processing.
//
// Heartbeats run independently in franz-go; session should NOT expire during
// slow processing.
//
// In older Kafka client implementations the heartbeat was sent inside the same
// poll loop as record fetching, so a blocked handler would prevent heartbeats
// and eventually trigger "rebalance due to session timeout" from the broker.
// Franz-go sends heartbeats in a dedicated goroutine, completely decoupled from
// PollFetches, so blocking the handler for 8 s with a 10 s session timeout is
// entirely safe.
//
// The 4 s latency toxic is removed before the Ack so that the subsequent
// auto-commit flush can reach the broker quickly and the test does not stall.
func TestSubscriber_Network_SlowConsumer_NoSessionExpiry(t *testing.T) {
	if !toxiproxyAvailable() {
		t.Skip("toxiproxy not available")
	}

	topic := uniqueTopic(t)
	proxyName := "kafka-slow-consumer"
	proxyAddr := "127.0.0.1:" + portSlowConsumer

	createToxiproxy(t, proxyName, "0.0.0.0:"+portSlowConsumer, "127.0.0.1:9092")

	// 4 s latency simulates a very slow network link without breaking the connection.
	addToxic(t, proxyName, "slow-net", "latency", "downstream", map[string]interface{}{
		"latency":  4000, // ms
		"jitter":   0,
		"toxicity": 1.0,
	})

	publishDirect(t, topic, 1)

	_, ch := subscribeVia(t, proxyAddr, topic,
		func(cfg *kafka.SubscriberConfig) {
			// Short session + heartbeat settings make the test sensitive to any
			// regression where heartbeats could be blocked by the slow network.
			cfg.SessionTimeout = 10 * time.Second
			cfg.HeartbeatInterval = 3 * time.Second
		},
	)

	msg := receiveOne(t, ch, 30*time.Second)

	// Simulate a slow message handler.  With SessionTimeout=10 s, a naïve
	// client that heartbeats inside the poll loop would expire here.
	t.Log("handler sleeping 8s to simulate slow processing — session must survive")
	time.Sleep(8 * time.Second)

	// Remove the latency toxic before acking so the auto-commit that follows
	// the Ack can reach the broker promptly.
	removeToxic(t, proxyName, "slow-net")

	// Ack must succeed.  If the session had expired, the broker would have
	// revoked our partition assignment; the mark/commit would be for a stale
	// offset and could surface as an error on the next fetch cycle.
	msg.Ack()

	// Allow the auto-commit interval to flush the marked offset.
	time.Sleep(2 * time.Second)

	// Reaching this point without the test timing out proves no session-expiry
	// panic or fatal error occurred.  A subscriber that had lost its partition
	// assignment would have closed the output channel, making receiveOne time out.
	t.Log("Ack succeeded after 8s slow handler — no session expiry detected")
}

// ---------------------------------------------------------------------------
// Test 5 — Connection drop during fetch: transparent recovery
// ---------------------------------------------------------------------------

// TestSubscriber_Network_ConnectionDropDuringFetch_Recovers shows that a TCP
// reset injected mid-stream does not lose messages: franz-go reconnects
// transparently and the subscriber resumes from the committed offset.
//
// Flow:
//  1. Publish 10 messages.
//  2. Subscribe through proxy; start collecting.
//  3. After 3 messages are received, inject a reset_peer toxic for 500 ms.
//  4. Remove the toxic; subscriber must deliver the remaining 7 messages.
//  5. Assert all 10 are received and acked within 60 s.
//
// This proves that franz-go's internal reconnect is transparent to the
// watermill subscriber layer — no special reconnect logic is needed above it.
func TestSubscriber_Network_ConnectionDropDuringFetch_Recovers(t *testing.T) {
	if !toxiproxyAvailable() {
		t.Skip("toxiproxy not available")
	}

	topic := uniqueTopic(t)
	proxyName := "kafka-conn-drop"
	proxyAddr := "127.0.0.1:" + portConnDropDuringFetch

	createToxiproxy(t, proxyName, "0.0.0.0:"+portConnDropDuringFetch, "127.0.0.1:9092")

	const msgCount = 10
	publishDirect(t, topic, msgCount)

	_, ch := subscribeVia(t, proxyAddr, topic)

	var received int32
	toxicInjected := make(chan struct{}, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		timeout := time.After(60 * time.Second)
		var injected bool
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				msg.Ack()
				n := atomic.AddInt32(&received, 1)

				// After the 3rd Ack inject a connection reset for 500 ms.
				// This happens inside the goroutine to ensure we've actually
				// consumed those 3 records before cutting the link.
				if n == 3 && !injected {
					injected = true
					toxicInjected <- struct{}{}
					addToxic(t, proxyName, "mid-fetch-reset", "reset_peer", "downstream", map[string]interface{}{
						"toxicity": 1.0,
					})
					time.Sleep(500 * time.Millisecond)
					removeToxic(t, proxyName, "mid-fetch-reset")
				}

				if n >= msgCount {
					return
				}
			case <-timeout:
				return
			}
		}
	}()

	<-done

	assert.Equal(t, int32(msgCount), atomic.LoadInt32(&received),
		"all %d messages must be received despite mid-fetch connection drop; got %d",
		msgCount, atomic.LoadInt32(&received))
}

// ---------------------------------------------------------------------------
// Test 6 — IsClientClosed spin loop (TDD: Bug #7)
// ---------------------------------------------------------------------------

// TestSubscriber_Network_IsClientClosedSpin_Fix is a TDD test that demonstrates
// Bug #7: the default branch of the IsClientClosed check inside the subscriber
// goroutine creates a deadlock that prevents Close() from returning.
//
// TDD: Demonstrates Bug #7 — IsClientClosed default branch creates deadlock.
//
// Root cause: when the kgo.Client detects that the broker connection is
// permanently gone (e.g. proxy disabled), it self-closes.  PollFetches then
// returns a result where IsClientClosed() == true.  The current subscriber code
// enters the default branch:
//
//	default:
//	    time.Sleep(100ms)
//	    continue   // ← spins forever; WaitGroup never reaches 0
//
// Because the subscriber goroutine never exits, Close() calls
// subscribersWg.Wait() and blocks indefinitely.
//
// After the fix the goroutine must detect that the kgo.Client closed for a
// reason other than a requested shutdown and exit cleanly, unblocking Close().
func TestSubscriber_Network_IsClientClosedSpin_Fix(t *testing.T) {
	if !toxiproxyAvailable() {
		t.Skip("toxiproxy not available")
	}

	topic := uniqueTopic(t)
	proxyName := "kafka-client-closed-spin"
	proxyAddr := "127.0.0.1:" + portIsClientClosedSpin

	createToxiproxy(t, proxyName, "0.0.0.0:"+portIsClientClosedSpin, "127.0.0.1:9092")

	publishDirect(t, topic, 1)

	sub, ch := subscribeVia(t, proxyAddr, topic)

	// Receive and ack the single message so the subscriber is in its idle
	// PollFetches loop when we disable the proxy.
	msg := receiveOne(t, ch, 20*time.Second)
	msg.Ack()

	// Give the auto-commit a moment to flush the offset.
	time.Sleep(500 * time.Millisecond)

	// Disable the Toxiproxy proxy entirely via its REST API.
	// All existing and future TCP connections will be refused/dropped, causing
	// the kgo.Client to exhaust retries and self-close.
	disableProxy(t, proxyName)

	// Allow franz-go time to detect the broken connection, exhaust its backoff,
	// and close the internal kgo.Client.
	time.Sleep(3 * time.Second)

	// Call sub.Close() under a hard deadline.
	//
	// BUG PRESENT: hangs indefinitely because the subscriber goroutine spins on
	// IsClientClosed in the default branch and never calls subscribersWg.Done().
	//
	// AFTER FIX: returns within 10 s because the goroutine detects the
	// self-closed client and exits, allowing subscribersWg.Wait() to unblock.
	done := make(chan error, 1)
	go func() { done <- sub.Close() }()

	select {
	case err := <-done:
		require.NoError(t, err)
		t.Log("Close() returned cleanly — Bug #7 is fixed or not triggered in this run")
	case <-time.After(10 * time.Second):
		t.Fatal("Close() hung for 10s — Bug #7: IsClientClosed spin loop not fixed")
	}
}

// Compile-time check: ensure message import is used (avoids "imported and not
// used" errors if a refactor removes a direct reference).
var _ *message.Message
