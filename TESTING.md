# Testing Guide — watermill-kafka-franz

## Overview

This document describes the testing methodology, bug findings, and integration test battery for the `watermill-kafka-franz` subscriber and publisher.

Tests are split into two layers:

| Layer | Location | Broker required | Run with |
|---|---|---|---|
| Unit / short | `pkg/kafka/` | No | `make test-short` |
| Integration | `tests/integration/` | Yes (Redpanda) | `make test-integration` |

---

## Infrastructure

Integration tests run against **Redpanda** (Kafka-compatible, ~2 s startup) plus **Toxiproxy** for network-fault injection.

```
┌──────────────────────────────┐
│  Test process                │
│  127.0.0.1:9092  ─────────────────────────► Redpanda (container: watermill-redpanda)
│  127.0.0.1:19092 ─► Toxiproxy ────────────► Redpanda
│  127.0.0.1:8474  ── Toxiproxy control API  │
└──────────────────────────────┘
```

Start the stack:

```bash
make docker-up          # starts Redpanda + Toxiproxy
make wait-for-redpanda  # blocks until Redpanda is healthy (~10 s)
```

Run integration tests (requires the stack to be up):

```bash
make test-integration   # go test -race -tags integration ./tests/integration/...
```

Run everything together:

```bash
make test               # docker-up + wait + integration tests + docker-down
```

Unit tests (no broker):

```bash
make test-short
```

---

## Subscriber Stop / Close Contract

The intended lifecycle is:

```
Stop()  → signals goroutines to stop fetching new messages
          in-flight messages (already sent to output) can still be Acked/Nacked
          new Subscribe() / SubscribeInitialize() calls are rejected

Close() → calls Stop() internally
          closes s.closing (unblocks any handleMessage waiting for Ack/Nack)
          subscribersWg.Wait() — blocks until all goroutines exit
          closes all kgo.Client handles and the admin client
```

Both methods are safe to call multiple times (idempotent via CAS atomics).

---

## Bug Findings

The following bugs were identified through static analysis and confirmed by TDD tests. Each test is tagged with its bug number.

### Bug 1 — CRITICAL: Subscribe/Close race (WaitGroup contract violated)

**File:** `subscriber.go`

**Root cause:** `subscribersWg.Add(1)` was called *after* the `closed`/`stopped` atomic checks. A concurrent `Close()` could pass `subscribersWg.Wait()` (counter=0) and return while a `Subscribe` goroutine was still starting — a direct violation of Go's `sync.WaitGroup` contract.

**Production impact:** `Close()` returns claiming all goroutines are done, but a live goroutine is still running. Any resource teardown after `Close()` (OTel flush, dependent service shutdown) races with the live goroutine.

**Fix applied:** `subscribersWg.Add(1)` moved to the very top of `Subscribe()`, before any early-return check. Matching `subscribersWg.Done()` calls added to all early-return paths.

**Test:** `TestSubscriber_CloseSubscribeRace` — run with `-race`.

---

### Bug 2 — CRITICAL: Unmarshal failure never commits offset (poison pill)

**File:** `subscriber.go`

**Root cause:** When `Unmarshaler.Unmarshal` returned an error, the record was skipped via `continue` without calling `client.MarkCommitRecords(record)`. With `AutoCommitMarks()`, the committed offset for that partition was permanently frozen before the bad record. After any rebalance or consumer restart, the same record was redelivered and failed again — indefinitely.

**Production impact:** A single malformed message permanently blocks all subsequent messages on the affected partition from being consumed. Silent — nothing after the poison pill is ever delivered.

**Fix applied:** On unmarshal failure, call `client.MarkCommitRecords(record)` (and `client.CommitRecords` under `DisableAutoCommit`) before `continue`, skipping the record at the cost of losing it (correct at-least-once behaviour for undeserializable messages). Log includes topic/partition/offset for operator visibility.

**Tests:** `TestSubscriber_PoisonPill_DoesNotBlockSubsequentMessages`, `TestSubscriber_PoisonPill_PartitionDoesNotStallAfterRebalance`, `TestSubscriber_PoisonPill_ErrorIsLogged`.

---

### Bug 3 — HIGH: subClients slice grows unboundedly

**File:** `subscriber.go`

**Root cause:** `Subscribe()` appended each new `kgo.Client` to `s.subClients` but never removed it when the goroutine exited (e.g., caller context cancelled). `Close()` only nils the slice on final shutdown. In long-running services using Watermill's router restart logic, each reconnect cycle leaked one entry.

**Production impact:** Steady memory growth proportional to reconnect count. On services running for weeks with frequent rebalances, this is significant. Also, `Close()` called `client.Close()` on dead entries (harmless due to franz-go idempotency, but unnecessary).

**Fix applied:** A deferred cleanup in the goroutine removes the client from `s.subClients` (under mutex) before exiting, using the swap-with-last pattern for O(1) removal.

**Test:** `TestSubscriber_SliceGrowth_OnContextCancelledResubscribe`.

---

### Bug 4 — HIGH: CommitRecords failure silently swallowed under DisableAutoCommit

**File:** `subscriber.go`

**Root cause:** When `DisableAutoCommit=true` and `client.CommitRecords` failed (network error, partition rebalanced away), the error was only logged. `handleMessage` returned `nil` and the poll loop continued to the next record. The caller's handler had already processed the message and consumed the Ack signal, but the offset was never committed. On the next restart, all records since the last successful commit would be redelivered.

**Production impact:** Silent reprocessing window. Operators who chose `DisableAutoCommit=true` explicitly want hard commit guarantees — this silently broke that guarantee without any observable signal.

**Fix applied:** `handleMessage` now returns the commit error, which propagates up to the poll loop goroutine (which returns, causing the goroutine to exit). The subscriber's output channel is closed. The caller / Watermill router detects the closed channel and re-subscribes, causing the unacknowledged record to be redelivered from the last successfully committed offset.

**Test:** `TestSubscriber_Network_CommitTimeout_UnderDisableAutoCommit` (Toxiproxy required).

---

### Bug 5 — HIGH: context.DeadlineExceeded logged as spurious "Fetch error"

**File:** `subscriber.go`

**Root cause:** The fetch error filter only skipped `context.Canceled`. When a caller passed a context with a deadline, or when `FetchMaxWait` caused an internal poll timeout, franz-go injected `context.DeadlineExceeded` into the fetch errors. This appeared in logs as `"Fetch error"` at Debug level — noise that looks like a real broker error.

**Fix applied:** Filter updated to `errors.Is(err.Err, context.Canceled) || errors.Is(err.Err, context.DeadlineExceeded)`. Using `errors.Is` for forward compatibility.

**Test:** `TestSubscriber_DeadlineExceededNotLoggedAsFetchError`.

---

### Bug 6 — MEDIUM: AllowRebalance() is dead code

**File:** `subscriber.go`

**Root cause:** `client.AllowRebalance()` is a no-op when `BlockRebalanceOnPoll` is not configured (which it is not, by design — the comment explains why). Franz-go's `allowRebalance()` returns immediately with `if !cl.cfg.blockRebalanceOnPoll { return }`. The call was confusing to readers, implying rebalance-blocking behaviour that does not exist.

**Fix applied:** The call was removed.

**Test:** `TestSubscriber_Rebalance_AllMessagesDeliveredWithTwoConsumers` (regression guard confirming rebalances still work).

---

### Bug 7 — MEDIUM: IsClientClosed default branch creates CPU spin / deadlock

**File:** `subscriber.go`

**Root cause:** When `fetches.IsClientClosed()` returned true and `s.closing` was not set, the `default` branch slept 100 ms then called `PollFetches` again on a closed client — which returns `IsClientClosed()=true` immediately. This created a 10 Hz spin loop that never exited and held `subscribersWg` at 1, causing `Close()` to block forever (deadlock). The comment claiming this handled "startup or reconnection" was factually incorrect: `ErrClientClosed` is only injected when the client's internal context is cancelled (i.e., when the client is fully closed), not during reconnections.

**Fix applied:** The entire `default` branch was removed. `IsClientClosed()` now always causes the goroutine to return, relying on the subscriber's supervisor (Watermill router) to re-subscribe if needed.

**Test:** `TestSubscriber_Network_IsClientClosedSpin_Fix` (Toxiproxy required).

---

### Bug 8 — MEDIUM: Nacked message copies have non-cancellable contexts

**File:** `subscriber.go`

**Root cause:** When a message was Nacked and requeued, the copy's context was built on `context.WithoutCancel(ctx)` with no `context.WithCancel` wrapper. Handlers using `msg.Context().Done()` as a shutdown signal would never receive cancellation on redelivered copies. The original cancel function (passed into `handleMessage`) only cancelled the first message's context.

**Fix applied:** Context creation was moved entirely into `handleMessage` via a new `buildMsgCtx` helper. Each Nack iteration: (1) cancels the previous message context, (2) builds a fresh cancellable context for the copy. The `cancel context.CancelFunc` parameter was removed from `handleMessage`'s signature. The `defer cancelMsg()` at the function level cancels whichever context is current when `handleMessage` returns.

**Tests:** `TestSubscriber_NackedMessageContextIsCancellableOnClose`, `TestSubscriber_NackedMessageContextCancelledOnStop`.

---

## Integration Test Battery

All tests are in `tests/integration/` with build tag `//go:build integration`.

### Lifecycle (`lifecycle_test.go`)

| Test | What it verifies | Requires Toxiproxy | TDD bug |
|---|---|---|---|
| `TestSubscriber_StopAllowsInflightToAck` | In-flight msg can be Acked after Stop; new Subscribe rejected | No | — |
| `TestSubscriber_CloseSubscribeRace` | No goroutine outlives Close(); run with -race | No | Bug 1 |
| `TestSubscriber_StopThenClose_NoDeadlock` | Stop then Close returns within 10 s | No | Bug 7 |
| `TestSubscriber_MultipleCloseIdempotent` | 5 concurrent Close() all return nil | No | — |
| `TestSubscriber_SubscribeAfterClose_Rejected` | Subscribe after Close → "subscriber closed" | No | — |
| `TestSubscriber_SubscribeInitialize_CreatesAndIdempotent` | Topic created, idempotent, messages delivered | No | — |

### At-Least-Once (`at_least_once_test.go`)

| Test | What it verifies | Requires Toxiproxy | TDD bug |
|---|---|---|---|
| `TestSubscriber_AtLeastOnce_AllMessagesDelivered` | 50 messages, all UUIDs received | No | — |
| `TestSubscriber_AtLeastOnce_NackCausesRedelivery` | Nack → same UUID redelivered | No | — |
| `TestSubscriber_AtLeastOnce_NackWithSleep` | NackResendSleep observed (≥ configured value) | No | — |
| `TestSubscriber_AtLeastOnce_MultiplePartitions` | 30 msgs across 3 partitions all received | No | — |
| `TestSubscriber_AtLeastOnce_OffsetCommittedAfterAck` | Re-subscribe skips already-acked messages | No | — |
| `TestSubscriber_AtLeastOnce_NoDuplicatesOnCleanStop` | No redelivery after Stop+Close+re-subscribe | No | — |
| `TestSubscriber_DisableAutoCommit_ManualCommit` | Manual commit survives re-subscribe | No | — |

### Poison Pill (`poison_pill_test.go`)

| Test | What it verifies | Requires Toxiproxy | TDD bug |
|---|---|---|---|
| `TestSubscriber_PoisonPill_DoesNotBlockSubsequentMessages` | Valid msgs after poison pill are delivered | No | Bug 2 |
| `TestSubscriber_PoisonPill_PartitionDoesNotStallAfterRebalance` | Poison pill skipped after rebalance | No | Bug 2 |
| `TestSubscriber_PoisonPill_ErrorIsLogged` | Error entry contains topic/partition/offset | No | Bug 2 |

### Network Faults (`network_fault_test.go`)

All network fault tests skip if Toxiproxy is not available (`toxiproxyAvailable()` check).

| Test | What it verifies | TDD bug |
|---|---|---|
| `TestSubscriber_Network_BrokerRestartRecovery` | Franz-go reconnects after TCP reset; all msgs delivered | — |
| `TestSubscriber_Network_HighLatency_NoTimeout` | 500 ms latency does not cause subscriber exit | — |
| `TestSubscriber_Network_CommitTimeout_UnderDisableAutoCommit` | Commit failure is propagated, not swallowed | Bug 4 |
| `TestSubscriber_Network_SlowConsumer_NoSessionExpiry` | Heartbeats are independent of handler latency | — |
| `TestSubscriber_Network_ConnectionDropDuringFetch_Recovers` | Mid-batch TCP drop is transparent to consumer | — |
| `TestSubscriber_Network_IsClientClosedSpin_Fix` | Close() returns within 10 s when client self-closes | Bug 7 |

### Rebalance (`rebalance_test.go`)

| Test | What it verifies | Requires Toxiproxy | TDD bug |
|---|---|---|---|
| `TestSubscriber_Rebalance_AllMessagesDeliveredWithTwoConsumers` | At-least-once across 2-consumer rebalance | No | — |
| `TestSubscriber_Rebalance_NoDuplicatesAfterStableGroup` | No redelivery after stable group closes | No | — |
| `TestSubscriber_RollingDeployment_InconsistentGroupProtocol` | Rapid start/stop churn, consumer self-heals | No | — |
| `TestSubscriber_OffsetOutOfRange_Recovers` | OFFSET_OUT_OF_RANGE recovered via reset policy | No | — |
| `TestSubscriber_SliceGrowth_OnContextCancelledResubscribe` | subClients does not grow on reconnect cycles | No | Bug 3 |

---

## TDD Methodology

Tests for the 8 bugs above were written to **fail before their fix is applied** and **pass after**. The workflow for each:

1. **Red** — Run the test against the unfixed subscriber. The test fails (race detected, message never delivered, Close hangs, etc.).
2. **Fix** — Apply the targeted change to `subscriber.go`.
3. **Green** — Re-run the test. It passes.
4. **No regression** — All other tests continue to pass.

Tests with `// TDD:` comments in their source identify which bug they target.

---

## Running Against a Real Cluster

To run integration tests against a real Kafka / Redpanda cluster instead of the local Docker stack:

```bash
KAFKA_BROKERS=broker1:9092,broker2:9092 \
  go test -race -tags integration ./tests/integration/... \
  -run 'TestSubscriber_AtLeastOnce|TestSubscriber_Rebalance'
```

Network-fault tests that require Toxiproxy will be automatically skipped if `toxiproxyAvailable()` returns false.

---

## Known Interac-Controller Production Error

**Error:** `kafka server: The provider group protocol type is incompatible with the other members` (`INCONSISTENT_GROUP_PROTOCOL`, Kafka error 23)

**Observed:** Repeatedly in staging (clusters `pcfi-stg` and `pcfi-stg-secondary`) during rolling deployments on the `rails-interac-controller-3` consumer group, topic `rails.interac.commands`.

**Pattern:** Error fires → consumer reconnects → resumes (self-heals within ~30 s). No restart required.

**How this subscriber handles it:** Franz-go manages group-level errors internally. `INCONSISTENT_GROUP_PROTOCOL` causes franz-go to reset group state and rejoin with backoff. This does not surface through `fetches.Errors()`. The subscriber's poll loop continues transparently. The `TestSubscriber_RollingDeployment_InconsistentGroupProtocol` test validates the self-heal property.
