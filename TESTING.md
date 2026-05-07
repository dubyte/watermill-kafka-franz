# Testing Guide — watermill-kafka-franz

## Overview

This document describes the testing methodology and integration test battery for the `watermill-kafka-franz` subscriber and publisher.

Tests are split into three distinct layers, each with a single gating mechanism:

| Layer | Purpose | Location | Files | Broker | Run with |
|---|---|---|---|---|---|
| **Unit** | Pure Go logic — config, marshaling, state machines | `pkg/kafka/` | `*_test.go` | No | `make test-short` |
| **Watermill compliance** | Verifies the library correctly implements the `message.Publisher` / `message.Subscriber` interface contract using watermill's own test suite | `pkg/kafka/` | `*_integration_test.go` | Yes | `make test-integration` |
| **Kafka behaviour** | At-least-once semantics, network faults, rebalancing, poison pills — use-case driven | `tests/integration/` | `*_test.go` | Yes + Toxiproxy | `make test-integration` |

**Gating rule:** any file that requires a broker carries `//go:build integration`. Running `go test ./...` without that tag is always safe — no broker needed, no hangs.

```
pkg/kafka/
  config_test.go               ← unit
  marshaler_test.go            ← unit
  publisher_test.go            ← unit
  subscriber_test.go           ← unit
  publisher_integration_test.go  ← compliance (//go:build integration)
  subscriber_integration_test.go ← compliance (//go:build integration)
  pubsub_integration_test.go     ← compliance (//go:build integration)

tests/integration/
  helpers_test.go              ← behaviour helpers (//go:build integration)
  lifecycle_test.go            ← behaviour (//go:build integration)
  at_least_once_test.go        ← behaviour (//go:build integration)
  poison_pill_test.go          ← behaviour (//go:build integration)
  network_fault_test.go        ← behaviour (//go:build integration)
  rebalance_test.go            ← behaviour (//go:build integration)
```

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

The integration tests hard-code `127.0.0.1:9092` as the broker address (see `tests/integration/testmain_test.go`). To run against a different cluster, edit that address or adjust your local DNS/port-forwarding so that `127.0.0.1:9092` reaches your broker.

Network-fault tests that require Toxiproxy will be automatically skipped if `toxiproxyAvailable()` returns false.
