<!--
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
-->

# Kafka / franz-go interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework composition and capability maintainers.
**Status:** implemented private technical integration; bounded service profile,
not the complete Activity data/reference or recovery protocol.
**Package:** `github.com/frost-leo/fathomry/internal/broker/franz/v1`.

## Responsibilities

One concrete `Client` provides producer and consumer operations with one
provider-owned configuration contract. There is no grouping-level broker
interface, native-client escape, application configuration loader or public
business API. Network I/O belongs in Activities/process-local code, never in
Temporal Workflow logic.

This supplies foundations for frozen source/build selection (R02/R03), accurate
and partial output/effects (R08–R10), explicit checkpoints (R11), version refusal
(R12), bounded work and independent evidence (R14–R16), metadata/error identity
and authorized configuration (R19–R21). It does **not** decide Item/attempt
membership, successful-empty business meaning, publication conditions, business
source progress, Run disposition or safe recovery.

The framework still owns the protocol: write data, confirm the declared output
scope, publish its reference, pass the reference record's own position, then
read reference and data. The integration test exercises this composition using
**test-owned DTOs**, not a published reference format.

## Capability and support matrix

| Category | Implemented boundary | Explicit exclusions |
| --- | --- | --- |
| Configuration/connection/readiness | Strict prepared settings, explicit full endpoint set and cluster ID, topic metadata checks on control and actual producers | DNS/environment discovery, dynamic membership/endpoints, implicit topic creation, readiness as business acceptance |
| Record content/coordinates | Bytes, nil versus empty key/value, ordered duplicate headers, millisecond timestamps, partition/offset and topic ID on reads | Business identity inferred from coordinates; producer ACK as atomic topic-incarnation proof |
| Production/batching/order | Bounded asynchronous batches; per-input results; all-ISR ACKs and native idempotency; manual partitioning; none/gzip | acks=0/1, native callback injection, global ordering, process-restart/application retry deduplication |
| Exact/historical reads | `ReadExact`, bounded exact sets, ID-addressed Fetch v13, explicit missing/unavailable/expired outcomes | Automatic offset reset, timestamp search as exact lookup, reading arbitrary unsupported codecs/legacy message formats |
| Range/direct consumption | Paginated physical intervals and lifetime-owned `Consumer` cursor; no whole-output materialization | Inferring logical batch membership from an interval; background prefetch or hidden polling retries |
| Checkpoints/groups | Explicit `CommitOffsets`/`FetchOffsets`, or `Consumer.Commit`, in an exclusively configured non-member group; v10 topic IDs | Group subscriptions, heartbeat/membership/rebalance rights, autocommit, static membership, KIP-848 consumer sessions/share groups |
| Transactions | Separate serialized transactional producer; atomic bounded Kafka-only batches; committed-read isolation | Arbitrary transaction callbacks, consume-transform-produce/group EOS, cross-service or whole-Run transactions |
| Limits/backpressure/closure | Shared admission and nested leases, bounded copies/decode/evidence, asynchronous completion, permanent producer fencing after cancellation grace | Distributed quotas, exact SDK attempt counts, hard RSS/latency limits, automatic producer replacement |
| Errors/evidence | Private fault identities, original causes, partial results, separate primary/cleanup facts; required Inbox independent of diagnostics | Logs as a completion ledger; durable evidence persistence/reconciliation |
| Logs/metrics/traces | Optional payload-free lossy `invocation.Observer`; separate explicit [W3C bridge](otelbridge/interface.md) | Native kotel/kzap/global provider hooks, automatic telemetry exports, broker-directed client metrics |
| Administration/Schema Registry | None in production API; tests create/delete only isolated owned resources | Topic/ACL/retention administration, group administration, Schema Registry/serialization policy, Connect/Streams |

Group checkpoints are a technical facility, **not consumer-group membership**.
Generation `-1` declares standalone offset storage. The framework must exclusively
own the group and authorize each prefix; concurrent commits have no CAS,
monotonicity or cross-partition atomicity guarantee. Committing next offset 3
does not prove processing of records 0–2. Group absence/retention is distinct from
business completion or absence of an execution.

## Construction and call sequence

1. `Select(OptionsV1, layers...)` calls `resource.Prepare` once. Attach
   `resource.WithLimits` before `resource.Assemble`; `LimitsV1` recommends
   limits for defaulted options, **not differently overlaid settings**.
2. Assembly owns two native clients (control/read and ordinary production), plus
   one optional transactional client. Its check observes the selected cluster,
   complete endpoint set, topics/IDs and partitions; it creates no remote resource.
3. `Bind` requires the exact selection, valid shared limits and an independently
   owned `invocation.Inbox[Result]`. Borrowing aliases retain original source
   identity, revision and allowance. Consumers cannot close the source.
4. `Produce`/`ProduceTransaction` copy inputs before returning asynchronous
   receipts. Other finite calls are synchronous but return the same receipt/
   independent-evidence contract. Setup/refusal errors return no receipt; after
   admission, inspect both waiting errors and `Result.Err()`.
5. Consume evidence independently with `Inbox.Next`. Retain unresolved receipts;
   release a delivery only after required handling and actual local-use completion.
   Inbox dequeue alone does not free capacity. No exporter runs on producer promises.
6. Close cursors, finish evidence handling and call `Assembly.Close`. Retain
   incomplete assembly cleanup and use its explicit continuation; native close
   does not settle external effects.

`Consume` reserves one stream root with an explicitly cancellable/deadlined
lifetime. `Next` and `Commit` use nested calls, not a second source reservation.
They reject concurrent use. A completed final page remains open for explicit
processing/commit; `Close` never commits. Closing or lifetime cancellation stops
new work and waits for an active child before recording final cursor progress.
Lifetime cancellation propagates to children so that a completed metadata request
does not authorize another fetch/commit. Already-issued requests still finish
under native network deadlines. Root and incremental evidence require separate
inbox capacity; drain pages incrementally, not only at cursor close.

See the [executable integration](../../../../../../internal/broker/franz/v1/integration_test.go)
and [consumer tests](../../../../../../internal/broker/franz/v1/consumer_test.go).
These are private composition examples, not bootstrap work assigned to business authors.

## Results and effects

- `WriteAcknowledged` retains the selected SDK's successful delivery evidence.
  A usable coordinate additionally requires `PositionKnown`, `IdentityChecked`
  and no relevant error. Native duplicate-sequence responses can succeed with
  offset `-1`; this yields an error without erasing ACK evidence.
- `IdentityChecked` means metadata matched before/after delivery on the **actual
  producer**. Kafka's producer callback does not observe topic ID atomically.
  Concurrent topic recreation/endpoint remapping during production is unsupported.
  An exact read separately verifies the topic ID on the fetch response.
- Non-nil delivery errors conservatively retain unknown effects. Local admission
  refusal and records never submitted are distinct. Partial success is indexed
  by input order; partition offsets do not provide ordering across partitions.
- A transaction result is independent of its record ACKs. An abort can leave ACKed
  records invisible to committed readers. Unconfirmed end remains unknown and
  fences future transactions. Successful local close does not turn it into abort.
- `ReadFound` can contain a tombstone or present zero-length bytes. `ReadMissing`
  requires a validated scan to pass the requested offset without visible data.
  `ReadExpired` means below observed log start, not a diagnosis of retention
  versus explicit deletion. `ReadUnavailable` means at/above the stable offset;
  pending transactions, future offsets and missing confirmation are not empty output.
- `Page.Complete` certifies scanning the requested **physical** interval only.
  Its cursor includes control records and gaps. Output count/byte limits return
  resumable prefixes; an oversized first record/batch refuses explicitly.
  Failed pages can retain observations without cursor advancement. Retrying them
  can repeat those observations. The first batch must fit supported decode bounds.
- Watermark observation is explicit; a zero Page/local empty interval has none.
  Original root targets, Kafka data, references, checkpoints and diagnostic history
  have different lifetimes. The historical seven-day direction is not a proven
  per-record TTL or seven days after Run completion.

## Cancellation and failure ownership

The provider uses shared budgets/leases rather than equating cancellation with
no effect. Native idempotent delivery may ignore ordinary retry/deadline limits
while uncertain; native transaction initialization can also wait indefinitely.

After delivery context expiry, a source-owned watcher grants `CleanupTimeout`
for late completion, then permanently fences and once-closes that producer.
All affected calls still drain their own callbacks; assembly separately joins
native shutdown. A queued transaction has no watcher until it acquires its gate,
so canceling it cannot close another transaction. There is no silent recovery/
replacement or switch to weaker idempotency. Control/reads use a separate client.

There is no standalone global Flush API or manual-flushing mode. Wait for every
relevant batch receipt; an unrelated flush or source Close cannot substitute for
its per-record evidence. Source closure waits for admitted ownership to end.

Native `RecordDeliveryTimeout` is disabled because this SDK measures age from
the record timestamp. Invocation time and producer fencing bound the selected
lifecycle without rewriting historical timestamps. Native sockets/retry timers
remain separate; no hard wall-clock termination of Go/kernel work is promised.

Errors are centralized in [errors.go](../../../../../../internal/broker/franz/v1/errors.go).
They preserve deliberate `errors.Is/As`, original native and cancellation causes,
while formatting hides native text, payloads and credentials. Inspect primary and
cleanup separately. Deliberately unwrapping native causes is not sanitized.
Runtime configuration, values and handles reject accidental JSON serialization;
a future durable protocol needs its own versioned representation.

## Details and evidence

- [Options, bounds and version axes](options.md).
- [Verification profiles and runnable commands](verification.md).
- [Accepted integration standard](../../../../../architecture/internal-sdk-integration.md).
- [Issue #36](https://github.com/frost-leo/fathomry/issues/36).
