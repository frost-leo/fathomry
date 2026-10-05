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

# Kafka Adapter interface

[Documentation](../../../../../README.md) / Public Adapter reference

**Audience:** process-local application and Framework-composition authors.
**Status:** implemented bounded native profile; controlled peers and isolated
plaintext/no-auth service checks do not qualify deployed TLS/SASL or failover.
**Package:** `github.com/frost-leo/fathomry/adapters/broker/kafka/v1`.

## Ownership and call sequence

1. Supply explicit `Settings`; `Validate` does not connect. `Recommend` derives
   one-source policy; `Compose` counts each simultaneously owned source instance
   and overlapping generation separately. Do not use generic Runtime defaults.
2. Construct caller-owned `adapters.Runtime` and `adapters.Inbox[Result]` from
   that policy, then pass `Dependencies` to `Open`.
3. Open performs real metadata readiness. Retain **every non-nil Owner**, even
   on error. Client is non-owning; only Owner closes source clients.
4. Use Client directly, or install Owner.Handle in a public
   `resource.Instance[Handle]` whose Release calls Owner.Release.
   `Using` borrows one Fixed/Follow generation per complete operation/session
   family. A replacement larger than its declared Budget refuses before dispatch.
5. Drain required evidence with `NextReleased` or the existing Framework receiver.
   Admission-order `Next` can return a still-live Open/session root; claim and
   retain that root separately if using this lower-level route.
6. Close Consumers/Groups, then Owner. Canceled waits retain cleanup continuation.
   Finalization requires no new admission slot. Framework uses the same public
   resource/operation contracts, not another Kafka client.

`WithID` freezes opaque correlation. Result attribution preserves the acquired
public generation and native preparation revision. These are not SDK, deployment
or durable schema versions. Shared public data lives in
[broker/v1](../../v1/interface.md), with no Internal import.

## Supported capability

| Family | Meaning and limits |
| --- | --- |
| Metadata | Frozen cluster/topic-ID checks; no topic creation or readiness guarantee for future calls |
| Produce | Bounded copied input, asynchronous read-only Receipt, actual per-record ACK coordinates and partial/unknown effects |
| ProduceTransaction | Separate serialized producer; bounded Kafka-only batch; record ACKs do not imply transaction commit |
| ReadExact / ReadPositions | Original addresses/order and distinct found/missing/expired/unavailable evidence; no next-record substitution |
| ReadRange / Consume | Physical pages and retained direct cursor; independent of groups; no automatic reset/commit |
| CommitOffsets / FetchOffsets | Explicit standalone OffsetGroup, generation=-1; no membership, processed-prefix or cross-partition atomicity guarantee |
| ConsumeGroup | Classic cooperative-sticky dynamic membership, bounded assignments/pages and explicit member-bound commits |
| Codecs | none/gzip/snappy/lz4/zstd; common preflight before native record/header allocation |
| Routing | manual by default; keyed uses native StickyKeyPartitioner and requires Partition=-1 |
| Authentication | none, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512; static credentials over verified TLS only |

The selected LZ4 profile is one modern independent-block frame; legacy and
concatenated streams are not admitted. Fetch response metadata is bounded before
native array allocation, including aborted-transaction entries; see the native
option contract below for its explicit tag and cardinality limits.

The [native options contract](../../../../internal/broker/franz/v1/options.md)
lists defaults, units and bounds. Endpoints are explicit literal IPs; topic IDs
are frozen.
SCRAM admission caps challenges at 8 KiB and iterations at 4096–65536 before native
proof processing; larger server profiles are explicitly unsupported.
Settings are plain-loadable configuration data: explicit configuration
JSON contains sensitive values, while direct formatting/logging is redacted.
Runtime records, headers, errors and owners refuse implicit JSON serialization.

Nil keys/values remain different from present empty bytes. Nil value is a
tombstone, not successful-empty business output. Duplicate headers retain order.
Record getters return caller-owned copies; private shared content is immutable.
Do not mutate input concurrently with submission.

## Group processing and lifecycle

ConsumerGroup is independent of OffsetGroup; identical configured names reject.
InitialOffset and ResetOffset must explicitly select error, earliest or latest.
Absence and out-of-range positions are observed before applying policy; stable-end
unavailability is not an automatic reset.
The selected initial/reset point is retained even at EOF; latest does not
continually skip newly arriving records.

Opening starts membership, not readiness. Snapshot returns current bounded state;
Wait observes a later revision. Revision gaps expose coalesced transitions.
Callbacks never invoke application code, sinks, translation or public admission.
Assignment growth outside the source's frozen partition topology refuses.

Next fairly selects a partition without an uncommitted page. GroupBatch contains
immutable data and a private token. CommitBatch explicitly declares **that entire
page** processed. It cannot accept arbitrary offsets or bypass an outstanding
page. Reopening resumes according to the stored checkpoint and explicit initial/
reset policy. Latest with no valid checkpoint can skip earlier data; reopening is
not an unconditional replay guarantee. There is no automatic business retry.

Every generation invalidates old tokens, including cooperative retained partitions.
Retained partitions keep their selected local cursor and reissue uncommitted work
under fresh tokens; only new/reacquired partitions reapply checkpoint/start policy.
A code-owned PreCommitFnContext rechecks actual
member/generation/topic IDs after stripping caller values. A pre-dispatch refusal
is different from an issued request with an unknown outcome. Unknown commits
terminate the session rather than permitting later writes to race them.

Native membership uses no Poll and no data cursor; existing ID-addressed fetches
own decoding. Loss/fatal errors terminate the session, preventing accumulated
native fake-fetch errors. Rejoin requires explicitly opening a new session.
Closing preserves a separate native context long enough to send LeaveGroup;
cancellation does not prove successful remote leave. Dynamic single-member leave
uses exactly LeaveGroup v2 so the selected SDK cannot drop per-member errors.
Root evidence retains
separate cleanup errors. Closing never commits.

## Evidence, limits and producer identity

A canceled production wait neither resends records nor releases the source.
One bounded bridge worker joins actual promises before projecting evidence.
Native callbacks do not run public sinks. Each retained native root is claimed
before child calls enter its Inbox; children transfer incremental facts separately.
Receiver failure/Retry redelivers facts, not Kafka operations.

Compose includes persistent source socket/codec buffers, native root work,
assignments, private/public evidence and overlapping generations. Caller-retained
copies and arbitrary native cause graphs are not a hard RSS bound.

Ordinary and transactional producer fences remain permanent for that source.
There is no automatic replacement, identity randomization or fence reset.
Transactional sources require a caller-owned TransactionIDs registry shared by
all local sources targeting a cluster. Same-ID overlapping Open refuses before
native construction; the entry releases only after native source shutdown.
Cross-process exclusivity remains the caller's obligation, not a generation lease.

IdentityChecked means metadata matched before/after delivery, not an atomic
topic-ID observation in the producer ACK. Concurrent topic recreation during
production remains unsupported. Exact reads separately verify fetch topic IDs.

## Errors and exclusions

FacilityKafka is 0x180, component fathomry/broker_kafka. English and zh-CN resources
join the explicit offline CLI atlas. Primary, cleanup and individual record/
partition errors stay separate; errors.Is/As and InspectError retain native
causes. Deliberate native-error inspection is sensitive, not safe logging.

No administration, KIP-848, static/share groups, OAuth/GSSAPI, Schema Registry,
arbitrary native hooks, cross-service transaction or generated-project profile.
No Workflow commands or durable offset/assignment format; replay is not applicable.

## Executable evidence

- [Group lifetime tests](../../../../../../adapters/broker/kafka/v1/group_test.go).
- [Framework and independent module](../../../../../../adapters/broker/kafka/v1/integration_test.go).
- [Authorized service fixture](../../../../../../adapters/broker/kafka/v1/integration_service_test.go).
- [Native qualification limits](../../../../internal/broker/franz/v1/verification.md).
- [Issue #105](https://github.com/frost-leo/fathomry/issues/105).

Local tests require no existing service:

```sh
go test -mod=readonly -race -count=1 ./adapters/broker/... ./internal/broker/franz/v1/...
```

Only after separate authorization:

```sh
FATHOMRY_KAFKA_TEST_CONFIG="$PRIVATE_KAFKA_CONFIG" FATHOMRY_KAFKA_TEST_WRITES=1 \
  go test -mod=readonly -tags=kafka_service -race -count=1 -timeout=4m \
  -run '^TestKafkaAuthorizedServiceCore$' ./adapters/broker/kafka/v1
```

The fixture creates unique gh105-<random> topics/groups, writes synthetic values
and verifies cleanup. It covers five-codec independent effect read-back and two
classic members, redistribution, explicit commits, stale refusal, leave/rejoin
and no close-time commit. It changes no service configuration or business data.
Its PLAINTEXT/no-auth RF1 profile does not qualify deployed TLS/SASL, multi-replica
failover or production capacity.
