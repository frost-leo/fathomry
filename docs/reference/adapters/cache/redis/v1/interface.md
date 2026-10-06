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

# Redis public adapter

[Documentation](../../../../../README.md) / Public package reference

**Audience:** direct consumers and Framework composition owners.
**Status:** implemented unreleased candidate for [#108](https://github.com/frost-leo/fathomry/issues/108);
service qualification is limited to the profiles below.
**Package:** `github.com/frost-leo/fathomry/adapters/cache/redis/v1`.

## Responsibilities and call sequence

This is a bounded Redis capability API, not a Get/Set wrapper or raw SDK escape.
It uses official go-redis **v9.22.0** without a replacement, and delegates native
routing/framing/ownership to [Internal](../../../../internal/cache/redis/v9/interface.md).
It does not implement durable output/evidence storage, distributed quota,
business retries/idempotency, locks, Item/Run completion or exactly-once delivery.

1. Call `DisableNativeLogging` at **single-threaded process startup**, before any
   go-redis client/goroutine, including outside Fathomry. Native logging is global
   and can contain sensitive text. No constructor, preparation, catalog lookup or
   import mutates it. Do not replace the native logger while clients run.
2. Strict-load ordinary `Settings` using public `configsource.Schema[Settings]`
   and `Prepare`; validate with Redis `Validate`. Prepare Redis settings once with
   `Prepare` or composition-only `PrepareWithPassword`.
3. Obtain `Prepared.Policy`, or `Compose` every source instance that can coexist.
   Repeated preparations represent distinct native owners, including old/new
   Fixed/Follow generations. Create a caller-owned `adapters.Runtime` and
   `adapters.Inbox[Result]` from that policy; the observer is optional.
4. `Prepared.Open` constructs a source under the supplied lifetime. Retain every
   non-nil `Owner`, including with setup errors. Construction is not readiness:
   an explicitly granted `PING` is an ordinary evidence-producing operation.
5. Hand consumers `Owner.Client().Cache()` / `.Messaging()`, not the Owner or
   password authority. Views and copies neither create quota nor close resources.
   `NewCommand` / `View.Command` freeze binary-safe arguments and semantic owner.
6. Independently receive released evidence and acknowledge only after required
   handling. A caught error or canceled wait never acknowledges evidence.
7. End callbacks and accepted work; call `Owner.Close` until actual shutdown is
   confirmed. Close needs no fresh admission/evidence slot. A timeout retains a
   reachable owner/cleanup worker; non-cooperative callbacks remain live owners.

The executable [direct consumer](../../../../../../adapters/cache/redis/v1/testdata/direct/main.go)
imports no Framework or Internal. The [Framework consumer](../../../../../../adapters/cache/redis/v1/testdata/framework/main.go)
uses existing resource Fixed/Follow bindings, operation runtime and released-evidence
receiver. Framework itself gains no Internal dependency or Redis service engine.

## Capabilities and qualification matrix

Every command uses exact `Commands`/`AdminCommands` grants and the same bounded
path. Commands unavailable in the selected Redis version/module return native
server errors; a grant is not server support or an ACL substitute.

| Capability | Public path and semantics | Executable checks |
| --- | --- | --- |
| Standalone / Sentinel / Cluster / Ring / explicit Universal submodes | Typed Settings; frozen complete endpoint authority; Ring is independent sharding, not replication | `TestPublicRedisTopologies`, Internal topology tests |
| Trust and credentials | Explicit plaintext or CA/name/mTLS; separate Sentinel credentials; composition-owned local password refresh affects new connections only | `TestPublicTLSMutualTLSAndMaintenanceTrust`, topology password test, option parity |
| Keys, TTL and data families | `View.Execute` / binary-safe Command; native units for strings/counters, hashes, lists, sets/sorted sets, bits, geo and HLL | Public service data-family checks |
| Ordinary Pipeline | Mixed declared capabilities, positional replies/errors, partial effects; independent Cluster commands can cross slots | Public mixed/partial tests and Cluster service test |
| Dedicated / WATCH / MULTI-EXEC | Callback-scoped pinned Session; no callback retry or transaction splitting; Cluster same-slot restriction | Public lifetime/conflict/EXEC tests; topology tests |
| Scripts and functions | Granted EVAL/EVALSHA/FCALL and explicit FUNCTION administration; NOSCRIPT retained; no uncertain-write retry or global deployment | Public service partial Lua, topology functions |
| SCAN family | Native cursor pages, duplicates/expiry possible; COUNT is a hint, not a snapshot or output bound | Service SCAN/HSCAN/SSCAN/ZSCAN; Cluster/Ring raw SCAN refused, Dedicated scan is node-local |
| Blocking lists | Finite command using source/caller deadlines; timeout does not prove no dequeue | Public service blocking test |
| Streams/groups/pending/claim/ack | Messaging view, stream IDs and native acknowledgements; Redis 8.8 XNACK/XCFGSET included; no business completion assertion | Public service Streams checks |
| Channel/pattern/sharded Pub/Sub | Messaging-only callback; pull Receive exposes confirmations and messages separately; reconnect may lose messages or repeat confirmations | Public event/lifetime tests, three service modes, Internal reconnect checks |
| Admin/module commands | Explicit grants and `WithKeyPosition`; HIMPORT dedicated-only; no blanket module qualification | `TestControlledExtensionAndDedicatedOnlyCommand`, Internal routing tests |
| Experimental CSC | Explicit opt-in; standalone/Universal-standalone, RESP3, DB0, fixed credentials; bounded estimated entries/bytes/staleness | Actual service hit and invalidation before expiry; rejecting option controls |
| Experimental automatic/deferred pipeline | Explicit opt-in; standalone/Sentinel/Cluster and matching Universal; Ring refused; Submit returns immutable public receipt; enqueue is not revocable by per-command cancellation | Stopped-wait/post-enqueue cancellation tests, real multi-command batch-width observation and topology Automatic checks |
| Maintenance push | Disabled by default; explicit RESP3 standalone/Cluster/Ring profile; Sentinel refused; native bounded workers/queues/dials | TLS/option/Internal maintenance tests; cloud handoff remains unqualified |
| Pool/socket/cache/batch statistics and profile | `Client.Stats(ctx)` / `Profile(ctx)` are detached diagnostics, not required evidence or service/version observations | Profile/budget tests, real cache and batch controls |
| Startup logging | Explicit bootstrap only; no hidden constructor/catalog mutation | Isolated subprocess keep/disable controls |

Raw native clients, Cmdable/Cmder, mutable options/results, arbitrary dialers,
hooks, push processors, routing callbacks, global exporters and streaming credential
I/O are not exposed: they bypass endpoint, ownership or reservation invariants.
Raw AUTH/HELLO/SELECT/CLIENT/MONITOR/replication/session-control grants are refused.
Native cache strategies and unordered/multi-shard auto-batching are not passthrough
options: the selected engine has one shard/concurrent batch and a finite delay.
No product requirement requires uncontrolled callback I/O or unordered replay.
Server ACLs, not key prefixes or local grants, remain the security boundary.

The pinned native auto-batcher deliberately ignores each batched command's context
after enqueue, even during owner/runtime cancellation. Queued mutations may still
execute and return known successful replies after cancellation. `Submit` and
`Automatic` retain those effects and all ownership; they do not synthesize a
cancellation result. `Timeout` bounds native dial/read/write/pool phases, not total
enqueue-to-completion time. Native batch-permit waiting has a separate 30-second
backstop; approximate MaxBatchBytes is not a RESP wire-byte cap. Use `Execute`
for a per-command execution context. This preserves the
[pinned SDK contract](https://github.com/redis/go-redis/blob/c7f59a2a950eb5131cc27bfff716d6d3382e4490/autopipeline.go)
without inventing a second batching/revocation engine.

## Complete settings fidelity

Fields use explicit snake_case JSON/mapstructure names. Version 0 selects format
1; duration fields end in `_ns` and use integer nanoseconds. Strict preparation
rejects unknown/duplicate/mistyped keys, invalid versions and unsupported combinations.
Runtime handles/callbacks are not settings. Explicit configuration JSON is sensitive;
ordinary formatting/logging is redacted. Do not mutate input during preparation.

| Group | Fields | Defaults / meaning |
| --- | --- | --- |
| Identity | Name, Version | Required source name; version 0/1 |
| Topology | Mode, UniversalMode, Addrs, AllowedAddrs, Shards, MasterName | Explicit topology/submode; authority defaults to explicit seeds/shards, never discovered nodes |
| Routing | DB, Protocol, ReadOnly, MaxRedirects | DB0, RESP3, primary reads, no redirects/retries by default; replica reads can be stale |
| Data credentials | Username, Password | Frozen identity; empty password with a non-default named ACL account is refused |
| Sentinel credentials | SentinelUsername, SentinelPassword | Separate static authentication; non-Sentinel combinations refused |
| Trust | Plaintext, RootCAPEM, ServerName, CertificatePEM, PrivateKeyPEM | Explicit plaintext opt-in or explicit trust set; no ambient discovery |
| Admission/socket | MaxActive, QueuedCalls, MaxConnections | 4 active, zero queue, 16 sockets; topology/dedicated headroom checked |
| Payload | MaxCommands, MaxArgs, MaxRequestBytes, MaxReplyBytes, MaxReplyElements | 32 commands, 4096 arguments, 1 MiB request/reply, 32768 reply elements |
| Time/pool | Timeout, CloseTimeout, MaxIdleTime, MaxLifetime | 5s operation/cleanup; **nil** MaxIdleTime means 5min, **non-nil zero** disables idle expiry via native -1; zero lifetime disables expiry |
| Grants | Commands, AdminCommands, AllowSessions, AllowSubscriptions | No grants/callback capabilities by default; exact grants, not implicit command families |
| Experimental cache | ExperimentalCache, CacheEntries, CacheBytes, CacheMaxStaleness | Disabled; 1024 entries, 8 MiB estimated bytes, 1s staleness |
| Batching | ExperimentalAutoPipeline | Disabled; unsupported Ring combination rejected |
| Maintenance | MaintenanceMode | disabled; explicit auto/enabled profiles |

Other documented zero bounds select the Internal defaults; zero queue/redirects,
false flags and explicit zero idle are not lost. Nil/empty authority/grant
collections follow the Internal validation/default rules. Public option-mapping,
strict-loading and effective-profile tests guard all fields.

`Password.Replace` is local synchronized replacement, not a credential fetcher
or reauthentication operation. CSC rejects rotation. TLS/Sentinel credential
replacement requires an explicitly constructed replacement source.

## Budget, generation and custody invariants

Internal `PrepareV1` exports metadata from the **same resolved frozen configuration**
used to construct its Selection: native work, each receipt, resident source and
actual admission bounds. The public layer adds only its own two-receipt bridge
storage and public metadata envelope; it does not reproduce private formulas.

Pool/wire/topology/cache/maintenance/auto-batcher costs stay source-reserved at
idle until actual native shutdown. These estimates are not hard process RSS
limits. Shared views/aliases use the same native pool/socket limits. Different
generations may legitimately own separate pools, but their concurrent resident,
work and evidence costs must all fit explicit aggregate composition capacity.

`Using` retains the actually borrowed generation until native work **and** receipt
transfer end. An adopted source larger than its declared public Budget is refused
before native dispatch. No public alias can enlarge one source's native admission.

Each callback root is claimed before any child can consume its private Inbox.
A child reserves public evidence before native work. The bridge verifies correlation,
waits for actual native release, resolves the pre-reserved public outcome, then
releases Internal custody. A callback that catches an error still leaves that
child's independently retained evidence. Source, callback and child results are
distinct. Cleanup never needs another slot at saturation.

Callback termination by `runtime.Goexit` (including a test callback's Fatal)
still unwinds native cleanup and transfers the already-claimed lifecycle record
before public ownership releases. Its callback failure and independent cleanup
facts are retained; the Adapter does not swallow termination or turn it into
successful command evidence. [Callback termination tests](../../../../../../adapters/cache/redis/v1/callback_test.go)
cover Dedicated, Watch and subscription paths.

An impossible bridge invariant/acceptance failure is quarantined on the reachable
Owner with its records and holds intact; `PendingTransfers` reports it and Close
cannot claim success. This fail-closed condition requires correcting the invariant,
not dropping evidence. It is not ordinary evidence pressure or an automatic retry
of external mutations.

## Values, effects and error ownership

`Reply.HasValue` distinguishes validated retained data (including decoded Redis
null) from absent, invalid or discarded data. Unavailable is the public ValueKind
zero; null, empty text/aggregate, numeric zero, binary strings, exact big integers,
maps, arrays and nested errors remain distinct. All outer getters are detached;
nested values are immutable. Result and runtime values refuse serialization.
Pub/Sub's payload slot preserves native text or array shape, including an empty
array distinct from empty text.

Reply state is independent: NotEntered, Unknown, Replied, CacheOrReply and
TransactionAborted. Replied includes server errors and does not imply no partial
effects; CacheOrReply is not fresh-server evidence. Attempt counts are explicitly
inexact when native auxiliary requests/redirects are not observed.

`TransactionResult.Execution` contains command facts;
`.Lifecycle` records local session cleanup. A successful lifecycle is not EXEC
success. `Result.AggregateError` separately retains dispatch/EXEC causes not
already represented by positional errors, including EXECABORT after queue errors.
All 256 supported command positions keep their error causes. Pub/Sub confirmation
is not a message; a publish/Streams XACK reply is
not business acknowledgement, durable delivery or Item/Run completion.

Command semantic selection is explicit. Streams/PubSub commands require Messaging.
Other raw operations, especially scripts/list queues/modules, can serve either
intent; the caller declares that intent rather than having Lua parsed or a brand
guessed. Mixed batches retain each command's declared owner without splitting
transactions. Their root view owns dispatch/lifecycle errors; intrinsic source
setup, credentials, shared source state and shutdown belong to cache.

Cache errors use `fathomry/cache_redis` facility **0x100**; messaging uses
`fathomry/messaging_redis` **0x181**. Shared public operation/resource/settings
occurrences retain their existing identity. Errors preserve `errors.Is/As`,
including redis.Nil, TxFailedErr, server errors, context causes and cleanup joins.
`IsNull` / `IsWatchConflict` do not require importing the SDK.
`InspectError` deliberately exposes sensitive native server text; never log it
blindly. Definitions, both resource sets and CLI catalogs are offline, bilingual
metadata; locale changes never change codes or effect facts.
Transparent callback wrappers retain their original graph and public error core,
but their potentially sensitive contextual text is excluded from presentation.

## Verification and limits

[Tests and independent consumers](../../../../../../adapters/cache/redis/v1)
exercise the real selected SDK, not a fake adapter engine. Public service tests
are build-tagged `redis_service` and **fail**, rather than skip, without explicit
inputs/authorization. They use random owned prefixes and independent exact deletion/
absence checks. Topology fixtures only start explicitly selected test-owned
loopback processes and join their own process handles on cleanup.

Executed service profiles for this candidate: Redis **8.8.0** standalone plaintext
(with the owner-supplied static credentials), and Redis **8.2.1** test-owned local
Standalone/Sentinel/Cluster/Ring and explicit Universal selections. TLS/mTLS is
qualified against controlled local protocol peers, not deployed production TLS.
Module-server combinations, cloud maintenance handoff, production failover, durable
delivery and performance/capacity claims remain unqualified.

The issue's owner-local verification record binds exact command receipts and a
source manifest to this uncommitted worktree candidate, based on
`bb615ac64498d7704f0d8edab01241fada071625`. It is not a merged/released revision,
and local checks do not substitute for remote required CI.

## Error boundary organization

Maintenance follows the shared [public Adapter contract](../../../../../development/public-adapters.md):
configuration and policy are separate, native budgets are authoritative, and
this provider's readiness, context and result semantics remain distinct.

Stable declarations, runtime mapping, diagnostics and locale embedding follow
[the public adapter error boundary](../../../../../development/adapter-errors.md).
Transparent public-error wrappers retain their existing core and original causes
without exposing wrapper text; explicit native semantic frames remain provider-owned.
This changes neither public APIs, numeric identities nor locale resources.
