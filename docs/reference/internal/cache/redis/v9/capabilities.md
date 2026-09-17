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

# Redis capability and topology census

[Documentation](../../../../../README.md) / [Redis interface](interface.md)

**Audience:** composition and deployment maintainers.
**Status:** capability classification for official go-redis v9.22.0, commit
`c7f59a2a950eb5131cc27bfff716d6d3382e4490`; not blanket service certification.

## Stable execution paths

| Family | Supplied path and limits |
| --- | --- |
| Standalone | Explicit single endpoint, DB, verified TLS/mTLS or explicit plaintext; native reusable pool |
| Sentinel | Separate Sentinel credentials/seeds, authorized discovery, primary or ReplicaOnly routing; ordinary failover is not durable write confirmation |
| Cluster | Seed discovery, slot routing, optional bounded redirects and replica reads; DB must be 0; no arbitrary topology-loader callback |
| Ring | Named independent shards, native consistent hashing and heartbeat; node loss changes placement, not replication or recovery |
| Universal | Explicit standalone/Sentinel/Cluster selection; Ring is its own mode. Sentinel ReplicaOnly uses the native failover constructor because UniversalOptions does not expose that flag |
| Keys, strings, counters, TTL | Native argument units and expiry sentinels; binary values and redis.Nil preserved |
| Hash/list/set/sorted set | Granted native commands and RESP2/RESP3 values; newer hash expiry or list commands remain server-version-dependent |
| Bitmaps, geospatial, HyperLogLog | Native commands, not new business policies or exact-cardinality guarantees |
| Pipeline | All modes; positional errors/results retained; independent Cluster commands may use different slots |
| WATCH/MULTI/EXEC | Explicit pinned session, once-only callback, same server connection; no rollback of every runtime command error and no implicit cross-shard transaction |
| Lua/EVAL/EVALSHA | Granted commands, explicit key count/routing; NOSCRIPT is retained. Callers choose EVAL fallback rather than silently replaying an uncertain mutation |
| Functions | FCALL/FCALL_RO plus explicitly granted FUNCTION subcommands; Redis 7+ server capability, no implicit deployment or global FUNCTION FLUSH |
| SCAN/HSCAN/SSCAN/ZSCAN | Incremental native pages; duplicates, expiry and cursor invalidation remain native; topological SCAN is node-local |
| Blocking lists/Streams | Finite controlled call with source/caller socket deadline; timeout is not proof of no dequeue/side effect |
| Streams/groups/pending/claim/ack | Explicit native commands; persistence/retention and business processing remain caller/server responsibilities |
| Pub/Sub | Channel, pattern and sharded subscription sessions; pull-based receive with separate confirmations and message evidence; reconnect may lose messages |
| Dedicated connections | Private callback-owned native pool under the source's existing admission/socket fence; server-local state is discarded by closing the pool |
| Credentials | Fixed ACL/password; composition-owned password refresh on new connections, fixed account identity; separate Sentinel credentials |
| Observation | Source pool/socket/cache statistics and optional payload-free invocation.Observer; required evidence is independently reserved |

Ring WATCH keys and channel subscriptions must map to one current shard per session.
Ring patterns are explicitly scoped by RouteKey, not broadcast to every shard. Cluster sharded
Pub/Sub requires supporting Redis versions (Redis 7+) and same-slot channels.
Native direct Pub/Sub callbacks/Channel buffers are not exposed.

## Experimental and optional native machinery

| Capability | Classification and supported selection |
| --- | --- |
| Client-side cache (CSC) | Experimental, opt-in; standalone/Universal-standalone, RESP3, DB 0, fixed credentials only. Separate native cache per source, finite entries/estimated bytes and max staleness. No arbitrary shared cache or invalidation processor |
| Automatic/deferred pipeline | Experimental, opt-in; standalone, Sentinel and Cluster, including Universal selections. Ring is rejected by native limitation. One native batching shard, one concurrent batch, finite flush delay; outer admission bounds accepted queues/waiters despite native soft batch thresholds |
| Deferred result access | Provider Submit returns an immutable receipt, not a native mutable deferred command. One owned waiter per admitted call waits for actual completion; stopping caller waiting cannot discard evidence or release the native use |
| Maintenance push handling | Disabled by default; explicit auto/enabled modes for RESP3 standalone/Cluster/Ring use built-in native handlers, bounded workers/queues/retries and the same dial fence. Sentinel is rejected because its v9.22 constructor does not install the corresponding manager. Live cloud handoff is unqualified |
| Dedicated-session handoff | Not activated: connection-affine WATCH/session work must not be silently migrated. A connection failure is reported, not replayed on another connection |
| External authentication refresh | No external I/O callbacks or streaming credential listener are installed. Refresh acquisition belongs to an explicitly owned outer capability; the supplied local snapshot only affects reconnect authentication |
| Native hooks/push/instrumentation callbacks | Not exposed: mutable commands, connection handles, arbitrary callback I/O and process-global exporters would bypass these bounds/evidence/privacy contracts. AddHook is not a business extension route |
| Raw/admin access | Explicit grants through the same bounded command path. Protocol-changing and unbounded session routes remain refused. Server ACLs, not command grants or key prefixes, enforce real privilege isolation |

Automatic batching does not honor individual command cancellation after enqueue;
a result accessor can outlive the caller's wait. Blocking commands may be diverted
by the native batcher. Admission remains held until actual completion. This is not
a new asynchronous task scheduler.

CSC invalidation is asynchronous and can lag, particularly through TLS/opaque
connection wrappers. Its configured max-staleness backstop is not linearizability.
A cache hit is never presented as fresh server-effect evidence. CSC cannot be
combined with rotating credential providers in this SDK and is rejected up front,
rather than silently running without the requested cache.

## Server/module-dependent command families

JSON, probabilistic structures (Bloom/Cuckoo/Count-Min/TopK/t-digest), search,
time series, vector sets, array commands and HIMPORT are available only when the
server/module and ACL support the exact command. Select them explicitly with raw
grants. Built-in count/subcommand/stream forms get native first-key positions;
`WithKeyPosition` also supplies an explicit override for raw or extension commands
whose routing metadata is unavailable. It does not waive same-slot
constraints or identify every key of an arbitrary module operation.

HIMPORT is permitted only inside a dedicated session: fieldset/session state
must not escape onto a reusable pooled connection. The native typed HIMPORT
registry helper is not exposed. Redis 8.10-specific families are not qualified by
the Redis 8.8/8.2 service runs. Stable SDK publication does not change that fact.

No raw command family is automatically assumed to work in every topology.
Empty passwords for non-default ACL usernames are explicitly refused: this
SDK's HELLO builder omits AUTH when the password is empty. This prevents a silent
account switch; passwordless named-user qualification would need a separate
controlled authentication path, not an SDK fork or a claim of native support.
[Pinned HELLO implementation](https://github.com/redis/go-redis/blob/v9.22.0/commands.go#L360-L377).
For example, the default native `DBSIZE` policy queries one Cluster node, not
an aggregate of every primary. The Provider does not install a dynamic command
policy resolver. Other server-global administration and SCAN can also be
node-local; Ring is not a server-side cluster; replica reads may be stale; a server
error can follow partial script/module effects. See the [executed profiles](verification.md).

Primary source anchors:
[Universal selection](https://github.com/redis/go-redis/blob/v9.22.0/universal.go),
[Cluster routing](https://github.com/redis/go-redis/blob/v9.22.0/osscluster.go),
[Ring](https://github.com/redis/go-redis/blob/v9.22.0/ring.go),
[Sentinel](https://github.com/redis/go-redis/blob/v9.22.0/sentinel.go),
[transactions](https://github.com/redis/go-redis/blob/v9.22.0/tx.go),
[CSC](https://github.com/redis/go-redis/blob/v9.22.0/csc_integration.go),
[automatic batching](https://github.com/redis/go-redis/blob/v9.22.0/autopipeline.go).
