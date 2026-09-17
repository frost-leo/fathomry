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

# Redis v9 interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** internal composition and capability maintainers.
**Status:** implemented internal Provider, using unmodified official
go-redis v9.22.0. Local protocol tests and explicitly authorized service profiles
are separate from production qualification.
**Package:** `github.com/frost-leo/fathomry/internal/cache/redis/v9`.

## Responsibilities and call sequence

This Provider supplies bounded, independently configured Redis access. It is not
a business cache policy, lock/rate-limiter platform, scheduler, durable Run ledger,
public framework API, or generic client abstraction.

1. At single-threaded **process startup**, call `DisableNativeLogging` before any
   go-redis client is started. This deliberately uses the official SDK's global
   logging API. Do not call it concurrently or re-enable raw native logging while
   Providers run. Source constructors never silently change process logging.
2. Prepare `OptionsV1` with `Select` (or `SelectWithPassword`), attach
   `resource.WithLimits(selected, LimitsV1(options))`, then assemble. If strict
   resource overlays change bounds, attach the corresponding resolved policy.
   Construction does not prove connectivity or perform application readiness I/O.
   Native Ring heartbeat/background initialization can establish connections.
3. `Bind` joins the exact selection, authoritative admission and a separately
   owned `invocation.Inbox[Result]`. Borrowed aliases share identity, allowance and
   native resources; they cannot close the owner.
4. Use `Execute`, `Pipeline`, explicit sessions, subscriptions, or opt-in automatic
   batching. Inspect receipt outcomes **and** errors. An independent framework
   receiver must handle and release inbox deliveries; business error handling
   does not release evidence capacity.
5. End all callbacks and accepted work, return borrowers, then close the owning
   assembly. Incomplete cleanup remains owned with a continuation; repeat that
   continuation only under a composition-owned aggregate cleanup budget.

An explicit `PING` grant and `Execute(NewCommand("PING"))` perform readiness
with evidence. No raw native client, mutable command, options, hook, pool, callback
handle or source shutdown method escapes the facade.

## Configuration and authority

`OptionsV1.Version` zero selects format 1. SDK, configuration, source revision,
service/protocol and future Workflow/data versions are separate axes. Settings,
maps, lists and overlays are frozen by `resource.Prepare`; configuration objects
must not be mutated concurrently with preparation. No environment, credential
file, URL, default localhost, custom dialer or ambient trust discovery occurs.

The topology is explicit: standalone, sentinel, cluster, ring or universal.
Universal requires `UniversalMode`; changing address count does not silently
change the requested topology. `Addrs` contains seeds; `AllowedAddrs` is the
complete TCP endpoint authority, defaulting to seeds/Ring shards. Include every
authorized discovered master, replica and Sentinel. Unlisted redirects and
discovery targets are refused, not silently trusted. Only TCP is supplied.

TLS uses supplied CA PEM and peer-name verification, TLS 1.2 minimum, and optional
client certificate/private-key PEM. Plaintext requires explicit selection. Trust
and certificate rotation require a replacement source. Static data-node and
Sentinel credentials are distinct. `SelectWithPassword` reads a composition-owned
local `Password` snapshot on **new connections** with a frozen username. Replacing
it performs no network traffic and does not reauthenticate existing sockets.
No external token-fetch or arbitrary streaming-credential callback is installed.
Non-default ACL usernames with an empty password are refused, including after
snapshot replacement: native v9.22 HELLO omits AUTH for empty passwords, so silently
using that combination could run as the default user instead of the frozen account.
This limitation applies separately to Sentinel credentials; it is not a claim
that Redis itself forbids passwordless ACL users.

Bootstrap MaxIdleTime zero selects the five-minute default. An explicit overlay
`max_idle_time_ns: 0` disables pooled idle expiration and is translated to the
SDK's -1 sentinel, not its different zero/default meaning. MaxLifetime zero means
no age-based expiration.

Commands are binary-safe immutable string vectors, not arbitrary Go values or
native marshal callbacks. `NewCommand` copies argument-slice storage. Redis bytes
may include NUL/non-UTF8; no JSON conversion is imposed. No commands are granted by
default. `Commands` grants known data commands; `AdminCommands` explicitly grants
administrative, module and extension commands. Exact `COMMAND|SUBCOMMAND` grants
are available. Unknown/new server commands are not silently treated as safe data
operations: composition must explicitly choose their raw authority.
Controlled connection/transaction callbacks additionally require `AllowSessions`;
subscription callbacks require `AllowSubscriptions`. Both default to false.

These grants are **not a sandbox or key/tenant ACL**. Lua, functions, modules and
administrative commands can have wider effects than their visible arguments.
Server ACLs, service identity and composition own that authority. Key prefixes,
hash tags and `ReadOnly` routing are not authentication.

AUTH/HELLO/SELECT, CLIENT, raw WATCH/MULTI/EXEC, subscription state changes,
MONITOR, replication protocol and equivalent connection-control routes are
refused even through raw grants. Controlled sessions and subscriptions own their
respective protocol transitions. Unbounded native iterators, native hooks,
raw handles/writers and deferred mutable Cmder accessors are not exposed.

## Execution shapes and results

- `Execute` is a finite native command, including finite-time blocking commands.
  `Pipeline` preserves positional results across partial failure, but is not
  atomic. Independent Cluster commands can span slots.
- `Dedicated` pins a node selected by a route key and owns a private native pool
  for the callback's lifetime. It closes that pool on every exit; it does not
  return connection-local WATCH/HIMPORT state to another caller. Ring operations
  in this callback explicitly address that shard, without implicit rehashing.
- `Watch` issues WATCH on the pinned connection and calls its callback once.
  `Session.Transaction` uses MULTI/EXEC on that same connection. Cross-slot
  transactions are rejected by Redis, never split into several transactions.
  UNWATCH and pool close run through existing ownership even at evidence
  saturation. No automatic transaction callback retry occurs.
- Session methods reserve independent child receipts with the root Call as
  Parent. A session root reports callback/cleanup lifetime, not the aggregate
  business success of its children. The convenience `Client.Transaction`
  returns that root; command results arrive separately as `Call+".exec"`.
- `Subscribe` is callback-owned and `Receive` is pull-based. There is no native
  Channel worker or hidden lossy send queue. Establishment confirmations,
  reconnect confirmations, messages and pongs remain distinct events. Redis
  Pub/Sub is not lossless or durable across reconnects. Callback completion closes
  the local subscription connection, not an acknowledged server-side unsubscribe;
  independent observation of remote removal can lag local socket release.
- SCAN-family pages preserve native cursors and replies. COUNT is a hint, not a
  byte bound or snapshot. For Cluster/Ring, raw SCAN requires a pinned session;
  it scans **one node**, not the entire topology. Topology changes can invalidate
  cursor assumptions. Composition chooses nodes/routes and incremental progress.
- Streams expose native group/pending/claim/ack commands through grants.
  XACK is a Redis technical acknowledgement, not a framework Item/Run decision.

`Result.Commands()`, `Value.Elements()`, `Value.Pairs()` and `Value.Bytes()`
return independent outer storage. Nested values are immutable and concurrent-read
safe. No native mutable result is published. Null, empty text, zero numbers,
arrays/maps, booleans, doubles and decimal big integers remain distinct.
Top-level `redis.Nil`, `redis.TxFailedErr`, server and transport errors retain
intentional `errors.Is/As` inspection. Nested RESP errors stay in `Value.Err()`.

`Replied` means a native reply was observed, including a server error.
`TransactionAborted` identifies the WATCH-conflict sentinel.
`CacheOrReply` deliberately does not claim a fresh server read.
`Unknown` after SDK entry includes missing acknowledgements, framing failures
and timeouts. These states do not prove durability, uniqueness, rollback or retry
safety. Scripts and transaction runtime errors may leave partial mutations.

## Bounds, cancellation and cleanup

Source options bound admitted calls, waiting count/bytes, command/argument count,
request bytes, RESP frame bytes/elements/depth, and physical sockets/dials across
all native pools, dedicated connections and auxiliary traffic. A complete frame
is checked **before** native length-based allocation. Parser failures poison the
connection and their original cause survives subsequent native socket checks.
Response reservations conservatively include decoded values and retained errors;
they are not measured heap/RSS limits. Cluster working reservations also cover
native fan-out across the configured endpoint authority. Aggregate scalar bytes,
elements and depth are checked again before immutable result publication; a
native aggregate exceeding these bounds is rejected with its reply evidence intact.

Native pool capacity is partitioned across declared endpoints, reserving sockets
for dedicated/subscription calls and the Sentinel listener. PoolSize and
MaxActiveConns enforce that partition; MaxIdleConns alone is not a hard bound.
Insufficient topology/headroom settings are refused. Ring preserves established
availability on local capacity exhaustion; its boolean heartbeat API may reset an
incomplete failure streak, but must not remove or revive a shard on that evidence.

RESP3 attributes and their decorated value share one frame budget. Attributes
nested inside metadata are refused because the pinned native discard/raw decoders
disagree on their boundaries. Malformed Pub/Sub decoding retires the subscription
and finishes its child evidence. Initialization failures never imply a decoded
application reply; a pinned-decoder marker preserves that distinction and native
fan-out error causes without an SDK patch.

Native pool buffers are 4 KiB each; topology/command metadata, TLS/OS allocations,
native housekeeping and caller-retained result copies are separate costs.
The endpoint allowlist is not a complete bound on native topology metadata.
Cache memory is an SDK estimate, not measured RSS. There is no distributed
account rate controller or cross-process quota.

Ordinary node command retries are disabled and dialing uses one attempt.
Cluster `MaxRedirects` defaults to zero: its native loop also retries network
failures, so enabling redirects **can amplify mutations after response loss**.
Attempts are explicitly inexact: observed SDK entries do not count every handshake,
topology refresh, heartbeat, redirect or native attempt.

Per-operation contexts and source timeouts bound socket deadlines, including raw
blocking operations. Cancellation can take until a socket deadline; it is not an
instant remote cancellation. Session lifetime must have a deadline or explicit
cancellation. Callbacks run on the caller's stack and must cooperate. Concurrent
session/subscription operations are rejected rather than queued behind themselves.
A copied session/subscription handle shares its original gate and lifetime.
A leaked/noncooperative callback continues to own its reservation.

Pool close starts only after admitted users/borrowers end. The transport seals
new dials, closes owned sockets and joins in-progress dialing; the native close
result is retained. Native background bookkeeping has no public universal join
handle and may finish after network quiescence. The Provider does not claim that
every SDK goroutine has exited at that instant.
Synchronous socket/SDK close can outlast a cleanup deadline; assembly close retains
its one owned cleanup worker and continuation rather than pretending it stopped.

## Experimental and service-dependent capabilities

See the [capability census](capabilities.md) and [verification](verification.md).
Neither a method name nor an upstream stable release certifies a service profile.
`Profile` reports non-secret effective configuration; service version remains
unknown until external evidence is supplied to compatibility assessment.

Sources and rationale: [Issue #62](https://github.com/frost-leo/fathomry/issues/62),
[SDK release](https://github.com/redis/go-redis/releases/tag/v9.22.0),
[SDK integration architecture](../../../../../architecture/sdk-integration.md),
[resource lifecycle](../../../../../architecture/resource-lifecycle.md) and
[errors/evidence](../../../../../architecture/errors-and-evidence.md).
