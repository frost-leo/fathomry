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

# Nacos v2 configuration interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework configuration-composition and integration maintainers.
**Status:** implemented bounded SDK-component profile; single-server plaintext
test-environment checks have run. Production TLS and multi-node acceptance are
not certified.
**Package:** `github.com/frost-leo/fathomry/internal/configsource/nacos/v2`.

## Responsibility and selected native surface

The integration uses official nacos-sdk-go/v2 v2.3.5 generated `Request` and
`BiRequestStream` clients, native configuration/control request types and native
response decoding. It does not start `clients/config_client`, the SDK global RPC
engine, snapshot/failover files, cloud credential plugins or native notification
workers. No SDK fork, private-field mutation or process-global hook replacement
is used. The dependency-upgrade TODO in `go.mod` and
[GH-21](https://github.com/frost-leo/fathomry/issues/21) states the conditions for
reassessing this compatibility path; upstream fixes must pass the preserved
counterexamples and all applicable contracts before removing safeguards.

Fathomry owns admission, HTTP password login, native connection sessions, bounded
push handling and cleanup. This is not a transparent facade over the high-level
SDK client. It provides no public application loader, precedence policy,
configuration publication API, automatic reload, service discovery, migration or
Temporal commands.

## Call sequence and ownership

1. Resolve `OptionsV1` outside Nacos. `Open` validates and freezes it before
   constructing local transport ownership; return is not service readiness.
2. `Read` acquires one preselected key. `ReadAll` acquires every selected key in
   input order, returning nil on any required failure.
3. Give independently owned `Document.RawCopy()` bytes their authorized
   `resource.LayerKind` and use existing `resource.Prepare` once. Local content
   can come from the existing Viper path. Neither SDK chooses layer priority.
4. `Watch` creates a locally owned subscription. `Next` returns invalidation
   metadata or an explicit observation gap. A `Resync` requires a complete
   re-read; it does not apply configuration or prove application adoption.
5. Close subscriptions or their owning client. Retain the same owner after a
   timed-out Close and retry with a fresh cleanup budget.

Open's context owns the whole lifetime. Canceling a Next wait does not cancel
the subscription. Clients and subscriptions support concurrent operations; do
not copy their runtime structs. Returned documents/changes are immutable, and
RawCopy returns a new sensitive byte slice. Bootstrap values are borrowed only
during Open; concurrent caller mutation during Open is unsupported.

Finite reads intentionally create and close one Nacos session per read/batch,
rather than reuse a possibly replaced TCP connection without Nacos registration.
A Watch owns a persistent session and rebuilds it after loss. This has measured
startup/connection costs and is not a claim of parity with a warm high-level SDK
cache or persistent native client.

## Bootstrap and resource limits

`OptionsV1` is a process-local Go contract, independent of SDK major v2 and of
application document formats. It refuses JSON persistence/reconstruction.

| Input or budget | Meaning |
| --- | --- |
| Name | Required 1–64 lowercase ASCII letters/digits/dot/underscore/hyphen; composition owns uniqueness and must exclude secrets |
| Namespace | Native default form when empty; other IDs pass unchanged, up to 128 UTF-8 bytes; server aliases such as `public` are version-dependent |
| AppName | Default `fathomry`; bounded 128-byte native application label, not a Run or Item identity |
| Servers | 1–8 explicit members of one authorized cluster, not configuration layers |
| ServerV1.HTTPURL | At most 2048 bytes; HTTP(S) root or `/nacos` context path; no userinfo, query, fragment or redirects; HTTP requires AllowInsecure |
| ServerV1.GRPCAddress | Explicit host:port, at most 512 bytes; port 1–65535; gRPC does not inherit security merely from the HTTP URL |
| Keys | 1–16 distinct group/data-ID pairs; each at most 128 UTF-8 bytes, without edge whitespace/control characters; omitted group becomes DEFAULT_GROUP |
| Username / Password | Both present or absent; limits 256 / 4096 UTF-8 bytes; no environment or cloud credential discovery |
| RootCAPEM | Optional trust roots, at most 64 KiB; otherwise system roots; no trust-all mode |
| AllowInsecure | False by default; true explicitly selects plaintext gRPC and permits isolated-test HTTP |
| RequestTimeout | Default 10 s, allowed 1 ms–1 min; cooperative admission/request/setup/decode phase budget |
| RetryDelay | Default 100 ms, allowed 1 ms–1 min; minimum delay for registration retries and observation recovery |
| ReconcileInterval | Default 30 s, allowed 1 s–5 min; technical state comparison, not application reload |
| ConcurrentRequests | Default 4, allowed 2–16; finite reads/batches and persistent subscriptions share this admission allowance |
| QueuedRequests | Default 0 refuses overload; allowed 0–64, FIFO wait and separate byte reservations |
| Subscriptions | Default 1, positive and less than ConcurrentRequests, leaving finite-read capacity |
| QueueCapacity | Default 16, allowed 1–64 invalidations per subscription |
| RPC input / output | Receive at most 8 MiB per gRPC message; send at most 64 KiB; header list at most 32 KiB |
| Protocol JSON | Valid UTF-8, one value, no duplicate keys, at most 64 levels and 32768 value nodes |
| Raw content | At most 1 MiB per document and 4 MiB per returned batch; one additional bounded document may be acquired before aggregate rejection |
| Login response | At most 64 KiB; token at most 16 KiB; TTL must be integer seconds in 1–604800 |
| Native acquisitions | At most ConcurrentRequests simultaneous dial/TLS phases, separately from logical admission; at most 64 resolved addresses attempted serially |

Each admitted logical operation reserves 16 MiB for a unary response and its
session's incoming stream message, retaining that allowance through decode.
Queues retain bounded metadata and bounded-count errors, not document payloads.
Reservations are not process RSS: TLS/protobuf/JSON buffers, copies and native
errors have additional costs. Composition bounds client population and retained
results/copies. Native error graphs remain intentionally inspectable rather
than silently truncated into supposedly lossless errors.

A subscription uses its permit for its entire lifetime. There is one receiver
and one serialized stream sender per session; setup is sent before the receiver
starts, and that receiver owns control acknowledgements. ACK timeout cancels the
entire session instead of abandoning a goroutine-per-send. Default/native gRPC
transport reconnection is refused within an existing Nacos epoch; new setup and
listen registration are required.

DNS lookup cannot be forcibly joined by canceling the standard resolver's wait.
It remains in the bounded native-acquisition account until actual return. A
timed-out Close therefore retains pending ownership. Sockets are registered before
handoff, late handoff is refused, and shutdown cancels sessions/closes sockets
before waiting. No forced termination of arbitrary caller callbacks or native DNS
is promised; caller-context expiry is not proof of released resources.

## Protocol, authentication and read evidence

The session performs ServerCheck, opens the native bidirectional stream and sends
ConnectionSetup, then requires a successful HealthCheck response. Sending setup or
sleeping is not registration proof. Code 301 can be retried up to 16 times within
the operation budget; exhaustion retains the last native error.

Configuration-query code 300 is missing, distinct from a successful empty string.
Empty/whitespace required documents are refused with ErrEmpty. Required content
is never replaced with an earlier read, snapshot or failover file. Encrypted
content is unsupported. Optional native MD5 is checked against original UTF-8
bytes when present; it is not authentication or an ordered revision. A zero
LastModifiedMillis or empty MD5 means unavailable observation.

Query replies do not echo the key; Document.Key is the requested identity.
ContentType is a native hint, not validation of the application's schema.
An explicit successful query observes the server's response; it does not prove
that an earlier publication has propagated to every server-side cache. The
service fixture waits for its exact published content/deletion before asserting
dependent behavior, rather than equating a write ACK with read-after-write
visibility. No stronger server consistency is supplied by this integration.
Required validation, exact numbers, dynamic key casing, list/null/empty semantics
and frozen configuration belong to existing preparation. Stable inputs are
assumed; no common-time snapshot across keys, servers or local input is implied.

Password login follows the selected HTTP context's native
`/v1/auth/users/login` route. Tokens are instance-local, refreshed with bounded
serialized login, and invalidated conditionally after the token actually used is
denied. A late rejection cannot discard a newer cached token. Nacos headers are
placed in the SDK payload metadata, not gRPC HTTP metadata. Both HTTP login and
gRPC independently verify certificates/hostnames in the secure profile.

Finite read failover stays within the configured member set and one total budget.
Failures never authorize addresses suggested by a server reset. No exact physical
wire-attempt count or atomic multi-member consistency is claimed.

## Observation and reconciliation

Watch initially reports a full-set resynchronization requirement after successful
registration. ConfigChangeNotify and the batch-listen response's ChangedConfigs
both produce bounded invalidations. Detection/change/reset ACKs preserve native
request IDs; reset acknowledges then retires the epoch, never follows an
unapproved pushed endpoint.

Reconciliation retains only the previous hash/presence of each selected key,
compares later queries, and commits its baseline only after the whole listen
response is validated. Thus an unpushed update or deletion is not silently
absorbed into a new baseline. Stream loss/reset creates a gap and new registration
with full resync. Queue overflow discards the oldest event and makes resync sticky
on the next delivery. Duplicates and coalescing are possible; this is not a durable,
ordered, exactly-once or resumable event log.

For a default/empty-namespace listener, native null/absent tenant in a push is
accepted as that default form. It is never accepted for a named namespace.
Unknown keys, wrong namespaces and malformed required fields retire the session
rather than mutate an unrelated configuration.

## Errors, diagnostics and evidence

Qualified errors use ProviderID `configsource.nacos.v2` and private fault kinds.
Native Go/gRPC/HTTP/JSON/TLS/context errors remain in standard error chains where
available. RemoteError retains observed result/error codes; its Message method
deliberately exposes sensitive native text, while ordinary formatting does not.
No missing metadata is invented by parsing human-readable error text.

Options, endpoints, keys, clients, documents, subscriptions and changes have
restricted ordinary formatting and runtime JSON refusal. Typed nil formatting is
safe; Go's ordinary nil-to-JSON-null behavior is not runtime reconstruction.
Namespace/key/MD5 getters and RawCopy deliberately expose data; they are not safe
metric labels. The package installs no SDK logger/parser/cache hooks and is not a
sandbox against unrelated Go code replacing global library facilities.

The [maintainer workflow](../../../../../development/testing.md) distinguishes
ordinary loopback tests, actual consuming build evidence and opt-in service work.
The current single-server test used the declared Nacos 3.2.4 instance through
explicit isolated plaintext endpoints and password authentication: raw read/write
fixtures, pushes, connection replacement/re-registration, denial, malformed
content, deletion and cleanup were exercised. Periodic reconciliation is longer
than the bounded push-test window so it cannot substitute for push acceptance.
The service refused empty publication (result 500/error 400); successful-empty
query handling remains a local-fixture check. This is not production TLS,
multi-node failover/discovery or service-artifact integrity certification.

Source/test categories are client ownership, native transport, authentication,
protocol, raw acquisition and observation, each with adjacent tests.
[Integration and rejecting preparation control](../../../../../../internal/configsource/nacos/v2/integration_test.go),
[actual consumer](../../../../../../internal/configsource/nacos/v2/testdata/consumer/main.go),
[opt-in service gate](../../../../../../internal/configsource/nacos/v2/service_test.go)
and [equal-session benchmark](../../../../../../internal/configsource/nacos/v2/read_bench_test.go)
provide the executable calling examples.
