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

# Verify the Redis Provider

[Documentation](../../../../../README.md) / [Redis interface](interface.md)

**Audience:** maintainers verifying Issue #62.
**Status:** local protocol/ownership checks and authorized isolated-service tests;
not production or universal topology qualification.

## Local checks

From the Redis worktree root:

```sh
go test -race -count=3 -timeout=3m ./internal/cache/redis/v9
go test ./internal/cache/redis/v9 -run '^$' -fuzz '^FuzzFrame$' -fuzztime=10s -parallel=2
go run ./internal/cache/redis/v9/testdata/consumer
go mod tidy -diff
go vet ./...
go test -race -count=1 -timeout=10m ./...
go build ./...
git diff --check
```

The consumer executes construction/binding/close without a service. Its executable
build information must identify official `github.com/redis/go-redis/v9 v9.22.0`
with **no replacement**. There is no Redis SDK copy under third_party.

Adjacent command, connection, options, protocol, transaction, pipeline,
subscription, error and integration tests exercise these native/shared boundaries:
binary/nil/empty replies; per-command errors and original causes; malformed/huge
RESP declarations before native allocation; unknown response loss; saturated
evidence and shared borrowing; automatic completion after caller wait cancellation;
session cleanup without spare quota; TLS/mTLS trust rejection; immutable copies,
private diagnostics and unknown compatibility evidence.

The attribute oracle checks exact generic values and raw replies, plus consecutive
pipeline reply boundaries. The pinned SDK's RESP3 push pre-read calls
[`PeekReplyType`](https://github.com/redis/go-redis/blob/c7f59a2a950eb5131cc27bfff716d6d3382e4490/internal/proto/reader.go#L92-L107),
which discards leading attributes before `RawCmd` runs; `ReadRawReply` preserves
nested attributes. A synthetic Protocol-2 parser control bypasses that pre-read;
it does not certify attributes on a RESP2 service. Rejecting controls cover
combined byte/element/depth limits, missing decorated values and ambiguous metadata.

The TLS protocol peer is not a production Redis TLS deployment. Native constructor
tests and source review are not failover or real-effect evidence.

## Authorized existing standalone service

```sh
FATHOMRY_REDIS_SERVICE_CONFIG=/path/to/private-connection-info.yaml \
  go test -tags=redis_service -race -count=1 -timeout=3m \
  ./internal/cache/redis/v9 -run '^TestRedisService$'
```

This explicitly selected test fails, rather than skips, if the configuration is
missing. The mode-0600, at-most-128-KiB YAML file supplies a `redis` mapping with
host, port, protocol, username, password and database. It is private input, not a
repository asset. The current existing-service fixture selects explicit plaintext;
TLS transport is separately verified by local protocol tests.

The test uses cryptographically random `fathomry:gh62:` keys/channels, remembers
its exact keys, removes only those keys and verifies their absence with an
independent native client. It never FLUSHes a shared database, changes ACLs/server
settings, enumerates unrelated values or logs connection credentials.
Normal command counters and the content-addressed Lua script cache can change;
the test deliberately does not flush shared script caches.

The exercised existing profile reports Redis **8.8.0**, Standalone primary,
RESP3 with a separate RESP2 shape check:
binary/empty/missing/TTL, native data structures, partial pipeline and EXEC effects,
WATCH conflict, Lua/NOSCRIPT, bounded incremental SCAN, Streams group/claim/ack,
channel/pattern/sharded Pub/Sub lifetime, blocking deadlines, actual CSC hits/invalidation and native
automatic batching. This does not certify a Cluster/Sentinel endpoint.

Pub/Sub controls require one live receiver, immediate local socket release, then
independently observed remote removal within two seconds; socket close is not a
server-side unsubscribe acknowledgement. CSC must serve an actual hit and observe
invalidation within two seconds with a one-minute staleness backstop, so expiry
cannot substitute for invalidation. Automatic SET effects are read back using
an independent native client; local protocol tests also require batch width >= 2
for both success and initialization failure.

## Authorized temporary topology profiles

```sh
FATHOMRY_REDIS_TEST_SERVER=/absolute/path/to/redis-server \
  go test -tags=redis_service -race -count=1 -timeout=3m \
  ./internal/cache/redis/v9 -run '^TestRedis(Topologies|LostAcknowledgement|RingCompoundCommands)$'
```

The test starts only its own loopback processes with unique temporary directories,
ephemeral data/bus ports and no persistence. It signals and joins only those
processes; it never searches for or kills an existing Redis service. Running it
requires explicit authorization to create these temporary resources. The supplied
binary, not a container or newly installed service, determines the server version.

The selected local server is official Redis **8.2.1**, built outside the product
tree. Profiles cover native Ring distribution/node loss, a three-primary Cluster,
Universal selections, Sentinel primary/replica promotion, functions and reconnect
password refresh. Controls additionally exercise stale replica reads, authorized
MOVED routing, cross-slot transaction refusal and a real mutation whose reply is
discarded by a loopback fault peer. Temporary-server data is removed with those
test-owned directories after process termination.

Ring compound-command controls distinguish the real key from count/options/group
names when selecting a shard. Cluster controls first confirm the default node-local
`DBSIZE` policy. A separate test-only native dynamic resolver enables fan-out,
checks the node-call count, compares the sum against independent counts, and
requires the original injected node error in both aggregate and command evidence.
That resolver is not enabled or exposed by the Provider.

## Remaining qualification limits

Cloud maintenance handoff, large-scale/native metadata/RSS limits, Cluster
replica promotion, network partitions, every reconnect/loss scenario, externally
acquired streaming tokens, every module/version-specific command and Redis
8.10-only commands are not certified. Pub/Sub is not durable; caches are not
linearizable; Redis acknowledgement is not cross-node durability or business
completion. Review the [capability census](capabilities.md) before deployment.

Exact worktree manifests, commands, initial failed controls and current results
belong in the private `reference/issues/gh-62` evidence folder. No private
connection file, service inventory, raw log or unpublished research is required
to consume these source files. Qualification is revision-specific: recheck the
exact consuming dependency graph, source/configuration and service evidence after
baseline, dependency or behavioral changes. An earlier worktree's passing results
do not certify a newly integrated baseline.
