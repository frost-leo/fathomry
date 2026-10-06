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

# Public Nacos configuration Adapter

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go consumers and Framework scenario authors.
**Status:** implemented with loopback native-protocol/HTTP, resource-composition
and independent-module checks. This is not new production/TLS/HA certification.
**Package:** `github.com/frost-leo/fathomry/adapters/configsource/nacos/v1`.
Public API v1 is distinct from the selected Internal SDK-v2 integration.

## Explicit settings and ownership

Settings is loadable data, separate from runtime dependencies. Bootstrap credentials
must exist before fetching the remote application document. Validate performs no
service I/O. Open borrows an explicit public Runtime/Inbox and returns an Owner;
local construction is not readiness. Any non-nil Owner remains cleanup responsibility,
including partial initialization. Owner.Client provides direct operations.

Every supported native field is mapped: Name, Namespace, AppName, both server
endpoints, default Keys, DynamicKeys, Writable, Username/Password, RootCAPEM,
AllowInsecure, RequestTimeout, RetryDelay, ReconcileInterval, ConcurrentRequests,
QueuedRequests, Subscriptions and QueueCapacity. JSON/mapstructure names and
nanosecond units are defined in Settings. Zero values select native defaults:
10s request, 100ms retry, 30s reconciliation, 4 active requests, 1 subscription and
16 invalidations; queued_requests=0 disables waiting.

Both endpoints are explicit. TLS verification is default; plaintext must be
selected. Empty trust uses system roots; supplied roots replace them. Dynamic
selection and write permission are independent, and neither overrides service ACLs.
No implicit namespace rewriting, environment lookup or peer discovery occurs.

Owner.Handle is opaque and has no Close or native-client authority. A
resource.Instance[Handle] can use Owner.Release as its cleanup callback.
Using(ref, dependencies) constructs a public resource-backed Client. Each operation
captures one generation; guards retain subscription leases through actual cleanup.
Reconstruction changes future borrows, never retargets an existing subscription.

## Complete native capability routes

| Route | Preserved facts and limits |
| --- | --- |
| Read / ReadAll | Required-document reads, input order, fresh native sessions, no cached fallback or usable prefix |
| ReadRaw / ReadRawAll | Missing versus present-empty; failed batch index (-1 when unknown/success) |
| Document accessors | Detached raw bytes, requested key/namespace, MD5, content-type hint and Unix-millisecond modification observation |
| Watch / WatchKeys | Default or frozen explicit key set, native invalidations, initial/recovery/overflow Resync |
| ObserveRaw / ObserveRawKeys | Already-acquired complete batches/errors, bounded handoff; no second native refetch/recovery engine |
| Publish / CAS / Delete | Explicit permission, native metadata and NotIssued/Unknown/Acknowledged/Rejected even with an error |
| Search | Bounded v1 search and v3 metadata-only fallback only for an unsupported endpoint, never auth failure |
| Client.Source | Shared raw capture/observation selection without hiding any provider-specific operation |

Empty CASMD5 means unconditional publication, not create-only. No dispatched mutation
is retried by the Adapter. Acknowledgement does not prove immediate cache/replica
visibility. Search content absence is not a successful empty Read. Pagination and
multi-key reads do not establish a common-time transaction.

Source Capture uses ReadRawAll for defaults. Explicit selections use ordered raw
reads while retaining one resource generation for the entire batch. Layer role,
decoding format and required/optional policy remain the consumer's responsibility.

The native bounds remain 16 keys per selection, 8 servers, 1 MiB documents,
4 MiB aggregate content and 8 MiB wire bodies. Native permission, endpoint,
identifier, duration and queue validation remains authoritative for that profile.
See the [native contract](../../../../internal/configsource/nacos/v2/interface.md)
for protocol support limits.

## Observation, custody and shutdown

ObserveOptions queue_capacity defaults to 2 and accepts 1..16 complete batches.
Overflow drops oldest and makes Gap sticky for the next delivery. Sequence is local
observation order, not a remote revision. Batch.Err is acquisition failure, separate
from an interrupted Next wait. Batch.DocumentsCopy returns fresh wrappers and bytes
remain explicit RawCopy access. Successful shared-source observations already carry
the complete batch; do not reread them.

Owner/Watch/Observe each retain a lifecycle evidence reservation. Finite operations
reserve separately before native dispatch. Evidence records preserve mutation
state on failure. Redelivery invokes only the receiver, never Publish/Delete again.
Custody capacity includes live lifecycle claims; do not await a live owner on the
only finite-record reception path. Runtime/Inbox ownership remains with the caller.

Next cancellation does not cancel its subscription. Close stops and joins actual
local work; retry after an expired wait. Owner.ShutdownComplete and Release.Complete
separate release evidence from retained cleanup errors. Native cleanup can be
complete despite an error. Remote instantaneous unsubscribe and rollback are not
promised. Common-runtime work/byte admission is independent of native quotas;
raw observation reserves 16 MiB plus 4 MiB per public queue slot.

## Errors, presentation and tests

InspectError projects supported RPC result/error codes, HTTP status and explicit
sensitive server text without requiring an Internal import. Original errors remain
available for Go traversal. Ordinary formatting, slog and localization never print
that server text or payloads. Settings serialization, explicit metadata/raw access
and native-cause inspection are sensitive. Runtime handles reject reconstruction.

Definitions and Resources compose with the public atlas/presenter at the logging
boundary, without implicit locale/settings lookup during operations.

[Native-protocol fixture](../../../../../../adapters/configsource/nacos/v1/client_test.go),
[reads/ownership](../../../../../../adapters/configsource/nacos/v1/read_test.go),
[management/search](../../../../../../adapters/configsource/nacos/v1/management_test.go),
[observation](../../../../../../adapters/configsource/nacos/v1/watch_test.go),
[resource replacement](../../../../../../adapters/configsource/nacos/v1/integration_test.go) and the
[independent consumer](../../../../../../adapters/configsource/nacos/v1/testdata/consumer/consumer_test.go)
are executable evidence. Existing Internal service qualification is not a claim
that every public service/deployment scenario has been exercised.

## Package organization

The [Adapter tree map](../../../../../../adapters/README.md) defines this package's role
and file responsibilities; shared mechanisms, capability vocabulary, preparation
and concrete providers do not acquire identical APIs by convention.

Error definitions/resources remain offline. Runtime construction, translation
and native inspection live in `error.go`; formatting/serialization guards live
in `diagnostics.go`. Bare or transparently wrapped/joined public errors retain
existing ownership, details and causes. Explicit native frames keep their
provider classification; bounded internal graph search does not imply that
external `errors.Is/As` on arbitrary caller graphs is bounded.
