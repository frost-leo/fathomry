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

# Nacos configuration Provider adapter

[Documentation](../../../../../README.md) / Public adapter reference

**Audience:** projects selecting remote settings and adapter maintainers.
**Status:** implemented finite reads; native loopback TLS/authentication and consumer
tests, not new external-service, multi-node or production qualification.
**Package:** `github.com/frost-leo/fathomry/adapters/configuration/remote/nacos`.

## Responsibility and call sequence

`New(Options)` validates and freezes explicit bootstrap without network I/O or
native clients. Pass its Provider to [configuration.Load](../../../../framework/configuration/interface.md).
The adapter owns authentication, finite acquisition and cleanup using the
[existing Nacos integration](../../../../internal/configsource/nacos/v2/interface.md).
The framework owns environment capture, strict preparation and detached results.
The business project never receives native handles or copies their assembly.

Each load opens a transient owner, reads selected keys in layer order, then joins
cleanup before returning. It does not invoke the high-level SDK configuration
client, start a watch, use snapshot/failover files, or retain credentials/tokens
in a global registry. Bootstrap credentials are retained in the selected Provider;
tokens/connections are scoped to one load.

Only this adapter selects the Nacos dependency graph. Framework contracts do not
import adapters, and local-only projects do not acquire the remote graph.

## Explicit bootstrap

Bootstrap must be available before the remote settings it locates. Loaded content
cannot authorize new endpoints, identities, keys or credentials.

| Declaration | Meaning and bound |
| --- | --- |
| SchemaVersion | Project-declared data schema, default 1; not inferred from content, SDK or Go API version |
| Servers | 1–8 distinct members of one authorized cluster, in initial failover order |
| Server.HTTPURL | HTTP authentication origin with root or /nacos context, <=2048 bytes; no userinfo, query, fragment or redirects |
| Server.GRPCAddress | Explicit host:port, <=512 bytes; no port offset or security inferred from HTTP |
| Namespace | Empty means native default; other IDs pass unchanged, <=128 UTF-8 bytes |
| AppName | Default fathomry, <=128 UTF-8 bytes; technical application label, not Run identity |
| Username / Password | Both present or absent; <=256 / 4096 UTF-8 bytes; never implicitly discovered |
| RootCAPEM | <=64 KiB trust roots replacing system roots; empty uses system roots; no trust-all mode |
| AllowInsecure | False requires independently verified HTTPS and gRPC TLS; true explicitly selects plaintext gRPC and permits HTTP |
| RequestTimeout | Total acquisition budget for all selected keys, including authentication/registration/failover; default 10s, 1ms–1min |
| RetryDelay | Existing native registration retry delay; default 100ms, 1ms–1min; no additional adapter retry loop |
| ConcurrentLoads | Default 4, 1–16 transient owners per Provider, including pending cleanup; saturation immediately returns LimitExceeded |
| Sources | 1–3 distinct group/data-ID pairs, with unique public names and Base/Environment/Local layers |

Source Name is a public non-secret label using the framework's 1–64 character
lowercase ASCII letter/digit/dot/underscore/hyphen grammar. Group defaults to
DEFAULT_GROUP. Group and required DataID are private UTF-8 identifiers <=128 bytes,
without edge whitespace or control characters. Namespace/AppName use the native
identifier rules too. Endpoint, credential and key errors do not echo their input.

Layer denotes precedence, not transport: a remote source can occupy the Local
overlay layer without reading a local file. This profile selects one remote
Provider, not a mixture of local and remote adapters. Zero sources is refused;
defaults-only loading can explicitly use the local adapter's empty file profile.

All source documents use the common YAML-mapping preparation grammar, including
JSON syntax. Native ContentType is only a hint and cannot change that grammar.
Original content preserves exact integers and dynamic-key casing until validation.

## Absence, failures and diagnostics

Optional permits only an observed native missing configuration (code 300).
A successful empty/whitespace response, malformed data, permission denial, transport
failure, unsupported encrypted content or capacity refusal is not absence.
Required missing and denied acquisition return Unavailable with safe
`configuration.Missing` or `configuration.Denied` causes for errors.Is.
Native messages and error graphs are withheld, including URL/key/namespace text.

Malformed/empty/unsupported content maps to Invalid; bounded capacity failures to
LimitExceeded; caller cancellation or the total deadline to Cancelled. An observed
native request's context.DeadlineExceeded cause or typed gRPC DeadlineExceeded
status also maps to Cancelled with a safe context.DeadlineExceeded cause, even if the
outer context has not signaled yet. This preserves request-budget evidence without
claiming the caller's context was canceled or exposing the native error graph.
Caller cancellation takes precedence and preserves its intentional cause. Native
error text or connection-retirement cancellation alone does not establish a caller
cancellation or deadline. Caller cancellation/deadline identities remain deliberate
inspection data. Failures have only the approved Provider/source labels; a failed
acquisition returns zero Input and a failed framework load returns zero Configuration.
An earlier successful read is never substituted after later failure.

Public options, endpoints, source declarations and Provider handles have restricted
ordinary formatting and refuse JSON persistence/reconstruction. Raw documents and
explicit Value copies remain deliberately sensitive; safe Description excludes
source keys, endpoints, credentials, namespace, hashes and values.

## Ownership, bounds and limitations

New copies retained strings and slices. Providers support concurrent reuse; copies
share admission. Separate Providers have separate allowances, so applications still
bound Provider population and their own caller goroutines.

The allowance is retained until cleanup completes. Close uses a cancellation-free
context to join the transient native owner; no background cleanup is abandoned.
Native DNS or caller HTTP trace hooks can delay return beyond RequestTimeout or
caller cancellation. This is an intentional cooperative deadline, not a hard
wall-clock promise. No public Close is needed because no native owner survives a
completed call.

Each finite key read registers and closes a session; one load can therefore pay
up to three session setups plus authentication. That favors truthful ownership and
optional-source handling over a warm persistent client's efficiency. No benchmark
or optimization claim is made. Existing native registration retries/failover stay
inside the selected member set and shared load deadline.

Each raw document is <=1 MiB and at most three are retained. The native integration
also bounds wire messages, dial/TLS work and responses; these do not bound total
process RSS, parser/copy costs or caller-retained results.

Fresh server responses do not prove propagation of a preceding write, a common-time
multi-key/multi-member snapshot, service readiness or authenticity of schema labels.
No metadata is relabeled as a durable Run input, content fingerprint or reload event.

## Executable evidence

[Public adapter tests](../../../../../../adapters/configuration/remote/nacos/nacos_test.go)
exercise plaintext and TLS/authenticated reads, strict preparation, private
diagnostics, absence/refusals, frozen declarations, concurrent bounds and cleanup.
A blocked native TLS hook proves cancellation does not release admission while
cleanup is pending; using the expired request context for Close fails that control.

The [project fixture](../../../../../../adapters/configuration/remote/nacos/testdata/project)
contains public declarations and a thin entry. Its
[standalone executable test](../../../../../../adapters/configuration/remote/nacos/consumer_test.go)
runs against native loopback protocol servers. The framework's
[independent module test](../../../../../../framework/configuration/consumer_test.go)
separately builds/tests the remote consumer from an unreplaced file-proxy artifact
and checks dependency isolation. Neither is new deployed-Nacos service acceptance.

See the [project guide](../../../../../development/load-project-configuration.md).
