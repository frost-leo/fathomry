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

# Nacos configuration Adapter

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent projects selecting an authorized Nacos cluster.
**Status:** implemented; loopback protocol/TLS tests and opt-in isolated service qualification.
**Package:** `github.com/frost-leo/fathomry/adapters/configsource/nacos/v1`.

Public API v1 is independent of the private integration's SDK-major v2 and its
selected Nacos Go SDK v2.3.5. No SDK upgrade or high-level cached client is introduced.

## Explicit bootstrap

`Select(Settings)` validates/copies data without constructing a Client, connecting,
reading environment or discovering endpoints. Settings, Server and Document are
ordinary exact-tagged sensitive DTOs, admitted inside project T. Runtime Selection
is separate and guarded.

Select 1–8 authorized `Server` members with explicit HTTPURL (including the chosen
root or /nacos context) and GRPCAddress. No gRPC port offset is inferred.
Select 1–16 `Document` slots with unique non-secret Name and native Group/DataID;
an empty Group means DEFAULT_GROUP. Namespace is explicit; empty requests the
native default form. Namespace aliases are not automatically rewritten.

TLS verifies each channel unless AllowInsecure explicitly selects plaintext for
an isolated target. RootCAPEM replaces system roots when present (maximum 64 KiB).
Username and Password must both be present or absent. Bootstrap sizes, identifiers
and protocol modes retain the [private profile](../../../../internal/configsource/nacos/v2/interface.md).
No ambient HTTP proxy or global SDK client is used.

Durations use nanoseconds:

| Setting | Zero default | Accepted nonzero range |
| --- | --- | --- |
| RequestTimeout | 10 seconds | 1 millisecond–1 minute |
| RetryDelay | 100 milliseconds | 1 millisecond–1 minute |
| ReconcileInterval | 30 seconds | 1 second–5 minutes |

These are inherited operational bounds, not measured latency recommendations.
Limits are per actual operation/owner; equal selections do not imply shared clients.

## Fresh raw capture and one live loop

Use the [common source contract](../../v1/interface.md). Capture opens finite
ownership, reads every selected key and joins cleanup before returning.
It never uses stale Current/cache/backup data. Observe returns an owner first,
then uses one native push/registration/reconciliation/recovery loop.
Framework consumes the complete bytes already acquired by that loop.

Native code-300 absence is Missing. Successful empty content is Present, even
though the existing private required-document Read refuses it. Raw capture preserves
decoded protocol UTF-8 content, not surrounding wire bytes. Encrypted/invalid
protocol data, denied requests and size failures yield no new batch.
Invalid Unicode escape sequences are refused before native decoding; valid
surrogate pairs and genuine replacement characters are preserved, not repaired.
The raw 1-MiB/document and 4-MiB/batch ceilings apply before retained publication;
native receive-wire messages remain bounded at 8 MiB. MD5/type/mtime are neither
authentication, ordered revisions nor application acceptance.

Push loss/coalescing, disconnect and failed acquisition remain dirty until successful
complete reconciliation. A second event is not required. Nacos registration is
re-established after a lost session; one completed multi-key batch is still not a
transaction. Native callbacks do not execute caller business callbacks.

A validated ChangedConfigs response also requests another pass after RetryDelay,
without waiting for a new push or the periodic interval. This happens in the same
raw observation loop/session. The earlier complete point-in-time capture can be
published before that follow-up; Available is not a claim of current remote truth.
Metadata-only private Watch retains its original acquisition behavior.

Failures expose stable Adapter conditions and deliberate standard/context causes,
including RPC DeadlineExceeded observed before the caller context expires.
Each failed native RPC attempt preserves its own standard cancellation/deadline
evidence before aggregation. Typed authentication/permission denials invalidate
only the cached token used by that request; the owned retry loop reauthenticates.
Native protocol/parser text and private fault graphs are not automatically retained
as public causes. Explicit context causes can still be sensitive.
AcquisitionFailure additionally supplies the safe source alias, known selected
document slot and public operation phase. A separate private raw-result index
provides slot evidence; authentication/session/listener/cleanup failures without such evidence
report an unknown slot rather than searching native text or caller cause graphs.
Multi-attempt aggregates have no unique slot. An aggregate-limit index identifies
the slot where the batch crossed its bound, not a per-document-size verdict.

Static declarations use `fathomry.adapters.configsource.nacos` and require no
constructed selection/client. Local-only and pure Framework graphs exclude Nacos.

## Qualification and non-goals

The [independent protocol consumer](../../../../../../framework/configuration/v1/testdata/consumer/remote_test.go)
covers raw Capture/Observe and Framework Load/Watch, TLS/password authentication,
bounds, denial, push, re-registration and recovery without another event.
The [opt-in service consumer](../../../../../../framework/configuration/v1/testdata/consumer/service_test.go)
uses only explicitly authorized, previously absent generated keys, verified cleanup
and an owned local relay for its own disconnects. See [verification](../../../../../development/configuration-testing.md)
for invocation, exact-profile distinctions and limits.

No configuration publish/delete/search API, namespace/account administration,
native handle export, distributed transaction, production deployment attestation,
automatic credential activation or business-client reconstruction is provided.
The fixture's narrow admin operations are test code, not public Adapter features.
