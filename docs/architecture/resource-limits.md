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

# Data guarantees and resource limits

[Documentation](../README.md) / Architecture

**Audience:** framework and integration maintainers.
**Status:** accepted architecture; implementation coverage is limited.

This topic states accepted cross-package obligations, not a claim that every
SDK or framework feature is implemented. See the [documentation map](../README.md)
for current availability and package contracts.

**Standard sections:** S07.

## Contents

- [Guarantees, granularity and bounds](#guarantees-granularity-and-bounds)

## Guarantees, granularity and bounds

An **Item** is the framework's business execution/accounting unit. A row or message
is a data unit. A logical operation can contain multiple SDK attempts and physical
batches; those batches may combine data from several Items. None of these mappings
is inherently one-to-one. Adapters preserve ownership across the mapping; Providers
return the technical granularity the SDK can actually establish. Missing per-Item
evidence remains unknown instead of being expanded from an aggregate success.

Business developers declare identity, ordering/conflict rules, required transaction
scope, and success conditions. The framework validates that its selected capability
and mode support the requested guarantee and provides the corresponding technical
protection. It must not silently downgrade the guarantee or shift undocumented
reliability obligations onto business code. Neither a database method nor a common
interface establishes cross-source, cross-service, or whole-Run atomicity.

Batching, streaming, retry, partial completion, and recovery must retain attribution
and distinguish complete empty output from missing output. Retry must not mix
unconfirmed old output with the current attempt or count attempts as new terminal
Items. Data/reference protocols must state completeness, ordering, expiry, and
recovery obligations; their formats and reconciliation algorithms are not chosen
here. Persisted references are not automatically still readable after retention.

Every supported mode must declare relevant bounds, units, enforcement scope, and
overload behavior. Review at least concurrent work, queued work and unresolved
receipts, records and bytes, per-record/per-batch size, connection and session
counts, SDK prefetch/decompression, background tasks, native memory, temporary disk,
and the evidence path itself. Bounds can use several owners, but must not have a
gap where admitted work or required evidence can grow without a declared limit.

Document whether overload waits, rejects, or uses an explicitly permitted lossy
path. Lossy telemetry cannot substitute for required effect evidence. A producer
buffer setting does not necessarily bound consumers; a count limit does not bound
bytes; Go heap metrics do not bound native memory. Guarantees unsupported by an
SDK mode need a restriction, an additional proven mechanism, or an explicit
unsupported status, not a claimed universal hard cap.

Local concurrency, rate, mutual exclusion, and shared quotas are different controls.
Framework constraints must remain effective across the workers and layers consuming
the actual resource scope. A per-instance limit does not prove an account-wide
limit. Provider retries, framework retries, and caller retries must have explicit
ownership and budgets; hidden multiplication of attempts is not acceptable. If
attempt-level evidence is unavailable, report that limitation instead of inventing
an exact count. No distributed limiter or feedback algorithm is selected here.

Millions of targets and long-running workflows motivate bounded processing, not
one client or goroutine per target. Workload bytes, simultaneous Runs, hardware,
latency objectives, and native-resource budgets remain unmeasured. Performance
claims require controlled comparisons at fixed correctness guarantees, including
overload and failure paths, not only favorable throughput.

## Implementation references

[Admission limits](../reference/internal/resource/admission.md) and [controlled calls](../reference/internal/invocation/interface.md).

This topic carries forward the [accepted integration standard](internal-sdk-integration.md)
from [Issue #3](https://github.com/frost-leo/fathomry/issues/3), including the
[Issue #15](https://github.com/frost-leo/fathomry/issues/15) boundary refinements.
