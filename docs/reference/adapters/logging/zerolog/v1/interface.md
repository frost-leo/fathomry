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


# Public zerolog logging

[Documentation](../../../../../README.md) / Public Adapter reference

**Audience:** independent applications and Framework compositions.
**Status:** implemented bounded synchronous zerolog v1.35.1 profile, locally qualified.
**Package:** `github.com/frost-leo/fathomry/adapters/logging/zerolog/v1`.

## Preparation and explicit output selection

Use Settings format 1, Prepare, Policy/Compose, independently owned operation and
evidence mechanisms, then Prepared.Open. Nil pointers select native defaults;
explicit zero/false remains present. Durations are nanoseconds. Preparation opens
no file and creates no native event. The final frozen native selection owns both
authoritative metadata and construction; public code does not copy native defaults.

Source minimum defaults to Info; each sink defaults to Trace. There are seven
named severities (Trace, Debug, Info, Warn, Error, Fatal, Panic), never process
exit/panic actions. Default record ceiling is 16 KiB (1 KiB–1 MiB), timeout 5 seconds
(1 ms–1 minute), queue zero (0–64). At most eight ordered named outputs:

- writer: a caller-owned io.Writer;
- record: a caller-owned conservative RecordWriter;
- managed-record: an explicitly qualified ManagedRecordWriter;
- file: an owned existing private directory, native size/count/gzip policy.

Dependencies supplies exact typed name maps, not loadable callbacks. Missing,
extra, typed-nil or mismatched bindings refuse before source effects. No stdout,
file, telemetry provider or network target is implicit. Borrowed outputs are
never synced or closed, including when they expose those methods.

File defaults are 8 MiB and four backups (1–128), optional synchronous gzip.
The existing native marker/sequence policy remains distinct from Zap's flock
profile: `.fathomry.lock` and `current.jsonl` identify managed ownership/data.
See [native file contract](../../../../internal/logging/zerolog/v1/file-output.md).

## Calls, records and safe ingress

Owner.Client/Handle are non-owning. Using captures actual Fixed/Follow generations
under an explicit budget. WithID changes public correlation, not native allowance.
Log is caller-cancelable and captures business time/PC; LogEntry preserves original
time and return PC. Zero means absent. Caller path/function encoding is opt-in.
Sync and Rotate act only on owned files and report borrowed-output non-applicability;
a source without files refuses maintenance rather than flushing borrowed sinks.

Log accepts existing bounded slog scalar/null/group attributes plus Attribute
with closed shared logging.Value. Existing native scalar input retains its
historical byte accounting and full local uint64 range. New values support
float32, binary/base64, UTF-8 byte-string, arrays and nested maps. Null and present
empty collections remain distinct. Total keys: 64; nodes: 256; depth: 8; keys:
1–128 UTF-8 bytes, unique per map. Message/key/payload charges and the final
encoded JSON line must each fit the selected record ceiling. Nothing truncates.

No arbitrary Resolve, Error/Stringer, reflection, raw fragment or user marshaler
runs. Safe public failure/i18n diagnostics are projected before native entry;
explicit caller content still needs its own privacy policy. Library-produced
numeric/collection fragments are not caller raw-JSON authority. Float32/64 preserve
the selected default finite numeric representation without native global-precision
callbacks. Native global Disabled and unsupported binary_log build still refuse;
this is not process-global independence or CBOR qualification.

Borrowed record sinks receive immutable Record snapshots with explicit Time, PC,
Caller, Level, Message, Source, Correlation, FieldsCopy, AttributesCopy and JSONCopy.
Closed public values have copying collection/byte accessors. Retention outside a
call belongs to that sink's own bounded owner. OTel composition reads typed fields,
never JSONCopy. No pooled mutable SDK Event, Logger or writer escapes.

Retain/With capture one generation. Immutable derived facades share family Close
authority and never retarget destinations. Four families per physical source,
128 cumulative views and 8 MiB declared retained storage per family are admitted.
Direct validation stacks remain caller-owned; input length checks precede copying.
Native derivations separately share the original source's cumulative accounting.

Client.Slog creates one retained safe gateway; View.Slog permits one per family.
The shared [restricted ingress](../../slog/v1/interface.md) preserves chronology,
original time/PC and trace values. Canceled slog association contexts do not
suppress an attempt: explicit owner lifetime/timeout controls it. Direct Log keeps
caller cancellation. Exact slog levels -8/-4/0/4/8/12/16 map to the seven severities.
Unknown inputs/invalid derivations cannot emit a misleading subset. Handler.Status
retains bounded out-of-band refusal when slog.Logger hides Handle errors.
No unrestricted standard Handler or selected native-handler compatibility is claimed.

## Physical ownership and level-only updates

Prepared.Adopt admits only source/sink thresholds. Original dependency identities,
output order, caller, record/queue/timeout settings and every file option remain
fixed. Up to 16 live policies share ONE native Access, queue and physical output
state. Old filters remain frozen; new views cannot mint write/Sync/Rotate capacity,
remove a live marker or reset failure/rotation state.

One physical public root owns the native assembly; logical policies are bounded
children. Retiring one generation does not retain it merely because another
generation uses the same physical source. Last policy release performs actual
cleanup. Different directories use separately budgeted Open/Compose overlap;
same-path ownership/format/retention changes require explicit quiescence/reopen.

Result.Source identifies the physical source, PolicyRevision identifies the
frozen filter, and Attribution identifies the actual public borrow. NativeCorrelation
links exact local correlation.call and OTLP logging.call, not public WithID/Sequence.
These IDs are process-local/source-scoped, not globally unique or durable.

## Typed event recovery and evidence

SinkResult embeds shared Output name/kind and preserves zerolog-specific Filtered,
Attempted, Accepted, Written, BytesKnown, Synced, Rotated, Stopped, WriteError and
MaintenanceError. A failed event may have partial effects; flags are not backend
durability or exact SDK attempt counts. Later success never rewrites old evidence.

Legacy byte/record errors stop that destination. Owned files keep conservative
failure-stop and retained recovery markers. Only managed-record uses typed
RecordOutcome: accepted requires nil error; rejected fails this event but permits
a later independent legal record; stopped latches closed/damaged/unknown state.
Malformed/typed-nil acknowledgements and sink panics stop conservatively. A rejected
event is never automatically resent, silently acknowledged or treated as success.

Fan-out remains synchronous, ordered and non-atomic. Slow/uncooperative outputs
block later ones; cancellation cannot terminate entered code or filesystem work.
Close/Release/ShutdownComplete join actual work, preserve failed candidates and
cleanup continuation, and distinguish completed errors from pending ownership.
Runtime, evidence and borrowed outputs remain caller-owned.

Optional [OTel composition](../otel/v1/interface.md) uses stable telemetry targets,
separate Runtime allowances and independent receipts. Actual Using/Retain bindings
are checked before native output, not only at source construction. Stop producers
and logging families/owners before export/telemetry, then finish evidence reception.
Framework.Close and declaration order do not infer a dependency DAG.

## Evidence

See [capabilities](capabilities.md), [source/tests](../../../../../../adapters/logging/zerolog/v1)
and [Internal interface](../../../../internal/logging/zerolog/v1/interface.md).
Reservations are declared envelopes, not hard RSS/disk quotas or forced callback
termination. Local protocol/filesystem tests do not certify production durability,
a backend deployment or a supported release.
