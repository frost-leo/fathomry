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

# Zerolog interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework composition and integration maintainers.
**Status:** implemented synchronous internal JSON profile; no public logger or
production telemetry/export certification.
**Package:** `github.com/frost-leo/fathomry/internal/logging/zerolog/v1`.

## Contents

- [Responsibilities](#responsibilities)
- [Capabilities and call sequence](#capabilities-and-call-sequence)
- [Structured records and context](#structured-records-and-context)
- [Results, failures and ownership](#results-failures-and-ownership)
- [Resource and compatibility limits](#resource-and-compatibility-limits)
- [Executable evidence and provenance](#executable-evidence-and-provenance)

## Responsibilities

This independently selectable integration uses zerolog **v1.35.1**. It imports
neither Zap nor a companion rotation engine and introduces no grouping-level
logging interface. SDK major, `OptionsV1` format 1, preparation revision and actual
consuming-build version are separate axes.

It supports explicit byte sinks, structured-record sinks and owned local files;
multiple sinks receive one logical event. [File output](file-output.md) owns the
rotation, compression, retention, recovery and filesystem contract. No CLI,
application logger selection, exporter/backend, durable execution ledger or
Temporal Workflow command/serialization is implemented here.

## Capabilities and call sequence

1. `Select(OptionsV1, layers...)` validates/freezes source settings through
   `resource.Prepare`. It performs no file I/O and discovers no environment,
   default stdout/stderr, config files or log directories.
2. Composition adds `resource.WithLimits` and calls `resource.Assemble`.
   Construction opens only selected owned files. A failed assembly cannot bind;
   retain its returned cleanup responsibility and report.
3. Create an independent `invocation.Inbox[Result]`, then call `Bind`.
   The bound `Logger` exposes `Log`, `Sync`, `Rotate` and a safe `Profile`,
   not the native logger, owning handles, writer replacement or mutable hooks.
4. Call with a caller-owned context and explicit `fault.Correlation`.
   Inspect both setup error and the accepted receipt's `Result.Err()`.
   The inbox receives that same result independently, even if the caller ignores it.
5. The framework handles the inbox evidence before releasing its delivery slots.
   Close the owning/borrowing assembly, not the logger. Closing a borrowed scope
   only returns that scope's responsibility. Already established borrowing scopes
   may finish after the original owner's shutdown starts.

The [consuming executable](../../../../../../internal/logging/zerolog/v1/testdata/consumer/main.go)
runs this complete local sequence, decodes both gzip/active output and a second
sink, and inspects actual build facts. It is an internal maintainer fixture, not
business-owned startup boilerplate or a public API example.

### Selected options

Bootstrap zero values select the documented defaults. Explicit overlays retain
the shared [configuration rules](../../../resource/configuration.md): structural
validation of every layer, fixed precedence, recursive object merging, list
replacement and rejection of unknown fields. Zero/null after overlay is not an
instruction to reapply defaults.

Before hashing runtime sink names or serializing defaults, `Select` bounds the
aggregate supplied settings-string bytes to 1 MiB. This is a necessary byte check,
not early semantic validation: an invalid default that an authorized overlay
repairs is still allowed. Shared preparation independently enforces the 1 MiB
encoded-default/document limits, including JSON expansion.

| Setting | Default and supported range |
| --- | --- |
| `Name` | Required non-secret source identity; shared resource label rules |
| `MinLevel` | `Info`; `Trace`, `Debug`, `Info`, `Warn`, `Error`, `Fatal`, `Panic` |
| `MaxRecordBytes` | 16 KiB; 1 KiB–1 MiB |
| `Timeout` | 5 s; 1 ms–1 min; overlays use `timeout_ns` |
| `QueuedCalls` | 0, immediate overload refusal; 0–64 |
| `Sinks` | 1–8 unique non-secret names; each selects exactly one output |
| Sink `MinLevel` | `Trace`; source and sink thresholds both apply |

`Writer` is an explicit borrowed `io.Writer`, including caller-selected stdout
or stderr. `Records` is an explicit borrowed `RecordWriter`. `File` selects an
owned [file sink](file-output.md). No runtime handle enters the prepared schema.
Overlays may select/configure files, but cannot invent, rename or change the kind
of a runtime-bound sink. Replacing the sink list requires complete entries;
unselected handles never run.

`LimitsV1` derives recommended admission from bootstrap options, **before
overlays**. A changed effective bound needs corresponding limits. `Bind`
requires exactly one active root, sufficient bytes and a queue no larger than the
effective setting; aliases cannot reset the original allowance.
`EvidenceBytesV1` supplies the per-result declared inbox reservation.

## Structured records and context

`Log` accepts a closed `slog.Attr` vocabulary rather than arbitrary SDK events:

- UTF-8 string, bool, signed/unsigned 64-bit integer and finite float64;
- duration as integer nanoseconds; time as UTC RFC3339Nano, years 1–9999;
- null through `slog.Any(key, nil)`;
- canonical nested slog groups and top-level empty groups. Dotted keys remain
  literal keys.

Native slog constructors remove empty child groups; their group values are
immutable. A noncanonical empty child inserted by changing native group storage
is explicitly refused with `ErrUnsupported` before admission, not silently
discarded during copying. Top-level empty groups remain `{}` in the output.
[Native group contract](https://github.com/golang/go/blob/go1.26.4/src/log/slog/value.go#L177-L195).

There are at most **64 attributes including groups**, **8 levels**, and **128
UTF-8 bytes per nonempty key**. Duplicate keys in the same group are rejected.
Arrays, maps, byte slices, errors, `LogValuer`, arbitrary `Any` values,
marshalers, raw JSON and native callbacks are refused before invocation. No
caller formatter runs during validation/encoding.

Input accounting charges message/string-value/key bytes plus 32 bytes per
attribute. Both that total and the **complete encoded record including newline
and fixed metadata** must fit `MaxRecordBytes`. JSON escaping can therefore
cause an accepted invocation to report an encoding-limit failure before any sink
is attempted. Oversized message/string values are rejected by byte length before
UTF-8 scanning, including when an oversized value is also malformed. Data is
never truncated to fit.

JSON has fixed `time`, `level`, `message`, `resource`, `correlation` and
`attributes` fields. The resource object contains provider, original scope,
source and preparation revision, never file paths/settings. Correlation preserves
call, parent and opaque owner; it is not authentication or inferred Run/Item state.
User fields remain inside `attributes` and cannot override these fixed fields.

Arguments are borrowed without concurrent mutation until `Log` returns.
Structured sinks receive an immutable `Record` with copied attributes and
read-only shared backing. `AttributesCopy` returns independent attribute/slice
storage, but native slog group values retain their immutable contract. Replace
attributes using native constructors when editing a copy; do not modify a
group's backing slice. Byte and source-metadata copies remain independently mutable.
Time values lose their monotonic component and normalize to UTC. Formatting and
runtime JSON guards are diagnostic redaction, not the explicit `JSONCopy` data
inspection API.

The operation context, including caller values, reaches `RecordWriter`; it is
not stored in the record or result. An adapter can inspect authorized trace
association and all typed attributes without reconstructing a pooled native
event. That adapter's export, buffering, retries and retained-record budgets remain
explicit separate responsibilities. The implemented
[OpenTelemetry RecordWriter bridge](../../../telemetry/otel/v1/zerologbridge/interface.md)
uses this boundary without changing borrowed ownership or local outputs.
Standard/native `slog.Handler` compatibility is not supplied.

## Results, failures and ownership

Each selected sink has separate copied `SinkResult` evidence:

- `Filtered`: selected thresholds excluded this event, not attempted delivery.
- `Attempted`: actual byte/structured write entry. `Accepted` requires full
  bytes with nil error, or a nil structured-sink acknowledgement.
- `Written` is a byte writer's reported count. `BytesKnown` distinguishes a
  valid count from absent or malformed evidence; neither proves durability.
- `Rotated` and `Synced` confirm successful local maintenance only.
- `WriteError` and `MaintenanceError` retain original causes separately.

A failed invocation may have known accepted sinks and untouched or uncertain
others. An error does not short-circuit later eligible sinks, but context expiry
prevents their later entry. Failed outputs stop accepting new events until the
source is explicitly reconstructed; there is no automatic retry or stream repair.
A maintenance failure can leave archive effects even when `Rotated` is false.

Shared `invocation.Attempts` remains **unobserved** (`Observed=0, Exact=false`)
for `Log`, `Sync` and `Rotate`. This integration does not instrument a shared
SDK-attempt count or impose a native-attempt maximum. A borrowed sink may queue
without executing an SDK, or synchronously retry inside one call; counting its
entry would establish neither an exact count nor necessarily a lower bound.
Per-sink `Attempted`, `Accepted`, `Written`, `Synced` and `Rotated` retain the
separate known output facts. Encoding, wrapper entries and filesystem syscalls
are not substituted for the shared SDK-attempt contract. Borrowed sink owners
must bound their own native retries and resource use.

Shared `fault.Error` presentation excludes record data and native text while
preserving `errors.Is/As`. Deliberate native-cause inspection is not redacted.
An injected sink panic is contained without retaining its arbitrary panic value;
the attempted effect remains unconfirmed, later sinks continue, and synchronous
ownership/evidence completes. Sinks must not terminate the goroutine/process or
hide resource use in workers. They must bound errors and any retained records.

Borrowed handles are never flushed or closed, including when they implement
`io.Closer`. Their owners arrange dependency order, synchronization outside this
source, flush and release. `Sync` and `Rotate` operate only on owned files and
refuse a source with no file outputs. There is no multi-sink atomicity or rollback.

## Resource and compatibility limits

One admitted root serializes output; a slow sink couples later sinks. Admission
waits have bounded count/declared bytes. There is no worker, retry, timer,
asynchronous log queue or lossy diode writer. Context deadlines cannot interrupt
an uncooperative byte writer or an in-flight kernel file operation. Such work
retains its lease and evidence reservation until it actually returns; assembly
close reports incomplete rather than releasing early.

If the final/only writer returns a full successful acknowledgement after the
context was canceled, that acceptance remains recorded and is not retroactively
changed into failure. Cancellation before later sink entry is recorded separately;
it does not undo earlier effects.

Per-root declared accounting is `2 MiB + 24 * MaxRecordBytes`, including a
sequential gzip workspace and bounded encoding/copies. Per-result accounting is
64 KiB. Neither bounds RSS, Go's allocator/GC, zerolog's process-global event pool,
arbitrary native cause graphs, or caller-retained record copies. Optional Observer
loss cannot replace or block the independent inbox. Required evidence is not a
durable execution ledger.

No zerolog global is mutated. Explicit field encoding avoids global field names,
formatters, error callbacks and severity actions. Native global `Disabled`
(or a higher custom override) still prevents native event creation: construction
or an enabled `Log` reports `ErrUnsupported`, never false delivery. The
`binary_log` build is likewise rejected before opening files. Filters and
maintenance do not claim an emitted event.

`Profile` copies safe effective settings, omitting sink names, paths and private
content. It does not characterize arbitrary injected sink implementations,
deployed filesystems or exporters. Actual binary inspection and
`compatibility.Assess` remain separate; missing records/unknown facts do not
become support. There are no power-loss, distributed quota, cross-process shared
file writing, production export, broad performance or multi-platform guarantees.

## Executable evidence and provenance

- [Record tests](../../../../../../internal/logging/zerolog/v1/record_test.go):
  bounded types/escaping, hostile callbacks, globals, fuzzing.
- [Logging tests](../../../../../../internal/logging/zerolog/v1/logging_test.go):
  multi-sink attribution, immutable context, saturation, cancellation, aliases,
  concurrent complete records and independent evidence.
- [File tests](../../../../../../internal/logging/zerolog/v1/file_test.go):
  independently decoded output, constant-timestamp collisions, fault containment,
  exclusive ownership, recovery refusal and cleanup.
- [Integration tests](../../../../../../internal/logging/zerolog/v1/integration_test.go):
  restricted facade, actual consuming binary/version/imports, JSON-build refusal
  and compatibility evidence boundaries.

This applies the existing integration standard S01–S11 at
`3baf854adc270609c2aef9f6f67debb5b0efea0a`; it changes no shared mechanism or
Workflow contract. [Issue #31](https://github.com/frost-leo/fathomry/issues/31)
records the scope and native counterexamples, not an acceptance certificate.
Pinned native behavior: [zerolog logger](https://github.com/rs/zerolog/blob/116c8060e034e8d46855354d22db2acbc8df9e1e/log.go),
[event finalization](https://github.com/rs/zerolog/blob/116c8060e034e8d46855354d22db2acbc8df9e1e/event.go).
