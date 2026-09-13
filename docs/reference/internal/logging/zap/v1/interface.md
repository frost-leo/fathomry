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

# Zap logging interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework composition and capability maintainers.
**Status:** implemented bounded integration; no public logging API or exporter.
**Package:** `github.com/frost-leo/fathomry/internal/logging/zap/v1`.

## Responsibilities and supported behavior

This independently selectable package uses Zap `v1.28.0` native cores, checked
entries, typed fields and JSON encoders. It reuses `resource` for preparation,
ownership and admission, `invocation` for budgets and independent evidence,
`fault` for technical errors, and `compatibility` for effective-profile reporting.
It does not modify those mechanisms or choose Run/Item outcomes.

One logical event can reach up to eight destinations: explicitly selected
stdout/stderr, multiple exclusive Linux local directories, and one optional
`StructuredSink`. Each destination has an immutable minimum level. Local output
is JSON lines; files support size rotation, optional synchronous gzip and bounded
backup count. See [file output](file-output.md) for ownership, space and recovery.

There are no implicit outputs, global logger replacements, registrations,
buffer workers, sampling, event retries, runtime level mutations or native
logger/core escape. Zerolog, a shared public logging model, time-based rotation,
arbitrary native callbacks, OTel providers/exporters and Workflow logging are not
implemented here. Ordinary wall-clock and file I/O do not belong in Temporal
Workflow logic.

## Composition and call sequence

1. Call `Select(OptionsV1, StructuredSink, layers...)`; nil disables the extension.
   The strict version-1 preparation validates all selected configuration before
   construction. Structurally invalid, unknown or mistyped fields are rejected in
   every layer, even when overridden. Semantic validation checks the final
   effective settings, not each intermediate layer.
2. Attach `resource.WithLimits` before assembly. `LimitsV1` reflects bootstrap
   defaults; when overlays change queue settings, composition supplies the
   corresponding resolved policy. Bind requires exactly one active call, enough
   reservation bytes, and a queue no wider than the prepared ceiling.
3. Assemble with separate caller-owned initialization/cleanup contexts. Retain
   any returned assembly on failure: files already created are not rolled back.
4. Bind the exact selection with an independently owned
   `invocation.Inbox[Result]` and optional `Observer`.
5. Use `Log`, optional `With`/`Named` derivation, and explicit `Sync`. Receive,
   inspect and release every inbox delivery independently of handling the direct
   error/result.
6. Stop the borrowing scopes and close the owning assembly. A blocked operation
   leaves cleanup incomplete and owned; close again after actual operation
   termination. No goroutine is launched to pretend a timeout stopped I/O.

The [consuming executable](../../../../../../internal/logging/zap/v1/testdata/consumer/main.go)
demonstrates an in-module composition, native file output and structured context.
It is a maintainer fixture, not bootstrap work delegated to business projects.

Borrowed aliases preserve the original source identity, preparation revision,
queue and active allowance. Derived loggers freeze supported fields but acquire
no new owner or allowance. Independent sources have independent settings.
A shared extension must be concurrency-safe across all its sources, even though
each individual source serializes its own calls.

## Inputs, defaults and boundaries

Exact declarations and bootstrap defaults are documented in
[options.go](../../../../../../internal/logging/zap/v1/options.go).
Zero bootstrap values default only where documented; explicit zero/null in raw
overlays follows the existing [configuration contract](../../../resource/configuration.md).
Bootstrap output count and string sizes are bounded before configuration copying:
at most eight outputs, 64-byte output names, 4096-byte directories and 16-byte
kind/level spellings. Overlays cannot excuse oversized bootstrap storage.

| Dimension | Contract |
| --- | --- |
| Source admission | One active call; 0–64 queued calls, default zero/reject |
| Timeout | Default 5 seconds; 1 ms–1 minute, separately for admission and cooperative execution |
| Declared working reservation | 4 MiB per admitted/queued call; not an RSS or native-memory certification |
| Independent evidence | 16 KiB declared per accepted operation; inbox count/total bytes are composition-owned |
| Destinations | 1–8 including the extension; unique safe output names; no duplicate stdout/stderr |
| Message | 0–64 KiB, valid UTF-8; empty is a present empty message |
| User fields | At most 64 across bound and call-site fields |
| Field charge | At most 64 KiB: sum of key bytes, string/binary bytes and 128 bytes per field |
| Field keys/name | Keys: 1–128 ASCII letters/digits/`._-`; derived name at most 128 bytes |
| Native JSON | Default maximum 64 KiB per encoded entry; configurable 1 KiB–1 MiB, checked before the writer |
| Caller | Off by default; opt-in caller path/function information can disclose source layout |
| History | No logger-owned event history; receiver, error-graph and derived-facade retention remain caller-owned |

The input ceiling bounds encoding work even if JSON escaping exceeds the selected
encoded limit. Such an entry fails the affected local branch before writing.
The extension has the separate bounded typed-input contract, not the JSON byte
limit. Neither reservation accounts for arbitrary native cause graphs, extension
queues, caller-retained contexts or an unlimited number of derived loggers.
Composition must bound those separately.

Native string, boolean, signed/unsigned integer, finite float, duration, binary,
byte-string and time fields are supported. Time instants normalize to UTC.
Native no-op fields (including `zap.Error(nil)`) are ignored without retaining
their values; submitted field count is still bounded.
Binary storage is copied. Raw reflection, object/array/stringer marshalers,
namespaces, pointer fields, non-finite floats and arbitrary errors are rejected
without executing their callbacks. Error fields must contain a non-nil
`*fault.Error`; its safe kind is encoded locally while its original cause graph
remains deliberately inspectable by trusted code. No automatic secret detector
is implied: message and accepted values are explicitly authorized log content.

Duplicate keys across `With` and call-site fields are rejected. `ts`, `level`,
`logger`, `msg`, `caller` and the `fathomry.` prefix are reserved. Six technical
correlation/source fields are added; this does not define business Run/Item
identity. Context values are never encoded into local output.

Caller input must not be concurrently mutated during a call. Derived fields
own copied storage; caller-created derived facades need their own count/lifetime
budget. Result slices returned by `SinksCopy` are independent; native errors
retain their original owners' concurrency and lifetime contracts.

## Results, errors and cancellation

Before admission, errors return directly with no receipt or sink work. After
admission, errors belong to `Receipt.Result().Err()` and the same independent
inbox outcome. A nil immediate Go error is not logging success.

Each sink reports `Filtered`, `Written`, `Synced`, `Failed` or `NotAttempted`.
Local `Bytes` retains the writer's reported count; for a structured extension it
is zero/unknown. A failed write can have partial effects. Successful write/sync
reports only that native boundary, not storage durability, export, atomic
multi-sink delivery or business completion. A later successful Sync cannot
erase a previous write failure. Native error identity survives `errors.Is/As`;
ordinary formatting and runtime JSON guards do not expose native text.

Destinations are attempted in configured order, then the extension. A returning
failure does not suppress later destinations. A blocked destination delays the
others. Cancellation is checked before each destination; it cannot interrupt a
blocked syscall/callback, roll back an earlier write or release still-running
work. A native write that returns success after cancellation remains observed
success, while not-entered later destinations remain explicit.
When an extension returns cancellation, both the native error and the original
caller cancellation cause remain inspectable.

Attempt observations count intercepted native branch dispatch/sync entry, not
physical syscalls, maintenance work or remote requests. They are deliberately
inexact: the extension may have opaque retries. There is no automatic event retry.

Streams are borrowed and never closed. Explicit Sync preserves native errors
(for example Linux pipes return `EINVAL`); shutdown does not invent stream
durability by suppressing those errors. Shutdown synchronizes/closes owned files
and invokes the extension's Sync once, without taking its resource lifecycle.
Earlier cleanup failures remain visible through repeated assembly Close.

## Structured observability boundary

`StructuredSink` is a trusted, explicit composition dependency with only typed
`Write(ctx, zapcore.Entry, []zapcore.Field)` and `Sync(ctx)` operations. Operation
callers cannot replace it or obtain a native core. Each emission receives
isolated field storage, entry metadata and the caller's context values/cause
through the bounded execution context. It can preserve original trace/span
context and error identity without parsing JSON.

The extension owns any retained copies, provider, buffering, workers and native
error graph bounds; its dependencies must outlive this source. Methods must
return normally, not require progress from this same logger and propagate the
supplied context. Reentrant logging with that context fails before admission,
including across sources. Replacing the context defeats this contract.
An extension error must not expose owning handles or new callback authority.

The existing `Observer` remains a separate payload-free lossy diagnostic channel.
Its export suppression continues to work and it never replaces required evidence.
No exporter callback runs on that observer's behalf in SDK completion.

The official [otelzap bridge](https://github.com/open-telemetry/opentelemetry-go-contrib/blob/c4c6248ec2289133b6a51f554ca9367ece1de8e7/bridges/otelzap/core.go)
was exercised against this actual facade in an isolated recording-provider proof.
It preserved typed fields, scope, caller, original `*fault.Error` and trace/span
context beside local output. This is not exporter/backend acceptance. That probe
uses its own OTel build list; none of those bridge/provider dependencies were
added to the product by that historical proof.

The implemented [Fathomry OpenTelemetry bridge](../../../telemetry/otel/v1/zapbridge/interface.md)
now translates this actual StructuredSink contract through bounded telemetry
admission and independent evidence. It is not the official bridge above; its
Sync deliberately does not invoke telemetry Flush or take provider ownership.
Zap remains independent of that optional dependency and retains its local sinks.

## Versions and executable evidence

`OptionsV1` format 1, package SDK major `v1`, Zap `v1.28.0`, preparation revision,
framework commit and any future durable format are separate axes. There is no
durable logging DTO. `Profile` copies non-sensitive effective settings and never
discloses paths/payloads. Extension service/native facts remain unknown; supplying
a sink does not certify its behavior or establish an exact compatible build.

The product selects Zap `1.28.0` and multierr `1.10.0`. The existing `x/sys`
`0.47.0` supplies Linux descriptor/lock/no-replace operations. Lumberjack remains
an unrelated Nacos transitive dependency, not this file implementation.
Nacos and the other existing packages are included in repository regression tests.

[Tests](../../../../../../internal/logging/zap/v1/integration_test.go) exercise
independent reception, shutdown history, native consuming-binary dependencies and
deliberately broken controls. Adjacent field/options/logging/file tests cover
bounds, privacy, concurrency, cancellation, real file/compression failures and
clean restart. The Linux benchmark changes gzip only while retaining native
JSON, input, rotation size, retention, admission and evidence obligations.
It is a small local cost comparison, not capacity or production certification.

Rationale and preparation counterexamples belong to
[issue #30](https://github.com/frost-leo/fathomry/issues/30).
This package applies S01–S11 of the
[internal integration standard](../../../../../architecture/internal-sdk-integration.md)
at baseline `3baf854adc270609c2aef9f6f67debb5b0efea0a`. No shared logging
architecture, zerolog integration or additional platform is selected by this API.
