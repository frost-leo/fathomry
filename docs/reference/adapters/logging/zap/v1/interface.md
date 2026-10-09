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


# Public Zap logging

[Documentation](../../../../../README.md) / Public Adapter reference

**Audience:** independent Go applications and Framework resource compositions.
**Status:** implemented synchronous bounded Zap 1.28.0 profile.
**Package:** `github.com/frost-leo/fathomry/adapters/logging/zap/v1`.

## Configuration and call sequence

Use Settings format 1, Prepare, Policy (or Compose for overlapping physical
sources), caller-owned adapters.Runtime/Inbox, then Prepared.Open. There is no
implicit output. Local stdout/stderr/file outputs and the explicit structured
destination each have independent debug/info/warn/error minimums. OTel-only has
no file; local-only imports/constructs no concrete telemetry provider.
Dependencies are borrowed and excluded from strict-loadable Settings.

Nil optional pointers select native defaults. Explicit zero/false survives
preparation and validation; durations are nanoseconds. Native authoritative
preparation owns defaults: timeout 5 seconds (1 ms–1 minute), queue 0 (0–64),
encoded record 64 KiB (1 KiB–1 MiB), file 10 MiB and five backups (1–64).
All outputs together are at most eight. An output order/name/path, caller flag,
timeout, queue, encoding/rotation/retention or ownership change is not a level
change. Prepare is inert; metadata and construction share the same final frozen
selection. Settings JSON is explicitly inspectable; fmt/slog hide private paths.

Owner.Client and Owner.Handle are non-owning. Using accepts public Fixed/Follow
references and a frozen envelope covering every adopted generation. WithID is
immutable correlation only. Direct Log captures the actual business caller/time;
LogEntry preserves supplied original time/PC/name, including absent zero time/PC.
Log and Sync return separate public receipts and required independent evidence.
A receipt denotes admission, not success. Enabled is only an inexpensive hint.

## Fields and retained use

Native supported primitive tags, pointer-helper nulls, binary/byte-string, duration,
time, Skip and safe public error fields remain available. Terminal levels/actions
DPanic/Panic/Fatal, Sugar, raw Core/CheckedEntry, globals, arbitrary callbacks,
reflection and user encoders/sinks remain refused. An SDK primitive-array helper
does not grant arbitrary ArrayMarshaler authority: use Field with closed
logging.Array/Group/Null data for bounded collections.

Top-level fields: at most 64; message and logical field data: 64 KiB each;
closed data: at most 256 nodes and depth 8. Keys use ASCII letters/digits/._-,
1–128 bytes; duplicate keys per map and reserved top-level encoder/fathomry keys
refuse. NUL in valid local UTF-8 text and full uint64 range remain local-capable.
Float32 native formatting is preserved. Binary arrays use base64 like binary
scalar/map fields. Inputs are copied before native use; no raw error formatter
runs. Public failure/i18n errors use the shared safe projection.

Retain, With(ctx, ...) or Named(ctx, ...) capture one actual generation in a
bounded owned family. Derived With/Named facades share that family and its Close
authority; they never silently retarget a destination or create native allowance.
There are at most four live families per physical source, 128 cumulative views
per family and 8 MiB declared derived storage. Close every family (or give it a
bounded lifetime and still join cleanup). Native Internal derivations separately
have one shared physical-source cumulative reservation, not one per policy.

These derivation bounds charge accepted retained views, not arbitrarily many
caller-owned input-validation stacks. Length checks precede copying; closed
families refuse derivation and cannot gain another accepted reservation.

Client.Slog creates one dedicated retained family. View.Slog permits one gateway
per family; its additional 8 MiB/128-view reservation is separate. It has at most
16 concurrent preparation/operation children, preserved record time/PC/group
order, safe Framework ErrorLog presentation and bounded out-of-band refusal.
See the [restricted ingress contract](../../slog/v1/interface.md); do not claim a
full unrestricted slogtest compatibility pass.

## One physical owner, multiple frozen policies

Prepared.Adopt(ctx, previousHandle) admits level-only changes, retaining original
destinations and dependencies. At most 16 logical policy owners are live per
physical source. Each policy has its own frozen thresholds/revision; all share
one original native active allowance, queue, write/Sync ordering, rotation,
failure state and directory lock. Lowering a threshold cannot be blocked by the
original native Core's old threshold.

Old Owner.Close joins old work and ends only its logical policy. Physical files
remain open/locked while any adopted owner survives. The last logical release
performs real native cleanup and ends the physical source root. There is no
global directory registry or retirement dependency from new generation to old
generation's public resource ownership. A retained old generation still sees
its original policy; Result.Source identifies the physical source and
Result.PolicyRevision identifies logical filtering. Attribution identifies the
actual public Fixed/Follow borrow.

NativeCorrelation separately copies the actual native call/parent/owner identity,
linking evidence to local fathomry.call and typed OTLP logging.call. Public
WithID/Sequence are different namespaces. Correlation is process-local and
physical-source-scoped, not globally unique/durable or proof that a record was
written. Filtered/refused native calls can have it; source/policy/family-only
observations have zero native correlation. SinkResult embeds shared Output name
and physical kind while preserving Zap-specific states, bytes and errors.

Adopt rejects every non-level change before effects, preserving last good.
A different directory uses another Open and explicitly reserved overlap.
Same-directory ownership/format/rotation/retention changes require quiescence and
an explicit reopen; do not release a live lock or advertise arbitrary hot reload.

## Effects, failure and termination

Fan-out is synchronous, ordered and non-atomic. A slow earlier output blocks later
outputs. Cancellation is checked before each sink but cannot interrupt an entered
uncooperative callback/file syscall. Results preserve each sink's filtered,
not-attempted, written/synced/failed state and writer-reported bytes. Local success
does not hide a structured rejection; a later successful Sync cannot rewrite
earlier failed receipts. Files retain conservative failure-stop/rotation history.

Owner.Close refuses new work and joins existing work/families before last-source
cleanup. Release.Complete and ShutdownComplete report actual local release;
timeout is not release. Every non-nil failed-construction owner remains reachable
for cleanup. Runtime/Inbox/structured destinations are always caller-owned.

Policy reserves physical residence, live logical views, finite work, retained
family/child work and independent evidence separately. It never enlarges a
supplied Runtime. Compose covers overlapping physical sources; level-only Adopt
uses the original physical reservation. These are declared envelopes, not hard
RSS/disk quotas or authority over caller payloads and uncooperative sinks.

The optional [OTel composition](../otel/v1/interface.md) borrows telemetry on a
different explicitly budgeted Runtime and retains separate evidence. Stop/join
logging producers first, then export scheduling and telemetry, then evidence
receivers. Framework.Close is not a dependency DAG.

## Evidence and native contract

See [source and tests](../../../../../../adapters/logging/zap/v1),
[Internal interface](../../../../internal/logging/zap/v1/interface.md),
[file profile](../../../../internal/logging/zap/v1/file-output.md) and the
[capability matrix](capabilities.md). No backend durability, release availability or
production qualification is implied by local native/protocol tests.
