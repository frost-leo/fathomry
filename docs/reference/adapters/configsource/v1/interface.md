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

# Raw configuration-source contract

[Documentation](../../../../README.md) / Public package reference

**Audience:** direct Adapter consumers and Framework configuration authors.
**Status:** implemented immutable raw capture and owned observation.
**Package:** `github.com/frost-leo/fathomry/adapters/configsource/v1`.

## Select, then explicitly capture or observe

Concrete [local](../viper/v1/interface.md) and [Nacos](../nacos/v1/interface.md)
`Select(Settings)` functions validate/copy bootstrap without I/O. Settings are
ordinary sensitive DTOs; `Selection` is a separate guarded runtime interface.
Its safe `Description` contains a module ID, non-secret source/slot aliases and
observation capability, never paths, endpoints, credentials or hashes.

`Capture(ctx)` acquires a fresh complete batch or returns no batch and an error.
It never substitutes an observer's old value. All finite transient ownership joins
before return; cancellation is cooperative and not a hard return-time guarantee.

`Observe(lifetime)` returns reachable ownership before asynchronous acquisition.
Success means owner-created, not ready. One Adapter loop owns acquisition,
registration/reconciliation and recovery. A failed acquisition stays pending for
retry even without another event. Framework must not perform a second capture loop.

Only the first-party implementations are qualified. These small interfaces are
consumer boundaries, not a generic provider registry or a security sandbox.

## Raw values and coherent state

`Batch.Documents()` returns owned ordered slot metadata. `RawCopy(slot)`
returns detached sensitive UTF-8 bytes and a `Presence`, or `ErrValue` for an
unknown slot. `Present` includes empty and whitespace content; `Missing` means
positive absence, not an unreadable, denied, malformed or empty document.
Acquisition does not promise that application parsing will succeed.
A nil batch is invalid; no usable prefix escapes on failure.

Limits per operation/observer: 1–16 slots, 1 MiB per decoded document, 4 MiB total
raw batch. Aggregate bounds are checked before retaining a new complete batch.
These are not process RSS or fleet quotas, nor limits on caller-retained copies.

`Observer.Current()` returns one coherent `State`:

- optional last complete `Batch`;
- `Generation`, advancing only for different ordered slot/presence/raw data;
- `Status`: Pending, Available, Degraded, Closing or Closed;
- latest bounded `Failure` and owner-bound `Cursor`.

Available is raw acquisition success, including Missing slots, not application
validity or proof of current remote truth. Failure retains the last complete batch.
Status-only recovery can advance Cursor without changing Generation.
Neither field is a preparation revision or native MD5. Batch completion is not a
transaction or common-time distributed snapshot.

## Waiting, cancellation and cleanup

`Next(waitContext, after)` returns the latest newer coalesced state, not event
history. A nil cursor requests current state immediately, including Pending/Closed.
Foreign or typed-nil cursor implementations reject. One Next caller is admitted
per owner; overlapping calls return `ErrBusy`. Current permits concurrent readers.
There is no Current-to-Next lost-wakeup window. After the terminal cursor,
Next returns `ErrClosed` instead of waiting forever.

Lifetime cancellation or `Close(cleanupContext)` begins Closing and fences new
accepted data. Cleanup/status transitions can continue. Closed means local owned
work has joined/accounted cleanup, not instantaneous remote unsubscription.
A timed-out Close retains the same owner. Its wait failure is not accumulated as a
permanent cleanup error; a later successful join can return nil. Actual cleanup
errors remain available. Returned batches remain usable after closure.
Terminal State preserves a bounded aggregate of the preceding operation/closing
failure and actual cleanup failure, rather than overwriting the reason startup or
acquisition failed. Close's return remains the actual cleanup result only.

Caller cancellation causes remain borrowed and deliberately inspectable. Native
acquisition receives cancellation/deadline signals and ordinary context values,
but not that arbitrary cause graph. Public error production appends the original
cause after classifying native evidence; it does not invoke the cause's
`Is`, `As`, `Unwrap`, `Error` or RPC-status hooks as an acquisition verdict.

Selections, batches, observers, states and cursors redact supported fmt/slog output
and refuse JSON reconstruction. Go still encodes typed nil pointers as `null`;
that does not reconstruct a usable runtime value. Arbitrary reflection/encoders,
explicit raw copies and deliberate cause inspection are outside safe diagnostics.
Logical aliases are caller-declared non-secret labels, not automatic sanitization.

`Module()` contributes this shared contract to
[Adapter-level catalogs](../../v1/interface.md); it constructs no source.
Conditions are usable without a catalog. See the concrete interfaces for deliberate
native/context cause exposure and their qualified service/filesystem profiles.

Acquisition refusals implement `AcquisitionFailure`: its direct
`failure.Occurrence` and `Acquisition()` facts describe the same event.
`AcquisitionInfo` contains the non-secret Source alias, an identified Document
slot (empty means unknown, not Missing), and a known public operation Phase.
Select/Capture/Observe/Close phases do not claim a native retry/effect verdict.
Concrete modules declare public ErrorFacts and bind required source/phase values;
optional document identity remains explicitly inspectable. Use a direct type
assertion on the occurrence for presentation, never recursive errors.As to borrow
another failure's facts, even when conditions are equal. Unknown native slots are
not inferred from diagnostic text or retained caller cause graphs.
