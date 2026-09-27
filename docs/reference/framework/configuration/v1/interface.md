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

# Typed configuration loading

[Documentation](../../../../README.md) / Public package reference

**Audience:** independent Go projects using explicit configuration sources.
**Status:** implemented Load/Watch with immutable snapshots; process-local, not a durable workflow format.
**Package:** `github.com/frost-leo/fathomry/framework/configuration/v1`.

## Developer declaration and explicit ownership

Define a concrete plain project struct T with exact JSON field tags.
`Settings[T]` combines Framework `Core` with that project value. Core owns:

- `framework.i18n.default_locale`: nonempty, at most 128 bytes, existing i18n
  request-locale syntax; unsupported translations follow i18n fallback.
- `framework.time.display_zone`: nonempty UTF-8, at most 128 bytes, with no
  controls, absolute paths, parent traversal or ambient Local. Platform availability
  is checked separately when binding Presentation.
- `framework.instance.name`: optional descriptive non-secret label, not runtime
  identity, uniqueness, an account or a lease.

`DefaultSettings(project)` supplies English/UTC and an empty instance name.
It is an ordinary shallow constructor; admission supplies isolation.
An explicitly empty Core value is not silently defaulted.

`Schema[T]{FormatVersion, Defaults, Validate}` declares the complete document
contract. The field is named FormatVersion to preserve the runtime wrapper's
fmt.Format guard. Admit shape and copied defaults before I/O, but validate required
semantics only after sources fill them. Validate receives an isolated final copy;
mutations are discarded. Core guards inspect the original candidate before that
callback, so repairing its copy cannot validate invalid published data.
The callback and captured closure state must be pure, deterministic and safe for
repeated/concurrent use.

Use keyed literals. T must be a nonrecursive plain struct; nested structs, pointers,
string-key maps, slices and supported scalar types follow the existing
[strict preparation contract](../../../internal/resource/configuration.md).
Embedded/private fields, tag options, interfaces, arrays, byte slices, custom
codecs, time.Time and runtime handles reject. Plain Adapter Settings can be fields.
Runtime Schema/Snapshot/State preserve exact generic identity, including rejection
of tag-only anonymous-struct value/pointer conversions. Identical aliases work.

## Plan and document format

Source bootstrap is separate from the configuration being fetched. An initial
literal file path or explicitly supplied startup inputs must be available before
connecting to Nacos; Nacos cannot supply its own initial credentials/address.
Adapter Settings are ordinary DTOs, so one local Load (optionally with explicit
environment bindings) can prepare a typed bootstrap containing nacos.Settings,
then Select that source for an independent application Load/Watch. The
[two-stage consumer](../../../../../framework/configuration/v1/testdata/consumer/local_test.go)
actually exercises this composition. No SDK/client factory is required.
Environment variables are a process input channel, not a distributed config store;
there is no automatic .env discovery/parser. A deployment/startup tool may inject
values from such a file, or mount a supported explicit YAML/JSON bootstrap file.
Each resulting source selection is frozen; changing a credential/address does not
automatically activate it or reconstruct clients.

`Plan.Modules` selects Adapter implementation declarations once. Inputs select
immutable deferred sources; every selected slot must bind exactly once to one
Base, Environment or Local layer. Source names and layer kinds are unique;
there are at most three total external slots. Optional means confirmed Missing
inherits defaults; it does not ignore empty, denied, malformed or unreadable input.
All source/module/watch-capability checks precede acquisition.

External documents are strict mappings, each with a required exact nonzero
integer `format` equal to Schema.FormatVersion:

```yaml
format: 1
framework:
  i18n:
    default_locale: en
  time:
    display_zone: UTC
  instance:
    name: worker-a
project:
  batch_size: 100
```

This scalar versions the complete core/project document, not independent migrations.
Every external layer is checked before merging; a correct upper header cannot
hide an incompatible lower layer. Original bytes enter the one resource preparation
engine. Defaults and generated variables are not files and do not manufacture
external headers.

Precedence is defaults < Base < Environment < Local < Variables, independent of
input ordering. Every layer is structurally checked. Objects/maps overlay
recursively, lists replace, absence inherits, zero/empty override, and null only
clears nullable types. Unknown/duplicate fields, unsupported YAML constructs,
inexact/out-of-range integer spellings and multiple documents reject.
Per-document/resolved-size (1 MiB) and depth-64 limits remain unchanged.

At most 64 Variables bind exact portable environment names to slash-delimited
struct-field paths, such as `/project/batch_size`. Paths cannot address format,
map keys or list indices; duplicate and ancestor/descendant targets reject.
Text is literal, including empty or "null", and targets strings/pointers to strings.
JSON supplies exact JSON tokens. Generated variables use strict JSON decoding,
not JSON-as-YAML: valid Unicode strings/map names and surrogate pairs retain their
values; unpaired surrogates and duplicate decoded names reject. Required checks
absence, not present-empty text.
Each selected name is captured once per admission, then frozen. A new Load/Watch
is the explicit recapture path; source changes do not reread environment.

## Load versus Watch

`Load(ctx, schema, plan)` performs finite Capture calls and one preparation, then
returns a complete Snapshot or no snapshot/error. It starts no observers.
Defaults-only Load is valid. Finite cleanup remains joined after request cancellation.

`Watch(lifetime, schema, plan)` requires at least one observable source. It creates
reachable ownership before asynchronous source startup/preparation. A successful
return is not readiness; an initial failure never fabricates valid zero settings.
Capture one Snapshot for each logical business operation instead of sampling fields
independently across updates.

```go
live, err := configuration.Watch(lifetime, schema, plan)
if err != nil {
    return err
}
// Keep live reachable until Close with a separate cleanup budget confirms joining.
state, err := live.Current()
for err == nil && state.Status != configuration.Ready {
    if state.Status == configuration.Closing || state.Status == configuration.Closed {
        break
    }
    state, err = live.Next(startupBudget, state.Cursor)
}
```

Next code must handle wait errors/degraded state and still close the owner; the
startup budget must not be mistaken for the whole lifetime.
[Independent consumers](../../../../../framework/configuration/v1/testdata/consumer/local_live_test.go)
exercise the complete ownership paths.

Current associates optional Snapshot, Status, Failure and Cursor under one guard.
Pending means initial/replacement preparation; an old snapshot can remain readable.
Ready, Degraded, Closing and Closed are separate states. Source raw Available
does not mean typed Ready. Rejected updates retain the last accepted value.

Framework consumes complete Adapter observations; it does not refetch or poll.
Each candidate starts from original defaults and the frozen complete input vector.
Ordered source/slot/presence/raw equality controls no-ops, not a secret hash or
random preparation revision. An unchanged pure rejection is not repeatedly
validated. Status-only recovery can reuse an accepted snapshot.

One bounded serial preparation worker allows the coordinator to incorporate new
observations during a slow validator. Its local publication guard rejects work
superseded by already-incorporated data/eligibility changes; diagnostic-only cursor
changes do not starve stable data. This is not an atomic all-source freshness check
or a distributed common-time snapshot.

## Waiting, close and explicit presentation

Zero Cursor returns current state immediately; foreign cursors reject. Only one
Next caller is admitted, while Current supports concurrent readers. Events coalesce.
Wait cancellation does not cancel ownership. After the final Closed cursor, Next
returns ErrClosed. Cursors use owner-local opaque positions, not ordered public numbers.

Close/lifetime cancellation begins Closing and fences new snapshot publication.
An arbitrary validator cannot be forcibly interrupted; Close can time out while
the same owner retains cleanup responsibility. A later wait can succeed after actual
joining. Past wait timeouts are not permanent cleanup errors; real cleanup errors
remain observable. Old snapshots and returned copies survive closure.
The terminal State retains the preceding operation/closing failure alongside an
actual cleanup failure in a bounded, inspectable occurrence; Close still reports
only actual cleanup, not prior wait failures.

`Snapshot.ValueCopy` deliberately exposes sensitive detached Core/project data.
`Description` returns safe owned format/revision/source-presence/schema-path metadata,
not endpoint/path/environment inventory or dynamic map keys.
`Snapshot.Presentation` separately resolves the platform-IANA zone after acceptance.
Its locale feeds existing i18n Resolve/Bindings.Resolve; FormatTime presents an
instant in the private bound zone. Nil/zero Presentation access rejects.
UTC uses a private fixed-zero-offset zone. TZ/time.Local are never mutated.
Platform ZONEINFO/tzdb/toolchain search remains deployment-owned, with no historical
cross-host reproducibility promise. Binding failure changes neither source nor
accepted configuration and starts no hidden retry loop.

Schema, Plan, input/binding wrappers, Snapshot, State, Cursor, Live and Presentation
have fmt/slog guards and reject JSON reconstruction. Plain Settings/Defaults and
explicit raw/typed copies remain sensitive DTOs. Go's typed-nil JSON `null` is
not a usable reconstructed handle. Caller validator/context causes deliberately
remain reachable through errors.Is/As; safe diagnostics never traverse/print them.

## Layer registration and boundaries

`configuration.Catalogs(modules...)` is only a convenience that contributes this
feature and shared-source vocabulary to
[Framework-layer registration](../../v1/interface.md). Static queries never need a
source or valid configuration. The reusable layer facility is not owned by this feature.

Configuration acceptance is not credential activation, business-client reconstruction,
service readiness, Run freezing or workflow execution. No CLI, configuration editor,
management backend, fleet registration or resource startup is introduced.
See [testing and profile limits](../../../../development/configuration-testing.md).
