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

# Settings interface

[Documentation](../../../README.md) / Public package reference

**Audience:** project schema owners, configuration producers and component authors.
**Status:** implemented data storage/access contract. Application source loading,
Watch orchestration, localization and project generation are not supplied here.
**Package:** `github.com/frost-leo/fathomry/settings/v1`.

## Responsibilities

Settings holds project-owned typed configuration data independently of its origin.
It has no fixed complete application schema and does not load files, inspect
environment variables, decode a document, install SDK clients, own Watch loops,
register listeners or manage resource lifetimes. There is no application host,
service locator, automatic client rotation or implicit fallback configuration.

A producer owns source authorization, decoding, schema/semantic validation,
precedence and update ordering. After accepting a complete value it prepares and
publishes that value here. A component owns its subsection type, expected path,
validation/default rules and copy policy. Settings does not infer those meanings.

Production dependencies are the [failure core](../../failure/v1/interface.md) and
the standard library, never Internal, i18n, Framework or an SDK.

## Prepare, publish and capture

1. Define the project's complete Go type and its validation/copy functions.
   Validate the proposed value before publication.
2. `New(value, clone)` prepares a typed Snapshot by running the non-nil owner copy
   function once. Preparation does not certify semantic validity or source freshness.
3. `NewStore[T]()` creates an empty domain. `Publish(snapshot)` replaces its complete
   value with one atomic pointer swap, without invoking owner code.
4. Give consumers `store.Reader()`, not the Store write handle.
   `Reader.Capture()` returns one coherent View without copying data.
5. Use `As[T](view)` to recover the exact root Snapshot and `ValueCopy()` to copy it,
   or `Read[T](view, path, clone)` to copy only a selected subsection.

The [runnable examples](../../../../settings/v1/example_test.go) show old/current
views and exact type recovery. The [independent project fixture](../../../../settings/v1/testdata/consumer/consumer_test.go)
defines its own outer schema, private time.Time field, custom account settings,
strong validation and copy rules. Its separate
[component package](../../../../settings/v1/testdata/consumer/component/preferences.go)
knows only its own preference type, not the project's outer struct. That preference
fixture demonstrates data access, not an implemented i18n renderer or locale schema.

A failed admission does not publish anything. A producer must avoid Publish after
failed semantic validation; Settings cannot undo a producer publishing bad data.
Concurrent writers have atomic replacement order, not source-version order.
Producers must fence stale asynchronous loads before publishing them.

## Explicit process default, independent business domains

`Configure(reader)` can explicitly install an already-populated application's
Reader once. `Default()` captures the currently published snapshot from that same
cell. Later publications through the original Store are visible to new captures;
existing Views stay on their old snapshot.

There is no implicit installation, replacement, reset or initialization by reading
a file. `Configured()` reports reader binding, not application readiness, valid
credentials or completeness of every component's settings.

Zero/unpublished readers return ErrUnconfigured without claiming the default.
Admission happens first: even after installation, an invalid reader returns
ErrUnconfigured. A second populated-reader installation returns ErrConfigured,
including reinstallation of the same reader.

Ordinary business configuration creates its own Store/Reader and never replaces
the application default. Multiple applications/tenants and ordinary tests should
use explicit Readers. Process-default tests use fresh processes rather than a
production Reset escape hatch.

A component can internally call Default and select its own subsection, so unrelated
business functions need no extra locale parameter. This does not add automatic
localization to failure.Error. Before settings are configured, presentation code
must handle ErrUnconfigured without losing the original error or recursively asking
Settings to localize its own failure.

## Copying, retention and concurrency

No central Go-type whitelist or universal deep copier is imposed. Named types,
private fields, interfaces, nil values, arrays, time.Time, recursive models and
other owner-defined types can be stored. Source codecs may impose separate
constraints; acceptance here does not prove a particular SDK can decode that type.

Copy functions run synchronously on the caller's stack. They must be bounded,
concurrency-safe, non-panicking and non-mutating, isolating mutable data according
to the owner's contract. Immutable sharing is allowed; intentional borrowed
references need an explicit owner and lifetime. The callback and its captures
remain retained. They are data-copy policies, not hooks for reload/client startup.

New and ValueCopy use the project's copy function; Read uses the selected
component's function. Publish, Capture, As, Configure and Default do not invoke
cloners. Missing/invalid selections do not invoke them either.
Settings cannot prove a copier is correct: returning a map/pointer unchanged may
expose aliases. Tests preserve a deliberately shallow-copy counterexample.

Copied runtime handles share their underlying snapshot/cell. Publication and
capture are concurrent-safe; do not overwrite shared handle variables concurrently.
Do not mutate constructor inputs concurrently with copying. Reader lacks publication
authority, but it is not a security boundary against a malicious copy callback.

A Store retains only its current publication. A captured View or recovered Snapshot
keeps the older data and callback alive until callers release their references.
There is no version-history limit or automatic resource cleanup. Settings starts no
goroutines, timers or subscriptions and needs no Close.

## Select a typed subsection

Read copies only the selected value, not unrelated project data. Its path uses
[JSON Pointer string syntax](https://www.rfc-editor.org/rfc/rfc6901.html#section-3),
but traverses Go data rather than applying encoding/json serialization rules:

- Empty path selects the root. Slash separates tokens; `~0` means tilde and `~1`
  means slash. URI-fragment forms such as `#/application` are not accepted.
- Struct fields must be direct, exported and have an explicit nonempty `json`
  name. Private/untagged/`json:"-"` fields are not selectable. Anonymous fields
  are not promoted; explicitly tagged exported anonymous fields can be selected.
  Duplicate selected names return ErrPath. Other tag options do not change lookup.
- Maps need string-kind keys, including named string key types. Arrays/slices
  use zero-based unsigned decimal indexes without leading zeros, signs or `-`.
- Intermediate pointers/interfaces are transparent. A nil intermediate or absent
  key/field/index returns `(zero, false, nil)`.
- The final leaf must have exactly type T, including named types and tags.
  It is not coerced, dereferenced or unwrapped. Typed nil pointers/interfaces and
  zero-valued leaves are present. Private root fields remain available through an
  explicit full ValueCopy; selection tags are not a security policy.
- Custom methods and marshalers on the data are never invoked by navigation.
  Reuse one captured View when reading related sections that must agree.

Each selection is bounded to 4,096 path bytes, 64 tokens and 64 intermediate
dereferences. Pointer syntax and escapes are checked before lookup; contextual
array-index syntax is checked only when an array/slice is reached. Cyclic pointer/
interface traversal terminates at the bound. These limits do not bound the entire
project's schema, data size, retained native graph or work in its own copy function.

## Errors and implicit presentation

Errors use the shared numeric failure contract in configuration facility `0x041`:

| Error | Boundary |
| --- | --- |
| ErrSnapshot | Unprepared Snapshot/View |
| ErrCopy | Missing copy function |
| ErrType | Wrong exact type or unsupported traversal container |
| ErrPath | Malformed/oversized path, ambiguous selected tag or excess traversal |
| ErrStore | Zero/uninitialized write handle |
| ErrUnconfigured | No published reader/default value |
| ErrConfigured | Attempt to replace the installed application Reader |
| ErrSerialization | Runtime handle JSON persistence |

`Definitions()` returns detached declarations for explicit atlas composition.
Match errors with errors.Is and inspect them with failure.Inspect. Rejected paths,
types and configuration values are not echoed into safe diagnostics.

Snapshot/View/Store/Reader formatting and slog show only handle labels; they do not
format payloads or invoke copy functions. Their JSON hooks reject persistence and
leave existing handles unchanged. Standard codec behavior for nil pointers still
applies; a null pointer is not a reconstructed live handle. Explicit copied values
may contain secrets and use their owner's serialization/presentation rules.

## Executable evidence and limits

Run `go test -race ./settings/v1`. Grouped unit tests cover aliasing, arbitrary
owner types, missing/nil values, malformed paths, concurrent publication, old views,
privacy and failure definitions. Default tests isolate process state and race
installation/publication. The [independent module driver](../../../../settings/v1/integration_test.go)
checks actual consumer execution, dependency direction, exact generic types and
Reader's lack of publication authority. It creates its temporary module; the fixture
directory is not itself a standalone go.mod.

`BenchmarkRead` compares full-root and selected-section copies with the same
128 KiB unrelated payload and copy guarantees. `FuzzRead` exercises malformed
selection paths. These are storage/access checks, not real configuration-source,
Watch, locale-rendering, service-readiness or Temporal Workflow/replay qualification.
Do not consult mutable process settings directly from deterministic Workflow code.
