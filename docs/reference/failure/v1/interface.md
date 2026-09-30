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

# Numeric public failure contract

[Documentation](../../../README.md) / Public package reference

**Audience:** component authors and callers of public Adapter/Framework capabilities.
**Status:** implemented public error foundation. Shared settings and i18n are also
available; public Adapters/Framework and CLI code lookup are not implemented yet.
**Package:** `github.com/frost-leo/fathomry/failure/v1`.

## Identity and definition ownership

`Code` is an explicitly allocated uint32 identity. `Identifier` is a
separate readable symbol. Neither is a localized message or a native SDK status.
The [code allocation contract](code-allocation.md) defines the customer-failure
flags, 11-bit subsystem Facility, 16-bit local Number and capability-domain bands.
Database, cache, object storage and other domains are independent of public code
layers. `Code.Domain` decodes the capability using the explicit range manifest;
extensions follow mirrored capability bands rather than a single third-party category.
Published codes must never be reassigned to another meaning. First-party ownership
and catalog facility conflicts are checked; this is not namespace authentication.

`Code.String` and text/JSON encoding use eight uppercase hexadecimal digits with
a `0x` prefix. JSON numbers and null are refused to keep one explicit representation.
`ParseCode` also accepts unsigned decimal query spellings, without signs,
whitespace, separators or implicit octal. Invalid flags, fields, overflow and
legacy 64-bit spellings reject without narrowing. The native
encoding/json parser can reject malformed JSON before calling a Code hook.

Each component supplies `Definition` data: Code, Identifier, Module, Component,
Revision, Message, Description and an optional Details contract reference.
Identifier is exactly `Module.Component.reason`. These namespace components are
lowercase ASCII names with letters, digits, underscores and hyphens; each name
starts with a letter. Metadata is code-owned, non-sensitive data.

Message is a static developer explanation, not native text or an interpolation
template. Description adds definition-level meaning without requiring a runtime
occurrence. An [i18n binding](../../i18n/v1/interface.md) can explain the same numeric code in another
language without changing the error identity. Definition Revision and detail
Contract Version are semantic axes independent of Go-package and SDK versions.

## Construct, compose and inspect

`New` constructs the shared core from a definition, an optional Location and
deliberately retained native/public causes. It does not require a catalog, settings,
a formatter or a global registry. It also accepts definitions with a Details
reference so a component can build its own error representation.

`NewDetailed[T]` is the convenience composition:

```go
occurrence, err := failure.NewDetailed(
    definition, location, componentData, cloneComponentData, nativeCause,
)
```

T is not constrained to a reflected schema, public fields or plain scalar types.
Components may use time.Time, interfaces, private fields, recursive models or
non-struct types. The component supplies its typed copy function once beside its
data contract. It runs at construction and on explicit `Details` reads.

The copy function must not mutate its input. It owns isolation, immutable sharing,
borrowed-reference lifetimes, bounds and concurrency; it must be synchronous and
non-panicking. The kernel cannot verify arbitrary copying logic and does not hide
callbacks in goroutines or impose a universal detail-size/type policy. Reference
sharing must be deliberate, not an implicit shallow-copy default. Captured
dependencies remain retained as long as the occurrence retains the function.
Formatting, logging, atlas lookup and runtime serialization guards never call it.

A component may instead implement `Occurrence` on its own type and compose a
`*Error` core. It owns its additional accessors, formatting, copying and lifetime.
The independent consumer exercises this without using `Detailed`. The generic
helper exists to avoid repeating forwarding methods, not to prohibit custom types.

`Inspect` selects only the directly supplied occurrence and returns its exact core.
It never searches through a wrapper or join for a preferred error. Take identity
and details from the same selected occurrence. A Details contract reference is a
declaration, not proof that any arbitrary error value implements that accessor.

`errors.Is` matches a public Code and also traverses intentionally exposed causes.
`errors.As` retains access to exact native objects. Those standard traversals can
find descendants and do not select a primary semantic occurrence. A caught error
does not erase native effect evidence or authorize retries.

## Location, diagnostics and native evidence

Definition identifies the owning module/component. Location supplies an optional
public operation and non-secret logical instance. Empty labels mean unspecified.
Operation uses bounded lowercase qualified names; Instance permits bounded ASCII
letters, digits, dots, underscores and hyphens. Neither is discovered from a stack,
URL, database name or a native error string.

`Error` owns cloned definition/location strings and cause-slot storage. Native
error objects are borrowed unchanged, not copied or normalized. `Unwrap` returns
fresh slots with the original objects, including deliberately supplied typed nils
and duplicate causes. Their mutation, liveness and traversal follow their owners'
Go contracts. Even one cause uses `Unwrap() []error`, supported by errors.Is/As but
not by errors.Unwrap.

`Error()` includes numeric code, Identifier and the static developer explanation.
Supported fmt/slog diagnostics do not print detail values or call native Error
methods. Structured logging reports those identities, location and direct cause
count. Raw cause inspection and explicit Details access remain sensitive.

Runtime Error/Detailed values refuse JSON encoding and reconstruction; nil pointers
may still encode as null under encoding/json's ordinary absent-pointer rule.
Scalar metadata and catalog definitions are inspectable data, not an implicit
durable error bridge. Arbitrary foreign wrappers, codecs, malformed fmt directives,
reflection and unsafe access are outside the package's diagnostic contract.

## Offline definition atlas

`Prepare` atomically admits an explicit set without opening sources or creating
clients. Duplicate codes and duplicate Identifiers reject, including identical
declarations. There is no override order and no package-init registration.

- `Lookup(code)` retrieves the exact numeric definition.
- `LookupIdentifier(identifier)` retrieves the same definition by symbol.
- `Inspect` returns every definition in numeric order.
- `Components` lists declared capability domains, module/component owners, facilities and codes.
- `InComponent` returns one exact owner's definitions.

Each facility identifies one owner and each owner uses one facility in the selected
catalog. Conflicting extension allocations reject even with disjoint local numbers.
Unknown valid keys report absence separately from invalid input. Query containers
are caller-owned; editing them cannot change a catalog. Prepared catalogs support
concurrent reads. Component lookup concerns declarations, not deployed resource
instances. `Definitions()` supplies failure's own seven numeric admission errors
for explicit composition; built-ins are not silently added to every catalog.

## Admission and bounds

| Boundary | Limit |
| --- | --- |
| Identifier | 192 ASCII bytes |
| Definition module / component | 128 / 64 bytes |
| Reason token | 64 bytes |
| Static developer message | 512 UTF-8 bytes, one line |
| Definition description | 4096 UTF-8 bytes; newline/tab allowed |
| Operation / instance | 64 / 128 bytes |
| Direct supplied cause slots | 32, including literal nil slots before filtering |
| Definitions | 4096 |
| Catalog metadata charge | 4 MiB; strings plus 64 bytes per definition |

These bounds do not limit arbitrary reachable native error graphs or component
details. Components own their data and copy-work limits. Invalid inputs reject
without publishing a partial occurrence/catalog; the caller retains rejected inputs.
Admission failures themselves have numeric Codes, definitions and safe occurrences.

Zero Error/Detailed/Catalog handles are not valid initialized objects. A deliberately
prepared empty catalog is valid. Do not overwrite shared handles or mutate borrowed
constructor inputs concurrently. Typed-detail generic identity cannot be changed
by a tag-only ordinary Go conversion; genuine Go aliases retain their identity.

## Verification and remaining boundaries

[Examples](../../../../failure/v1/example_test.go) and the
[independent module](../../../../failure/v1/testdata/consumer/consumer_test.go)
exercise numeric/symbolic queries, custom and generic composition, copying,
runtime guards and native cause identity without any Internal import.

The [native boundary fixture](../../../../internal/conformance/failure_v1_test.go)
uses the actual private Viper parser and preserves its fault and JSON syntax error
through two public semantic layers. It does not implement a public Adapter.

Tests cover the 32-bit layout, allocations, collisions, invalid metadata, concurrent reads,
time.Time/private/interface/cyclic data, a deliberately broken shallow-copy
control and same-occurrence attribution. [Settings](../../settings/v1/interface.md)
provides data storage/access and [i18n](../../i18n/v1/interface.md) separately owns
localized presentation. Other public layers remain withdrawn.
No automatic localization in Error(), CLI lookup command, SDK error mapping, retry policy,
HTTP/RPC protocol or Temporal failure/replay bridge is supplied by this module.
