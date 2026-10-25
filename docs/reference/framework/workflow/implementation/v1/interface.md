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

# Workflow implementation binding

[Documentation](../../../../../README.md) / Public package reference

**Audience:** workflow authors, offline project tooling and future host integrators.
**Status:** static binding admission and inspection are implemented. No Activity
executor, Worker, runtime value validation or deployment authority is provided.
**Package:** `github.com/frost-leo/fathomry/framework/workflow/implementation/v1`.

## Responsibilities

Associate each declared binding key with an explicit Go function in one exact
Namespace / workflow code / business version. A node / Activity call site references
that binding; multiple sites can reuse one function registration. Business version,
declaration profile/fingerprint, ABI profile and executable digest are different
identities. Function pointers are not semantic hashes.

The declaration package remains the authority for graph/schema admission and
continues to reject unused bindings. Pure declaration consumers need not import
this package. This boundary depends on declaration, failure, i18n and type/codec
marker facilities, not root Internal, command engines or the Temporal runtime.

## Call sequence

1. Compile or parse a verified [declaration Bundle](../../declaration/v1/interface.md).
2. Create an ordered `[]Binding`: each key and `Activity(fn)` are explicit.
   The helper is inert and preserves typed-nil functions until preparation.
3. Call `Prepare(ctx, bundle, identity, bindings)` for an exact specification
   identity, without Mode or Revision. Missing, extra, duplicate or nil bindings
   refuse; there is no partial Registry on error.
4. Use `registry.ManifestCopy()` for detached metadata. `Lookup(key)` returns
   the original borrowed function, not an invocation wrapper or resource owner.
5. At a data boundary, decode `Manifest` and call `ValidateManifest` against
   the verified Bundle. Decoding alone establishes neither coverage nor callables.

`Describe` reports required unique bindings and call sites without claiming ABI
support. It keeps declaration-only consumers usable even for schemas outside
this ABI. A complete Manifest has deterministic binding/call order, the exact
specification Entry and the identified format/profile:
`fathomry.workflow-implementations/v1` and
`fathomry.required-json-values/v1`. It contains no function addresses, captures,
Go type names or fabricated per-function hashes.

For project integration, put optional `VersionBindings` on the one
`cli.Workflow` registration. See the
[project selection and inspection contract](../../../../cmd/fathomry/v1/interface.md#project-workflow-checks).
A missing version set is not supplied; a present empty set is a claim to check,
not absence. Known omitted versions are not checked. Modes do not duplicate
binding obligations. A full build checks every supplied claim.

## Initial ABI and presence

`Activity` accepts only functions or bound method values of this Go shape:

```go
func(context.Context, Input) (Output, error)
```

The compiler enforces the signature. Preparation checks the remaining shape and
nil-function rules. A method value can retain a nil receiver; its behavior/state
cannot be certified by reflection.

Input and Output must be non-pointer structs. Their fields bijectively match
named ports using explicit unique JSON tags, exported non-embedded fields and no
tag options. No ignored, extra or missing fields are admitted. Empty ports use an
empty struct. Apply the same field rules recursively to object properties.

| Required value-port schema | Go representation |
| --- | --- |
| String, including bounded enum | `string` or hook-free defined string |
| Boolean | `bool` or hook-free defined bool |
| Bounded signed integer | `int64` or hook-free defined int64 |
| Closed object, every property required | Explicitly tagged struct |
| Array | Slice of matched elements, excluding byte payloads |
| Declaration's nullable wrapper | One pointer to the matched non-null representation |

Optional ports/properties, `complete_batch`, standalone null, recursive Go types,
maps, interfaces, data functions/channels, unsafe pointers, floats, unsigned or
machine-width integers and arrays are unsupported. These refusals do not change
which static declarations are otherwise valid.

Known JSON v1/v2 and Text encoder/decoder hooks, including TextAppender, are
rejected on both T and *T. Modern and legacy/gogo protobuf markers, `json.Number`
and byte payloads also refuse. No hook, ProtoReflect method, user serializer, factory,
handler, Boot callback or service runs during matching.

Shapes are not values: ranges, enums, lengths, actual non-null values, runtime
presence and effects still need future validation. Nil slices, pointers and actual
outputs are not inspected. This profile does not qualify an arbitrary configured
Temporal DataConverter, history compatibility, retries or business behavior.

## Ownership, concurrency and bounds

Prepare borrows authored slices/maps only until return; do not mutate them
concurrently. Registry owns copied registration slots and immutable metadata.
Manifest copies are independent. Captures and receivers remain deliberately
borrowed behavioral state with caller-owned lifetime, cleanup and concurrency
rules. No capture graph is walked or deep-copied. Concurrent Registry metadata
reads are supported; do not overwrite shared handles.

Runtime handles hide contents during ordinary formatting/logging and reject JSON
serialization/deserialization. Explicit Manifest data includes logical identities
which can themselves be sensitive. Neither its parser nor validator can recover
callable-bearing authority.

Existing declaration bounds still apply. Additional limits are:

| Boundary | Maximum |
| --- | --- |
| Bindings and call sites per Manifest | 4,096 each |
| Encoded Manifest | 16 MiB |
| Matching depth | 32 |
| Type/schema traversal visits per preparation/validation | 1,048,576 |
| Methods on each inspected T or *T | 256 |

Text fields retain declaration identity/key bounds. Both JSON versions route
Manifest decoding through bounded strict admission: unknown/duplicate members,
null/missing collections and oversized input refuse. Decoding is structural;
`ValidateManifest` additionally checks the supported profile and exact Bundle
projection. Limits cannot become truncated success. They bound finite type/data
inspection, not arbitrary closure memory or process RSS.

Errors originating inside the Manifest decoding hooks use safe component messages
and retain native causes through `errors.Is`/`As`; limit and handle failures keep
their distinct codes. Standard-library parsing errors before a hook is entered
remain outside this boundary.

The caller owns context cancellation; no goroutine, connection or background
lifetime is created. Preparation/copy/output can still fail. Nil contexts and
invalid Bundle/Registry handles refuse. Context causes remain discoverable through
`errors.Is`.

## Failures and executable evidence

Facility `0x1C3`, component `workflow_implementation`, distinguishes
`ErrInvalid`, `ErrUnsupported`, `ErrLimit`, `ErrCanceled` and `ErrHandle`.
`Diagnose` checks the core/detail code and component identity before selecting a
direct completed validation failure; it does not traverse wrappers or joined
causes. Binding indices refer to the supplied slice, not sorted manifest order.
This consistency check does not authenticate caller-constructed diagnostic text
or choose retries.

The [hierarchical consumer](../../../../../../framework/workflow/implementation/v1/testdata/consumer/README.md)
uses one Activity per file and explicit registration beside each function.
Its independent-module test performs project vet and two real official builds.
Changing a pure callable from 42 to 43 changes executable bytes while static
inspection stays identical. Invalid supplied claims preserve the old output,
including under explicit legacy inspection. These are offline/compiler checks,
not live Temporal execution evidence.

```sh
go test -race ./framework/workflow/implementation/v1 -count=1
go test -race ./cmd/fathomry/internal/command/project/build -count=1
```

Binding and inspection evolution are scoped by
[issue #157](https://github.com/frost-leo/fathomry/issues/157).
