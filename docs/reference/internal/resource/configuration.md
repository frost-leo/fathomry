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

# Configuration preparation

[Documentation](../../../README.md) / Internal package reference

**Audience:** Provider and composition authors.
**Status:** implemented supplied-input preparation; no loader or reload.
**Package:** `github.com/frost-leo/fathomry/internal/resource`.

Start with the [resource interface](interface.md). This is the contract for
supplied settings, not an application configuration-file format.

## Format and layer semantics

`Input.Format` must equal the nonzero `Schema.Format`; there is no fallback to a
different version's defaults. This is the selected Provider's configuration schema
version, not the YAML specification version, module version, or source revision.

The supported DTO is a nonrecursive Go struct with explicit `json:"field"` tags.
Fields may contain strings, booleans, finite numeric values, pointers, nested plain
structs, string-keyed maps, and slices. Tag options, anonymous/private fields,
arrays, interfaces, byte slices, custom serialization, and runtime handles are
rejected. Default strings and map keys must be valid UTF-8; invalid bytes are
rejected instead of being silently normalized by JSON encoding. Named scalar types such as `time.Duration` can use their numeric
representation, with units defined by the Provider. SDK options, callbacks, dynamic
credential-refresh handles, and `time.Time` are not generic copyable settings:
derive or inject those at their actual ownership boundary.

Each explicit layer supplies one YAML mapping; JSON syntax is accepted as YAML.
This is a restricted YAML input contract, not a promise to accept all YAML features:

| Aspect | Contract |
| --- | --- |
| Precedence | Typed defaults < base < environment-specific < local < variables; independent of slice order |
| Layer identity | At most one document per non-default layer kind; the framework must resolve same-kind input selection explicitly; no loader or second merge policy is implemented |
| Structural validation | Every layer must have exact, known field names and compatible value types, even if a higher layer would override it |
| Semantic validation | Provider validation runs on the final effective settings, on a separate copy whose mutations are discarded |
| Objects | Recursive merge retains siblings; an empty object preserves inherited children |
| Maps and lists | String-keyed maps merge recursively without case folding; slices replace as a whole, never concatenate |
| Absence | Inherits existing settings; newly introduced objects use Go zero values for otherwise absent fields |
| Empty values | Empty string, false, numeric zero, and empty slice explicitly override |
| Null | Clears pointers, maps, and slices; rejected for non-nullable fields; not a deletion operator |
| Numbers | JSON numeric syntax, with target-type range checks; integer fields reject fractions and exponent notation without a float64 intermediate |
| Rejected syntax | Duplicate keys, non-string keys, anchors, aliases, merge keys, explicit/custom tags (including bare `!` on keys, values and collections), implicit timestamps, multiple documents |
| Bounds | Each input document and resolved JSON at most 1 MiB; normalized input nesting at most 64 levels |
| Reload | No implicit reload, environment reread, file watcher, credential rotation, or live settings mutation |

The pinned YAML parser is `go.yaml.in/yaml/v3 v3.0.5`. The restricted contract and
tests, not upstream defaults or a candidate SDK inventory, determine Fathomry
behavior. A parser change requires its own compatibility evidence.
Bare `!` loses `TaggedStyle` in this parser, so preparation also checks the original
character at each node's retained start position. This is not a character blacklist:
quoted/block scalars and comments may contain literal `!`. Source indexing follows
the parser's character coordinates, line breaks and BOM-selected UTF-8/UTF-16
decoding; it does not reinterpret or normalize scalar text.
[Upstream version policy](https://github.com/yaml/go-yaml#version-intentions),
[pinned release](https://github.com/yaml/go-yaml/releases/tag/v3.0.5).

## Loader integration constraints

Viper is not a managed resource or forbidden dependency. A future framework-supplied
configuration loader may use an isolated Viper instance where its semantics fit. The resource
foundation does not pass Viper objects to Providers. A Viper v1.21.0 comparison
confirmed that configuration lookup can fall back from explicit null to a default,
case-fold mapping keys, and modify a map supplied to `MergeConfigMap`; its
`MergeConfig` does preserve nested siblings in the tested case. These behaviors
are not interchangeable with the contract above. Taking `AllSettings()` cannot
recover input distinctions already lost. Loading must preserve the raw authorized
layer distinctions and invoke the existing preparation/merge once, not silently
apply a competing precedence or null policy. Configuration-center SDKs such as
Nacos would also be framework-owned; none is selected or integrated here.
[Viper documentation](https://github.com/spf13/viper/tree/v1.21.0),
[merge/lookup implementation](https://github.com/spf13/viper/blob/v1.21.0/viper.go).

## Isolation, provenance and revision

Preparation freezes resolved bytes, not an alias to the caller's maps or slices.
Every factory receives a freshly decoded settings copy, including when the same
selection is assembled concurrently in different scopes. Mutations by one factory
cannot change another instance's settings. The generic type boundary cannot be
reinterpreted as another configuration or capability type through an ordinary Go
conversion. Unsafe code is outside this boundary.
Private, defined generic identity markers seal both `Prepared[T]` and `Selection[C]`,
including anonymous struct types differing only in JSON tags. Such cross-type
value/pointer casts fail compilation; aliases of the identical type remain valid.
Converting plain settings before a new `Prepare` remains ordinary Go use and must
pass that preparation's own validation. No type registry or shared settings copy
is introduced.

Callers must not mutate input bytes/defaults concurrently with preparation.
Validation/factory closures and injected dependencies have their own explicit
concurrency and ownership contracts; freezing settings does not clone closures or
stop an SDK from secretly sharing resources. Those remain integration obligations.

`Description.Revision` is an opaque, random 128-bit preparation identity. Every
successful preparation gets a new revision, even for equivalent settings; reusing
that prepared value preserves it. It identifies the frozen effective settings,
including defaults, without hashing low-entropy secrets. It does not establish
content equality, authenticity, deployment correspondence, or compatibility.
Token refresh is not implemented, and mutable tokens never belong in frozen Run
input.

`Description.Provenance` records the declared layers and schema fields they
supplied, in precedence order. It is input provenance, not a reconstructed winning
origin for every dynamic map entry. Map/list contents, arbitrary map keys, raw
documents, file paths, and environment variable names are excluded. Returned
descriptions are independent copies. `Prepared` formatting is redacted and its
JSON serialization is refused.

## Executable evidence

[Preparation tests](../../../../internal/resource/config_test.go), [type/negative-compilation controls](../../../../internal/resource/types_test.go)
and [native YAML checks](../../../../internal/resource/yaml_test.go) exercise the rules.
Boundary repair: [Issue #17](https://github.com/frost-leo/fathomry/issues/17).
See [testing](../../../development/testing.md) for parser fuzzing.
