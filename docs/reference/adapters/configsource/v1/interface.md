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

# Configuration acquisition and strict preparation

[Documentation](../../../../README.md) / Public package reference

**Audience:** application authors and Framework configuration consumers.
**Status:** implemented public contracts, independent of Internal preparation.
**Package:** `github.com/frost-leo/fathomry/adapters/configsource/v1`.

## Responsibilities

Selected provider Sources supply complete original documents. This package owns
their immutable Batch contract and independent strict preparation. It does not
select services, own native polling/recovery, publish settings or reconstruct
instances. The complete provider APIs remain independently available:
[Viper](../viper/v1/interface.md) and [Nacos](../nacos/v1/interface.md).

`Source.Capture` returns a complete Batch, failed slot index (-1 if unknown/success)
and error. `Source.Observe` returns an owned Observer; Next separates a failed wait
from an Observation carrying a source error or a complete batch. Check Batch
validity and expected document count before consuming it. A zero Batch is invalid.
Missing, present-empty and acquisition failure are different facts.

`NewBatch` admits at most 16 UTF-8 documents, 1 MiB each and 4 MiB total.
It copies bytes; `DocumentsCopy` copies again. Missing slots cannot contain bytes.
Source observations may coalesce or drop events with an explicit Gap; a delivered
successful Batch is already a complete reacquisition, not a request to refetch it.

BindVariables[T] admits at most 64 explicitly captured struct-field bindings.
It performs no environment I/O. Unset differs from empty; literal text is for
strings, while explicit JSON preserves typed values/null/maps/lists. Duplicate
or ancestor-overlapping paths and map-key/list-index addressing reject. The
result is one bounded Variables layer consumed by the same preparation engine.

ParseDotenv decodes at most 64 KiB / 64 literal assignments into a caller-owned
map without filesystem or process access. ASCII environment names, single quotes,
strict JSON double quotes and comments are supported. Duplicate/invalid names,
export and multiline assignments reject; dollar signs remain literal. It does
not authorize the names, choose a file, mutate environment or select a provider.
The byte bound is inclusive whether or not the last line ends with LF or CRLF;
line terminators count toward the same document bound.
The composition root performs those explicit bindings before calling BindVariables.

## Prepare original layers

`Prepare(ctx, Schema[T], layers)` supports complete nonrecursive struct schemas
with explicit ASCII `json` field names (letters, digits, underscore, hyphen).
Fields may contain named/builtin scalars, pointers, nested structs, string-keyed
maps and slices. Anonymous/private fields, tag options, interfaces, arrays, byte
slices and custom JSON/text codecs are refused. This decoder profile does not
restrict the separate settings package's arbitrary owner-defined copy contract.

Schema Version is nonzero, naming the selected schema, not inferred from source
bytes or SDK/module versions. Each layer explicitly selects JSON, restricted YAML
or strict TOML. No in-band version header, file discovery, environment scan or
transcoding is inferred. Native Viper decoding remains a separate weak profile;
dotenv is not an application-document encoding here.

| Concern | Contract |
| --- | --- |
| Order | Typed defaults < Base < Environment < Local < Variables, independent of slice order; duplicate kinds reject |
| Validation | Every layer must have exact known struct fields and compatible types, even if later overwritten |
| Objects/maps | Recursive overlay; empty objects preserve inherited children; dynamic map keys remain case-sensitive |
| Lists | Whole replacement, never concatenation |
| Absent/empty | Absence inherits; zero, false, empty string and empty list explicitly override |
| Null | Clears pointers/maps/slices; refuses non-nullable fields; not a map-key deletion operator |
| Numbers | Lexical precision and target-width range checks; integer fractions/exponents reject |
| Syntax | Duplicate decoded keys, invalid Unicode, multiple documents, YAML anchors/aliases/merge keys/explicit tags/timestamps reject |
| Bounds | UTF-8 only; 1 MiB per document and resolved JSON, 64 child-depth steps, 32,768 structural nodes; bounded schema traversal |

TOML uses the pinned parser's original AST scalar tokens and native grammar/table
validation, not a decoded float64 map or a text-rewriting pass. Dotted keys,
inline tables and arrays of tables are supported. Integer tokens must fit signed
64-bit TOML range; floats retain lexical precision through target-width conversion.
Dates/times and non-finite numbers reject rather than silently becoming strings.
Duplicate/redefined tables and keys reject. Empty or comment-only TOML is a valid
empty map; empty JSON/YAML is invalid. A present-empty document is never treated
as a missing optional source slot.

New nested objects start from Go zero values for missing fields. The validator
receives a separate final candidate; mutations are discarded. Cancellation is
checked between bounded phases and after validation. User validation must be
bounded, non-panicking and cooperative; it cannot be forcibly interrupted.

## Accepted values and evidence

A successful Prepared owns canonical bytes; ValueCopy isolates every mutable
container. Snapshot supplies a public settings snapshot without publishing it.
Initial failure returns no usable Prepared or default-looking zero value.

Description contains schema Version, random opaque Revision and declared struct
field provenance. Revisions are neither sortable nor secret-content fingerprints.
Paths, environment names, dynamic map keys and values are excluded. Descriptions
are detached. Runtime handles refuse JSON reconstruction/serialization; deliberate
Raw/ValueCopy inspection is sensitive.

Component Definitions and Resources compose explicitly with failure/i18n.
They install no locale or catalog. Original parser/validator/cancellation errors
remain causes; default diagnostics do not format them.

## Executable contracts

- [Preparation, bounds, isolation and fuzz tests](../../../../../adapters/configsource/v1/prepare_test.go).
- [TOML native-structure comparison, numeric precision and fuzz tests](../../../../../adapters/configsource/v1/toml_test.go).
- [Raw batch contracts](../../../../../adapters/configsource/v1/acquisition_test.go).
- [Independent provider consumers and dependency/authority checks](../../../../../adapters/configsource/v1/integration_test.go).

These are process-local contracts, not durable Workflow execution or a complete
Framework loading/Watch scenario.

## Package organization

The [Adapter tree map](../../../../../adapters/README.md) defines this package's role
and file responsibilities; shared mechanisms, capability vocabulary, preparation
and concrete providers do not acquire identical APIs by convention.
