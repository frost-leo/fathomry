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

# Framework configuration interface

[Documentation](../../../README.md) / Public framework capability

**Audience:** independent business projects and configuration-adapter maintainers.
**Status:** implemented finite local/Nacos loading; no application runtime, generator,
reload, mixed-source aggregation or durable configuration protocol.
**Package:** `github.com/frost-leo/fathomry/framework/configuration`.

## Responsibilities and use

`Load` accepts a project-owned `Schema[T]` and `Request`, obtains original
documents from an explicitly selected `Provider`, and prepares one immutable
`Configuration[T]`. The [local adapter](../../adapters/configuration/local/interface.md)
provides file acquisition; the [Nacos adapter](../../adapters/configuration/nacos/interface.md)
provides remote acquisition and owned cleanup. Projects declare their schema, sources and permitted
environment bindings; they do not construct SDKs, resource factories or evidence
receivers. The framework package imports no concrete Provider or adapter.

`Configuration.Value` returns an independent plain-data copy, including nested
maps, slices and pointers. Values can contain secrets and are not diagnostics.
`Description` returns an independent `Description`. A zero Configuration
has no usable value or nonzero description; every failed load returns that zero.

## Input and preparation semantics

SchemaVersion is positive and must match the Provider's declared project-data
version. It is not a Go API or SDK version or a version inferred from
the document's bytes. The supplied schema defines the supported meaning;
matching declarations do not authenticate the source or detect arbitrary changes
to project code. There is no implicit migration or unknown-version fallback.

T uses the [existing preparation contract](../../internal/resource/configuration.md):
nonrecursive plain structs with explicit json field names, exact field/type and
numeric validation, no native handles/custom serialization, recursive object
merging and whole-list replacement. Every supplied layer is validated, even when
a higher layer overrides it. Validation receives an isolated copy; its mutations
do not alter the published configuration.

| Input | Meaning |
| --- | --- |
| Provider | Explicit non-nil acquisition capability; no global registry or project discovery |
| Documents | At most three, unique public names and Base/Environment/Local layers |
| Absent document | An adapter-observed optional absence; cannot contain data |
| Empty present document | Invalid input content, not a missing optional source |
| Order | Defaults < Base < Environment < Local < Variables, independent of slice order |
| Size | Each document and resolved JSON <=1 MiB; existing preparation depth <=64 |
| No documents | Explicit Provider-selected defaults/environment-only preparation |

The Provider's public label and source names are 1-64 lowercase ASCII letters,
digits, dots, underscores or hyphens. They must contain no secrets; grammar
validation is not a secret detector.

## Environment bindings

Only names explicitly present in Request.Variables are looked up. A selected
name is captured once before acquisition and never read again for that load.
No bindings means no process-environment discovery. This capture is not an
atomic transaction with files, or with concurrent process-environment changes.

At most 64 bindings are accepted. Name is an exact ASCII environment name,
<=256 bytes, beginning with a letter or underscore. Field selects a declared
struct path such as `/connection/host`, <=1024 bytes and 16 components.
Dynamic map keys, array indices and overlapping/duplicate field bindings are
refused. A whole map/list field may use explicit JSON encoding.

- Text is the zero/default encoding and requires a string or pointer-to-string
  field. Empty text and the spelling `null` remain literal strings.
- JSON accepts one explicit JSON value, preserving numeric spelling. It performs
  no implicit duration, boolean, list or locale conversion. The final preparation
  still enforces the field type and full input contract.
- Missing optional variables inherit lower layers. Required means present;
  an empty text value is present, while empty JSON is malformed.
- Binding validation occurs even when the variable is absent. Unsupported paths
  or encodings do not disappear because the current machine lacks a variable.

Selected values are valid UTF-8 and their aggregate input bytes are <=1 MiB;
the resulting variable document is also <=1 MiB. These and the document bounds
do not certify total RSS, arbitrary Provider allocation or native parser memory.

## Errors, ownership and cancellation

Stable failure identities distinguish invalid declarations, unavailable sources,
schema mismatch, invalid configuration, validation refusal, capacity refusal and
cancellation. Errors can be matched with errors.Is. Native parser/OS text is
withheld by the local adapter; selected safe filesystem categories remain
inspectable. Unclassified custom-Provider errors become Unavailable without
exposing their native cause. Provider-supplied public failure occurrences retain
their declared identity and deliberate cause contract.

The Nacos adapter retains safe Missing/Denied causes under Unavailable, rather
than leaking native remote errors. These optional inspection identities do not
change the local adapter's existing filesystem-cause contract.

Caller cancellation causes and the project's explicitly supplied validation error
remain deliberate public inspection data. Printing the outer failure stays safe;
walking those caller-owned causes is not sanitized. The loader does not localize
errors or turn an error category into retry/business-state policy.

All work is synchronous. Contexts are caller-owned, checked at phase boundaries
and passed to the Provider. The loader cannot interrupt arbitrary blocked
Provider/validator/filesystem/parser code and starts no worker to hide it.
A custom Provider must own cleanup and must not return unowned work, native handles
or usable partial data. Providers/validators are trusted Go extensions, not a
sandbox. Their inputs must not be concurrently mutated during a call.

## Description and evolution

Description includes the declared data version, opaque preparation revision,
Provider label, source presence, supplied schema paths and binding presence.
It excludes file paths, environment names, values and arbitrary map keys.
Contributions are supplied-field provenance, not a full winning-origin tree.
A missing optional source remains visible without inventing supplied fields.

Go APIs and structs evolve with the framework module's compatibility policy,
not a version suffix on each name. Existing consumer declarations and behavioral
fixtures protect field/default/zero-value, overlay and error semantics. Compatible
additions preserve those meanings; required fields, changed defaults, units or
guarantees need an explicit compatibility decision. An additive API may preserve
existing consumers; genuinely breaking stable APIs belong in a major module
release rather than a growing collection of numbered copies.

Project-data schemas are a separate concern. Load can process a declared schema 2
with its matching project type and source declaration, but does not reinterpret
schema 1 as schema 2. Explicit business conversion is demonstrated in the tests;
there is no automatic migration or speculative parallel API.

Configuration is a runtime handle, not a versioned wire DTO. Requests, raw input
and handles refuse ordinary JSON persistence/reconstruction. Description is safe
in-process metadata, not a promise of a stable CLI or durable record format.

## Executable evidence

[Unit, concurrency, privacy, schema evolution and fuzz cases](../../../../framework/configuration/configuration_test.go)
exercise the public contract. The [independent consumer test](../../../../framework/configuration/consumer_test.go)
builds a real file-proxy module artifact without source replacements and rejects
a direct internal-package import. Its [project fixture](../../../../framework/configuration/testdata/project)
pins original keyed declarations and behavioral expectations.

See the [project loading guide](../../../development/load-project-configuration.md)
and [version responsibilities](../../../architecture/versioning.md). This capability
does not establish service readiness or satisfy the entire Run input-freezing goal.
