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

# Technical fault interface

[Documentation](../../../README.md) / Internal package reference

**Audience:** internal mechanism and boundary authors.
**Status:** implemented private Go error contract; framework errors remain deferred.
**Package:** `github.com/frost-leo/fathomry/internal/fault`.

This package preserves technical kinds, bounded context and original causes.
It has no Run/Item/framework-attempt meaning, retry policy, localization, converter
registry or public/versioned error catalog. Its calling interface consists of
functions and concrete values; it does not require a new Go interface type.

## Capabilities

| Surface | Contract |
| --- | --- |
| `Kind` | Code-owned technical identity, not arbitrary native text |
| `Context` / `Correlation` | Value-only technical location and optional call association |
| `Kind.New` | Validate kind/context and retain original causes without presenting them |
| `Error.Diagnostic` | Copy kind/context/cause-presence metadata, not the native graph |
| `Error.Is` / `Error.Unwrap` | Preserve standard `Is`/`As`; return independent cause-slice storage |

Declare static kinds, supply safe context and return errors normally. An outer
boundary may wrap them with standard Go errors while preserving native inspection;
that does not add framework semantics to this package.

## Context and value conventions

- `Kind` uses 1-128 lowercase ASCII letters, digits, dot, underscore or hyphen.
- `Operation`/`Provider`/`Scope`/`Source` labels are optional; present labels use 1-64
  characters from that alphabet. `Scope` is original resource composition scope.
- `Call`/`Parent`/`Owner` are optional 1-128 character IDs; uppercase letters are also
  permitted. `Parent` cannot equal `Call`. Invocation separately requires `Call`.
- `Owner` is opaque association, not authorization, quota or effect proof.
- Empty `Context`/`Correlation` is valid outside a call. Invalid kind/context yields
  `Invalid` with empty context while retaining original causes.

Syntax is not secret detection or authentication. Callers must exclude secrets
and own the uniqueness scope of their identifiers.

## Ownership, concurrency and inspection

`Error` owns immutable context and copied cause-slice storage. Changes to input
slices, `Unwrap` output or a local `Diagnostic` do not rewrite it. Original native
objects are retained, not cloned; their SDK owns lifetime/concurrent-use guarantees.
Intentional `Is`/`As` preserves custom native matching and exact objects, not just text.

`Is` matches a `Kind`. A distinct occurrence does not become the same occurrence
merely because its kind matches. A nil `*Error` has no causes and `Error` returns
`<nil>`; a zero non-nil `Error` reports `Invalid`. Neither invents native evidence.

## Failure boundaries and evidence

Kinds do not establish external effects, successful cleanup or retryability.
Keep primary and cleanup observations at their owning boundary. See
[diagnostics](diagnostics.md) for presentation/JSON limits and the
[architecture](../../../architecture/errors-and-evidence.md) for the future framework role.

[Source](../../../../internal/fault/errors.go), [contract tests](../../../../internal/fault/errors_test.go)
and [boundary tests](../../../../internal/conformance/boundary_test.go) cover
native context, copying, validation and wrapping. Run `go doc -all ./internal/fault`
for exact declarations.
