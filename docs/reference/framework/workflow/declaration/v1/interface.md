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

# Workflow declaration interface

[Documentation](../../../../../README.md) / Framework reference

**Audience:** workflow authors and maintainers of offline declaration tooling.
**Status:** implemented static declaration compilation, portable export and checking.
No workflow executor, registration store, artifact qualification or scaffolding is provided.
**Package:** `github.com/frost-leo/fathomry/framework/workflow/declaration/v1`.

## Responsibilities

This package owns the declaration rules used by project Go code and
[`fathomry workflow vet`](../../../../cmd/fathomry/interface.md#vet-workflow-bundles).
It is independent of Framework Runtime, configuration Boot, Adapters and Temporal
clients. Importing it or composing its resources opens no services.
The `workflow/declaration` boundary owns static declarations only; workflow
hosting, registration and execution are not additional responsibilities of this package.

The [closed profile](profile.md) defines what the declarations mean statically.
The declaration describes the intended plan; a matching diagram does not certify
arbitrary imperative Go orchestration. Business handlers are not supplied or called.

## Calling sequence

1. Author a concrete `Declaration` containing `WorkflowDeclaration` entries.
   Each owns a `Specification` and optional `ModeDeclaration` entries.
2. Call `Compile(ctx, declaration)`. Invalid data returns no bundle and a structured
   failure. Compilation supplies supported wire identifiers and digests.
3. Call `bundle.BytesCopy()` to obtain complete, normalized, data-only JSON.
4. At another data boundary, call `Parse(ctx, bytes)`. It validates the complete
   supplied document and recomputes, rather than trusts, its fingerprints.
5. Call `Check(ctx, bundles...)` to compare the supplied identities and canonical
   content. A completed conflict is a report with `Passed=false`, not an I/O error.

Public declarations and helpers are documented in
[declaration.go](../../../../../../framework/workflow/declaration/v1/declaration.go);
[bundle.go](../../../../../../framework/workflow/declaration/v1/bundle.go) and
[compare.go](../../../../../../framework/workflow/declaration/v1/compare.go) define snapshot/report APIs.
`Fixed` and `Tuned` express a slot; they do not validate or install defaults.

## Authoring versus file admission

Authoring accepts nil collections as empty only where emptiness is allowed.
A zero workflow list, empty required graph, missing binding, zero policy slot or
unset required schema bound is not a usable default. Schema bound pointers preserve
explicit zero versus absence. Object authoring may omit empty properties/required
sets and its fixed `additionalProperties=false`; exports spell these out.
Optional pointers use nil for absence. An explicitly supplied empty default,
heartbeat setting or schema is still validated, not silently omitted. A fixed
value and a control reference cannot coexist even if the fixed text is empty.

A Go mode may omit a value only when its exact workflow control declares a fixed
default. An exported file must contain all mode values, including those defaults.
`Parse` does not fill missing fields, repair a digest or substitute a management
default. Harmless JSON/member/set ordering may normalize; execution sequences may not.

Mode ownership is inherited from the enclosing workflow tuple, never inferred from
compatible parameter shapes. Namespace/code/version and mode/revision are explicit
identities, not content hashes or SemVer compatibility calculations. Duplicate
identities within one bundle reject; different identities may have identical content.

## Ownership, limits and concurrency

Inputs are borrowed only for the call. Do not mutate a declaration or byte buffer
concurrently with compilation/parsing. Successful bundles retain no caller-owned
mutable maps, slices or pointers. Bytes and reports are detached copies.
A bundle may be read concurrently; do not overwrite its handle while in use.

Nil/zero bundle handles are invalid. JSON marshaling exports a valid snapshot,
including a value-copied handle; zero values fail. Standard JSON encoders represent
nil pointers as `null`, not as a valid bundle. Use `BytesCopy` for an export that
also rejects nil handles. JSON unmarshaling into a bundle is refused so that a
shared snapshot cannot be overwritten. Use context-bound `Parse`. Default
formatting/logging does not dump bundle contents, including for value copies.
Caller contexts are explicit; functions spawn no goroutines.

Admission bounds are not throughput or execution-capacity claims:

| Boundary | Bound |
| --- | --- |
| Bundle bytes / JSON container depth | 16 MiB / 64 |
| Specification bytes / inner depth | 4 MiB / 48 |
| Mode bytes | 1 MiB |
| Workflows / modes | 64 workflows, 64 modes per workflow, 256 modes per bundle |
| Check input set | 1–16 bundles, 64 MiB total canonical bytes |
| Decoder values and member names | 1,048,576 per document |
| Go authoring preflight | Same value/text budget, depth 128 and 4,096 bytes per string, before encoding |
| Diagnostics / conflict paths | 128 conflicting identities, 16 paths per identity, explicit truncation flags |

The profile adds per-collection and scalar bounds. Oversized source, normalized
content or report processing fails explicitly; inputs are never truncated into
a successful declaration. The final export is checked against the same value
budget after mode-default materialization, not only before it.
Cancellation is checked throughout finite passes.
These process-local APIs are not a Temporal Workflow execution API.

## Reports and failures

`Report.Entries` contains consistent identities and recomputed fingerprints.
Workflow specifications are compared before modes: modes under a conflicting
parent are counted in `UncomparedModes`, not presented as equal. Mode subsets can
coexist under an equal parent; no source-project membership is inferred.

`Conflicts` supplies identities, fingerprints and bounded differing paths without
source values. `Truncated` and `PathsTruncated` distinguish omitted details from
an exhaustive report. Check still fails if any omitted conflict exists.
The CLI's `Diagnostics` associates rejected documents with zero-based input indices;
counts cover successfully validated declarations, not unknown invalid contents.

The workflow owner uses facility `0x1C1` in the orchestration capability domain:

| Code | Meaning |
| --- | --- |
| `ErrInvalid` | Invalid JSON, shape, identity, references, graph, slots or mode values |
| `ErrUnsupported` | Unknown or mismatched document kind/format/semantics/profile |
| `ErrIntegrity` | Malformed or mismatching supplied fingerprint; missing required fields are invalid declarations |
| `ErrLimit` | Admission or processing bound exceeded |
| `ErrCanceled` | Caller cancellation; native context cause remains inspectable |
| `ErrHandle` | Invalid context, bundle handle, decoding route or input set |

`Diagnose` selects only a directly supplied completed validation occurrence,
returning a safe reason and JSON Pointer (empty denotes the root). It does not
traverse joins, select a cause or turn cancellation into completed checking.
Joint-constraint failures for a mode point to that mode's `config/values`, not to
the specification fields that declare the constraint.
Runtime errors are not wire DTOs. `Definitions`, `Resources` and `Component`
support explicit failure/i18n catalog composition, not a global registry.

A passing check proves only supplied static consistency. It does **not** prove
freshness, that every project declaration was exported, handler compatibility,
safe retries, actual effects, registration, deployed code or runtime readiness.

## Independent usage and evidence

The maintained [health declaration](../../../../../../framework/workflow/declaration/v1/testdata/health/declaration.go)
contains two connected nodes, three bindings and two modes. The
[export helper](../../../../../../framework/workflow/declaration/v1/testdata/consumer/main.go)
uses the project's declaration package and public APIs, not generated Boot.
See the [independent example recipe](../../../../../../framework/workflow/declaration/v1/testdata/consumer/README.md).

`TestIndependentConsumer` builds a separate module with `GOWORK=off`, verifies
the selected dependency, executes the helper and the actual official CLI, and
tests stale source, explicit regeneration, identity conflicts and exporter failure.
Compatibility fixtures cover the finite orders, branching and stage-gate profiles.
Independent JavaScript canonical vectors require Node.js; a missing executable
is an explicit skip, not cross-language qualification. No live services are needed.

This work implements the bounded outcome in
[issue #153](https://github.com/frost-leo/fathomry/issues/153), not workflow execution.
