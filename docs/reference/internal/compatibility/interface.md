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

# Compatibility interface

[Documentation](../../../README.md) / Internal package reference

**Audience:** composition and integration maintainers.
**Status:** implemented local assessment, not support certification.
**Package:** `github.com/frost-leo/fathomry/internal/compatibility`.

This package separates build facts, effective settings, reviewed records and
use policy. It does not select SDKs, authenticate deployment, probe services,
own resources or install a default support catalog.

## Capabilities and call sequence

| Surface | Calling contract |
| --- | --- |
| `Inspect` | Read the running executable's metadata, not the framework's go.mod |
| `FromBuildInfo` | Normalize supplied metadata; nil means unavailable, not current-process facts |
| `Build` / `Profile` / `Requirement` / `Record` | Separate evidence axes; caller-supplied records are not attested |
| `Assess` | Read original source and limits from one authoritative resource `Access` |
| `Report.Require` / `Policy` | Enforce declared policy without rewriting observed facts |

Use `resource.Prepare`, derive a safe `Profile` from the same effective factory inputs,
obtain `Access` through `resource.AccessFor`, inspect the build and assess reviewed records before granting a
capability. Keep the report on refusal. Do not create a passing baseline by
copying current startup facts into it.

## Caller obligations and zero values

Nil/zero `Access` cannot support a valid assessment. `FromBuildInfo(nil)` does not
borrow the current Go version; `Inspect` separately supplies runtime observations.
`Policy{}` requires tested evidence. `Report{}.Require` returns `ErrUnverified` even
under permissive policy. `AllowUnknown` and `AllowUntested` never waive
`Incompatible`. Invalid/unsupported/unverified technical
errors do not decide business retry or terminal status.

Supplied `debug.BuildInfo`, `Profile`, `Record` and hand-constructed reports are not authenticated.
Missing facts are not proof of absent code/services; unused requirements are not
linked code. `SourceRevision` is a preparation identity, not content equality.

## Data, ownership and limits

These are in-process Go APIs; new aggregates refuse JSON encoding/reconstruction.
They are not approved persisted DTOs, a Temporal failure/history format or an
attestation protocol. No Go errors, clients, contexts or runtime ownership handles
are serialized. Applications requiring a durable format must select its owner,
schema/version, limits, unknown semantics and historical-reader/migration policy.

`FromBuildInfo` borrows input only during the call. It returns fresh metadata.
`Build.Clone` copies slice storage; copying its outer Go struct alone does not.
`Assess` copies supplied build/profile/record/evidence slices and source-provenance
slices, including their field-name slices. Returned reports are caller-owned
snapshots, safe for concurrent reads without mutation. Do not mutate inputs
concurrently with inspection/assessment.

Limits: 64 selected/disclosed module paths each, 128 bytes per ordinary token,
256 bytes per permitted module path, 64 options per list, 32 requirements,
128 records, 32 evidence entries and 32 limitation IDs per record, 4 unique layers,
and the 6 defined behaviors. Records must include a limitation reference.
Unknown enum values, duplicate IDs/options/layers and malformed inputs are rejected
with safe errors. These metadata bounds are not process/native-memory guarantees.

Module/public API compatibility, error semantics, Provider configuration format,
source revision, data protocol, Workflow/mode, SDK/service/native version and
build/deployment identity retain their separate owners. These mechanisms do not
define Workflow commands, converters or history/payload formats.

## Details and executable evidence

[`Build` facts](build-info.md) and [assessment](assessment.md) expand the two subjects.
Run `go doc -all ./internal/compatibility` for exact declarations.
[`Build` source](../../../../internal/compatibility/build.go), [assessment source](../../../../internal/compatibility/assessment.go),
[consuming-build tests](../../../../internal/compatibility/consumer_test.go),
[policy regressions](../../../../internal/compatibility/assessment_regression_test.go)
and [effective-settings example](../../../../internal/resource/compatibility_example_test.go)
provide the implementation/evidence entry points.
