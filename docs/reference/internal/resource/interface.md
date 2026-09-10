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

# Resource interface

[Documentation](../../../README.md) / Internal package reference

**Audience:** framework composition and in-module Provider authors.
**Status:** implemented local contract, not service support.
**Package:** `github.com/frost-leo/fathomry/internal/resource`.

This package prepares typed settings and manages authoritative resource ownership,
borrowing, admission and cleanup. It is framework-maintainer machinery, not business
bootstrap code or a public Provider SDK. See [package boundaries](../../../architecture/package-boundaries.md).

## Capabilities and call sequence

| Surface | Calling contract |
| --- | --- |
| `Schema`, `Input`, `Prepare`, `Prepared` | Validate/freeze explicitly supplied settings before acquiring resources |
| `Select`, `WithLimits` | Keep the exact typed token and attach a policy before construction |
| `Assemble`, `Factory`, `Resource` | Use separate initialization/cleanup contexts; report acquired cleanup responsibility even on error |
| `Bind`, `AccessFor`, `Access` | `Bind` returns the capability; `AccessFor` supplies scoped admission; `Bind` alone does not wrap SDK methods |
| `Borrow`, `Delegate` | Declare borrowing/transfer selections; acquisition happens in the receiving `Assemble` |
| `Acquire`, `Lease.Retain/Release/Done` | Bound local-use trees over one root allowance |
| `Snapshot`, `Close`, `ReleaseResult` | Observe/discharge responsibility without inventing completion |

### Preparation before construction

1. Define a Provider-owned `Schema[T]`: configuration format, typed defaults, and
   pure semantic validation. Resolve and authorize secrets in outer composition.
2. Call `Prepare(schema, input)` for the explicitly selected source. Handle its
   error. This operation acquires no resource and reads no files or environment.
3. Call `Select(prepared, factory)` to obtain a typed selection token.
4. Call `Assemble(initCtx, cleanupCtx, scope, selections...)`. Give initialization
   and cleanup independently appropriate caller-owned budgets.
5. Use `Bind(assembly, selection)` only at the trusted outer composition boundary.
   Pass the resulting non-owning facade, not the assembly or resource, to consumers.

The assembly validates every selection and duplicate local name before any factory
runs. A zero/unprepared selection, missing constructor, unavailable source, or
known exclusive-transfer conflict is rejected at preflight. A sharing scope can
become unavailable after preflight; acquisition then fails through the normal
partial-initialization cleanup path.

Provider identity identifies an implementation, not its SDK version. Source names
are unique across all Providers within one assembly. Provider/name/scope labels
use 1–64 lowercase ASCII letters, digits, dots, underscores, or hyphens. Composition
must exclude secrets from these labels; syntax cannot authenticate or redact them.
They are not automatically appropriate metric labels.

Composition owns scope-label uniqueness wherever evidence from independent
assemblies is combined. `Scope` labels are not globally registered or automatically
qualified with a process/worker identity. Distinct isolated applications can reuse
labels; combining identically labeled assemblies would make their diagnostic
attribution ambiguous even though their resource records remain independent.

## Caller obligations and zero values

Do not copy `Assembly`; use its pointer. `Bind`, sharing, `Snapshot` and `Close` may run
concurrently under their contracts; callbacks/native objects have separate duties.
Keep a non-nil assembly returned with an error: it may still own incomplete cleanup.

Zero/unprepared selections are invalid. Nil/zero `Assembly.Close` or a nil `Close`
context returns `ErrSelection`, not success. `Snapshot` on a nil assembly is empty
metadata, not ownership evidence. Nil/zero `Lease.Release` is a no-op; `Done` is nil,
not a completed signal. An invalid `Access` cannot admit work.

`Prepared` settings and returned provenance use independent storage. Native causes,
capabilities and callbacks are not generically deep-copied. Do not mutate inputs
concurrently with preparation or infer physical isolation from distinct names.

## Results and failure boundaries

`Quiescent`, `Released` and Err in `ReleaseResult` are separate facts. Both positive
facts are required for completion; use explicit `Continue` rather than retrying
the original callback. `Close` is synchronous/cooperative, not forced cancellation.
[Technical errors](../fault/interface.md) retain primary and cleanup inspection,
without interpreting a timeout as no remote effect.

`Prepared`, `Access` and `Lease` have specific presentation/JSON guards. `Assembly`, `Report`
and all metadata are not covered by a universal runtime guard. None has an approved
durable protocol just because it is representable in Go. No loader, reload,
concrete service Provider or global registry is supplied.

## Details and executable evidence

- [Configuration](configuration.md), [ownership](ownership.md), [admission](admission.md), [shutdown](shutdown.md).
- [Configuration source](../../../../internal/resource/config.go), [ownership source](../../../../internal/resource/resources.go), [admission source](../../../../internal/resource/admission.go).
- [`Assembly` example](../../../../internal/resource/example_test.go), [configuration tests](../../../../internal/resource/config_test.go), [resource tests](../../../../internal/resource/resources_test.go), [cancellation tests](../../../../internal/resource/resources_cancellation_test.go).

Run `go doc -all ./internal/resource` from the repository root for exact declarations.
Foundation provenance: [Issue #4](https://github.com/frost-leo/fathomry/issues/4);
private migration: [Issue #15](https://github.com/frost-leo/fathomry/issues/15).
