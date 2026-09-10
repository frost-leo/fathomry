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

# Compatibility assessment and policy

[Documentation](../../../README.md) / Internal package reference

**Audience:** composition and integration maintainers.
**Status:** implemented conservative comparison of supplied evidence.
**Package:** `github.com/frost-leo/fathomry/internal/compatibility`.

Start with the [compatibility interface](interface.md) for trust/copying limits.
Profile and baseline truthfulness require accountable integration owners.

## Bind effective settings to the original source

See the [internal effective-settings example](../../../../internal/resource/compatibility_example_test.go).
The following steps belong to framework integrations, not ordinary business setup.

1. Resolve and freeze settings with `resource.Prepare`. The Provider owns format,
   defaults, units, bounds and semantic validation.
2. Derive the safe `Profile` from the **same effective factory settings** used to
   construct the resource, including relevant defaults and overridden options.
   Account for hidden native defaults, retries, environment/files and callbacks.
   Do not copy arbitrary settings, credentials, endpoints or hashes of secrets.
3. Bind internal `resource.Access`. Call `Assess(build, access, profile,
   requirements, records)` before granting business I/O.
   Assembly/client construction is not permission for business mutation.
4. Apply `Report.Require` with an explicit composition/deployment policy.
   Keep the report even when policy rejects use. Known unsupported configuration
   should also be rejected in Provider preparation when it can be identified there.

The framework supplies original scope, Provider/name, configuration format,
preparation revision and applied limits from the authoritative resource to `Assess`.
A borrowing alias does not relabel them or gain a second allowance. The report owns no resource and
does not change admission or shutdown.

`Profile.ImplementationModule` identifies the Provider implementation's module,
including an explicitly disclosed main module when applicable. Missing/unversioned
implementation evidence cannot be hidden behind an unchanged Provider name.
SDK mode, service mode/version, protocol, native version and critical effective
options are separate fields. Unknown/declaration/observation/not-applicable states
are preserved; a configured service version is not an observed server version.
There is no universal service-version engine or automatic probe.

The configuration format is compared. The random preparation revision is retained
in both actual and baseline records but **not used for content equality**: different
preparations get different revisions even for the same settings. Applied source
limits are compared separately. Effective non-secret options must cover the
behavior the guarantee depends on; this library cannot infer that coverage from
arbitrary native clients or self-reported labels.

## Tested baselines and decisions

A `Record` applies to one named guarantee and exact `Combination`. It links:

`StandardRevision → Implementation → Evidence.Reference → Baseline → Limitations`

References are bounded code-owned IDs resolved in the integration document.
They are not local paths, arbitrary error text or automatically trusted assertions.
The integration owner reviews records **after** tests execute against the captured
combination. Never copy current startup facts into a passing baseline merely to
make them match. Fathomry installs no default SDK/service support catalog.

Each `Requirement` selects its guarantee and required evidence layers.
The layers are mechanism, capability, SDK and service. Evidence separately states
test double, source review, execution or isolated-service execution, and
passed/failed/skipped/not-run status. A double cannot be entered as executed SDK
or isolated-service evidence. Source review cannot establish a successful
behavioral test; a relevant documented contradiction can establish incompatibility.

For every required layer, successful executed coverage must include **defaults,
retries, cancellation, error identity, results and resources**. A compilation
check, successful connection, missing test, skipped test or version-range match
cannot fill those obligations. Unsupported or irrelevant behaviors require an
accurately scoped guarantee and tests of its restrictions, not invented coverage.

| Decision | Meaning |
| --- | --- |
| `Tested` | An exact sufficiently known combination has executed coverage for every requested layer/behavior |
| `Untested` | The actual combination is sufficiently known, but lacks an exact record with successful required execution/coverage; no support is inferred |
| `Unknown` | A necessary fact about the actual combination is unavailable, development, redacted or merely declared |
| `Incompatible` | An exact matching record establishes a relevant failed guarantee; it takes precedence over passing records |

`Decisions` retain matched record IDs and per-baseline difference axes. The
`Actual` and `Baselines` remain independently inspectable; there is no
compatibility boolean flattening their differences. Exact matching is conservative:
replacement shape, main/dependency role, applicable VCS, module sums, Go/platform,
Provider implementation, configuration, limits and profile changes need evidence.
It is not an algorithm proving equivalence between different build arrangements.

For multi-baseline aggregation, uncertainty in a historical record remains an
`Unknown` **difference**, not an unknown fact about a sufficiently known `Actual`.
Adding an unrelated or insufficient record cannot change a known `Untested`
decision to `Unknown` and thereby bypass a policy that disallows untested use.
This also applies to a partially known baseline with no known mismatch: without
an exact match it cannot establish support or relabel known current facts.
A sufficient exact passing record establishes `Tested`; an exact relevant failure
always takes precedence. Record order does not change status or policy outcome;
matched IDs and diagnostic differences remain in input order. See the
[multi-baseline regressions](../../../../internal/compatibility/assessment_regression_test.go)
for [Issue #11](https://github.com/frost-leo/fathomry/issues/11).

`Policy{}` requires tested evidence for every requested guarantee.
`AllowUnknown` and `AllowUntested` explicitly permit use despite those statuses,
without changing the reported facts or claiming tested support. No policy flag
waives `Incompatible`; `ErrUnsupported` identifies that refusal.
`ErrUnverified` identifies insufficient accepted evidence. These are private
`fault.Kind` conditions, not public semantic identities or wire-format versions.
