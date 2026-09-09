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

# Version accountability and compatibility evidence

Status: implemented process-local foundation for [Issue #6](https://github.com/frost-leo/fathomry/issues/6).
This is not an SDK support catalog, service discovery, integrity attestation,
deployment admission service or complete Workflow-version consistency check.

`compatibility` consumes the existing [source boundary](named-sources.md).
It imports no concrete Provider, `testing`, exporter, orchestrator or Temporal SDK.
`internal/conformance` is maintainer-only testing support described in the
[executable integration guide](../development/sdk-integration.md).

## Public boundary and package ownership

The intended public caller is the independent application's trusted composition
or diagnostic code, not each business Item. It must inspect its own consuming
build and evaluate selected source guarantees without importing private Providers.
That requirement justifies this public diagnostic contract, not a public SDK
extension/testing framework. `Build`/`Profile`/`Record` describe the facts and
reviewed inputs; `Report`/`Decision` preserve the results; `Require` applies the
composition owner's explicit policy. None authenticates claims or grants resource
ownership. Integration owners remain accountable for baselines and effective
profiles; business code cannot manufacture support by inventing a passing record.

This responsibility spans the application build and several resources. It is not
owned by `source`'s configuration/lifecycle or `operation`'s individual calls.
It therefore stays a cohesive public package rather than moving unrelated code
into those packages or adding an empty grouping/forwarding layer. Normalization,
matching and validation helpers remain unexported in this package; a separate
internal implementation package would not hide anything further from consumers.
The [independent module test](../../source/boundary_test.go) verifies public
inspection/assessment, forbidden internal-test imports, and test-free production
dependencies. Package placement does not establish code/deployment authenticity.

## Actual builds are not dependency declarations

`Inspect(BuildRequest)` reads `runtime/debug.ReadBuildInfo` from the consuming
binary. It never reads the framework's `go.mod`, process environment, source
checkout or local Git at runtime. `FromBuildInfo` also accepts metadata read from
another binary through `debug/buildinfo`; nil means unavailable and does not
substitute the current process's facts. Neither API authenticates supplied metadata.

The report separates:

- The Go toolchain that built the running binary; `Inspect` can obtain this from
  `runtime.Version` even without module metadata.
- The main application module, including its available VCS revision and modified
  flag. Main-module versions may be VCS-derived, development, or contain `+dirty`.
- Fathomry as the main module **or** a contributing dependency module.
- Each explicitly requested implementation/SDK module's selected version/checksum
  and its distinct replacement, if any.

Go's build information describes modules contributing packages, not every
requirement or every checksum in `go.sum`. Consumer requirements and workspace
selection can change those versions; dependency-local replace directives do not
control the consuming application's selection.
[Go module selection/replacements](https://go.dev/ref/mod#minimal-version-selection),
[BuildInfo](https://pkg.go.dev/runtime/debug#BuildInfo).

`SDKModules` selects at most 64 relevant implementation/SDK module paths.
`DisclosePaths` permits up to 64 additional module paths in metadata, such as a
versioned fork or the main application's module. It does not add a dependency or
assert that the path independently contributes packages. Fathomry and requested
SDK paths are implicitly permitted. No unrelated dependency inventory is emitted.

| Available evidence | Representation and limit |
| --- | --- |
| Requested module has no unambiguous entry | `Present=false`; Path retains the query identity. This does not prove absence from the source graph |
| Versioned dependency | Original selected version and sum, when available; no borrowed main VCS revision |
| Versioned replacement | Original selection and replacement path/version/sum separately; unapproved replacement path redacted |
| Local replacement | `LocalReplacement`; path discarded, replacement version marked development; local modification status is unknown |
| Workspace/development module | Development version, no invented dependency VCS facts |
| Dirty main Framework | Available version/revision/modified facts retained, but not an exact tested-content match |
| Missing checksum, e.g. vendor metadata | Unknown checksum, never a fabricated hash or integrity assertion |
| Missing/ambiguous/redacted fields | Explicit unknown/redacted/development facts; not automatically incompatible |
| Native libraries, services and deployment | Not inferred from Go modules; their owners must provide appropriate evidence |

Main VCS settings describe the main source tree, not every dependency. Standard
build flags can suppress them. The API omits raw linker/compiler flags, CGO search
paths and other potentially sensitive settings; only selected bounded platform
settings are retained. Nonempty redacted build flags (including tags/experiments)
remain explicit in `OpaqueSettings` using fixed names only. Go's `-trimpath`
omission of linker/CGO flags is also marked unknown, not assumed empty. These
states prevent an exact tested match; use requires the explicit unknown policy.
No absolute, relative, Windows or UNC replacement path is
stored. Provider-supplied labels/options still must be non-sensitive: syntax is
not a general secret detector.
[Go VCS stamping](https://pkg.go.dev/cmd/go#hdr-Compile_packages_and_dependencies).

Checksums and revisions are reported build metadata, not a verification of local
module-cache/vendor contents, native code, artifact integrity, authorized
deployment, or all shared-code effects on a Workflow. The eventual R03 execution
consistency guarantee remains separately required.

## Bind effective settings to the original source

See the [executed composition example](../../compatibility/example_test.go).

1. Resolve and freeze settings with `source.Prepare`. The Provider owns format,
   defaults, units, bounds and semantic validation.
2. Derive the safe `Profile` from the **same effective factory settings** used to
   construct the resource, including relevant defaults and overridden options.
   Account for hidden native defaults, retries, environment/files and callbacks.
   Do not copy arbitrary settings, credentials, endpoints or hashes of secrets.
3. Bind trusted `source.Access`. Call `Assess` before granting business I/O,
   supplying the actual build, effective profile, required guarantees and reviewed
   records. Assembly/client construction is not permission for business mutation.
4. Apply `Report.Require` with an explicit composition/deployment policy.
   Keep the report even when policy rejects use. Known unsupported configuration
   should also be rejected in Provider preparation when it can be identified there.

`Assess` obtains original scope, Provider/name, configuration format, preparation
revision and applied limits from the authoritative resource. A borrowing alias
does not relabel them or gain a second allowance. The report owns no resource and
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
| `Untested` | Known combination differences or missing successful execution/coverage; no support is inferred |
| `Unknown` | A necessary fact/equivalence is unavailable, development, redacted or merely declared |
| `Incompatible` | An exact matching record establishes a relevant failed guarantee; it takes precedence over passing records |

`Decisions` retain matched record IDs and per-baseline difference axes. The
`Actual` and `Baselines` remain independently inspectable; there is no
compatibility boolean flattening their differences. Exact matching is conservative:
replacement shape, main/dependency role, applicable VCS, module sums, Go/platform,
Provider implementation, configuration, limits and profile changes need evidence.
It is not an algorithm proving equivalence between different build arrangements.

`Policy{}` requires tested evidence for every requested guarantee.
`AllowUnknown` and `AllowUntested` explicitly permit use despite those statuses,
without changing the reported facts or claiming tested support. No policy flag
waives `Incompatible`; `ErrUnsupported` identifies that refusal.
`ErrUnverified` identifies insufficient accepted evidence. These are public
`failure` identities with semantic revision 1, not a wire-format version.

## Data, ownership and limits

These are in-process Go APIs; new aggregates refuse JSON encoding/reconstruction.
They are not approved persisted DTOs, a Temporal failure/history format or an
attestation protocol. No Go errors, clients, contexts or runtime ownership handles
are serialized. Applications requiring a durable format must select its owner,
schema/version, limits, unknown semantics and historical-reader/migration policy.

`FromBuildInfo` borrows input only during the call. It returns fresh metadata.
`Build.Clone` copies slice storage; copying its outer Go struct alone does not.
`Assess` copies build/profile/record/evidence slices and obtains independent source
metadata. Returned reports are caller-owned snapshots, safe for concurrent reads
without mutation. Do not mutate inputs concurrently with inspection/assessment.

Limits: 64 selected/disclosed module paths each, 128 bytes per ordinary token,
256 bytes per permitted module path, 64 options per list, 32 requirements,
128 records, 32 evidence entries and 32 limitation IDs per record, 4 unique layers,
and the 6 defined behaviors. Records must include a limitation reference.
Unknown enum values, duplicate IDs/options/layers and malformed inputs are rejected
with safe errors. These metadata bounds are not process/native-memory guarantees.

Module/public API compatibility, error semantics, Provider configuration format,
source revision, data protocol, Workflow/mode, SDK/service/native version and
build/deployment identity retain their separate owners. No Workflow commands,
converter, payload format or replay behavior changed in #6.
