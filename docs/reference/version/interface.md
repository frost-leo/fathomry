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

# Software version and build-information interface

[Documentation](../../README.md) / Public package reference

**Audience:** independent Go applications, framework maintainers and future builders.
**Status:** implemented in-process contract; no release, CLI or deployment platform.
**Package:** `github.com/frost-leo/fathomry/version`.

## Responsibilities and call sequence

Use `Parse` for software version identity/precedence. Use `Inspect(Request)` for
the consuming program, or standard `debug/buildinfo.ReadFile` followed by
`FromBuildInfo` for another artifact. Inspect validates the optional
[linker declaration](injection.md); file normalization never reads the inspector's
declaration. `WithDeclaration` explicitly adds a caller claim without overwriting
reported facts. `Build.CheckDeclaration` compares a complete expected claim.

These operations do not manage business Workflow/run-mode versions, infer
compatibility, run Git, read CI variables, discover a clock, build software or
authenticate provenance. Reading a label is not release approval.
[Version ownership](../../architecture/versioning.md) explains the separate axes.

## Software values and timestamps

| Input or relation | Meaning |
| --- | --- |
| Empty string / zero `Version` | Unknown, not `v0.0.0` |
| Exact `(devel)` | Development, not an ordered version |
| `vMAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]` | Complete SemVer 2.0 syntax, mandatory lowercase `v`, maximum 128 bytes |
| `Version.String`, `Equal` and Go equality | Exact spelling, including case-sensitive prerelease/build metadata |
| `Compare` | SemVer precedence; build metadata is ignored; either unknown/development operand returns `Unordered` |
| Empty `Timestamp` | Absent |
| Present timestamp | Exactly `YYYY-MM-DDTHH:MM:SSZ`, UTC whole seconds, years 0001–9999 |

Shorthand, whitespace, leading numeric zeros, empty identifiers and invalid SemVer
characters fail. Numeric prerelease identifiers cannot have leading zeros; build
identifiers may. Large numeric components compare without machine-integer overflow.
The existing `golang.org/x/mod/semver` parser/comparator is reused after enforcing
the complete profile; its shorthand acceptance and metadata-dropping
canonicalization are not this API's identity contract.
[SemVer rules](https://semver.org/),
[x/mod v0.38.0](https://pkg.go.dev/golang.org/x/mod@v0.38.0/semver).

`Release` classifies ordered syntax, not an official release. Go pseudo-versions
also satisfy that syntax. A matching version, Git revision or checksum does not
establish identical artifact bytes. Build metadata distinguishes software version
identity but does not establish compatibility or chronological ordering.

Timestamp offsets, fractional seconds and leap seconds are rejected rather than
rounded. Unix epoch zero and year 0001 are present instants; use `Timestamp.Time`'s
presence result, not `time.Time.IsZero`, to distinguish absence. Source commit time
and producer-selected build time have different owners; no ordering is imposed
between them and no current-time default is invented.

## Build and component ownership

`Build` is an immutable handle. `Snapshot` separates:

- `Main`: the consuming main module, not the framework's own checkout.
- `Framework`: the actual linked `github.com/frost-leo/fathomry` module. When the
  framework is the main module, both identify that same module.
- `Dependencies`: only the requested contributing modules, sorted by path. An
  unused requirement need not contribute packages to a binary.
- `Source`: **main-source only**, never copied into dependencies or replacement
  checkouts. Git revisions are full lowercase 40- or 64-hex object IDs. Short IDs
  and uppercase IDs fail. Supported non-Git systems (`hg`, `svn`, `bzr`,
  `fossil`) retain their system, tree and time but redact unsupported revision
  spellings. Unknown systems and orphan source fields fail.
- `Go` and `Settings`: reported toolchain and selected target/build settings, not
  the runtime host. A development toolchain is classified without exposing its
  arbitrary development description.
- `Declaration`: a separately validated, complete application claim with
  `caller` or `linker` origin. Its release label never replaces a native module
  version; its build timestamp never replaces source commit time.

`Metadata` means a BuildInfo value was supplied, not that every field was present.
`Origin` distinguishes `runtime` from `supplied`; neither proves authenticity.
Even artifact metadata can be forged. `Present=false` means no unambiguous selected
entry was available, not proof of absence from the source graph. Such requested
paths use `Requested` evidence, not `Reported`. Empty native module versions stay
unknown; explicit `(devel)` is development.

`TreeState` is unknown/clean/dirty. A missing boolean is not clean. A native clean
main tree says nothing about local replacements, CGO libraries, signatures or
deployment. No branch, tag or source revision is inferred from a module version.

`Module.Version` remains the selected module version even when replaced.
`Replacement` separately reports the actual versioned module or a local
development replacement. Local paths are always discarded. Module `Sum` values
are bounded canonical Go `h1:` checksums when available, otherwise unknown/redacted;
they are not executable hashes or independently verified provenance.

## Disclosure and bounds

Selection and disclosure are explicit:

~~~go
request := version.Request{
    Dependencies: []string{"example.org/selected"},
    DisclosePaths: []string{"example.org/application", "example.org/fork"},
}
build, err := version.Inspect(request)
~~~

The framework path is always eligible. A selected dependency path is eligible too.
Main and versioned replacement paths require explicit disclosure unless otherwise
selected. Local filesystem paths can never be authorized by this list. Unselected
dependency inventories, raw flags, build environment and module/package filesystem
paths do not appear in snapshots or errors. Scalar grammar is not a secret detector:
callers must select only module identities they are permitted to disclose.

The existing private build normalizer owns module/replacement/path/checksum and
platform projection. The public adapter adds strict relevant-input validation,
source time and declarations without exporting private assessment types or
altering Provider assessment.

- Each request list: at most 64 entries, each valid module path at most 256 bytes.
  Duplicates normalize to one selection but still count toward the input limit.
- Native input: at most 4096 dependency entries and 256 settings; at most 1 MiB
  aggregate text across toolchain/package path, main/dependencies, one replacement
  per module, and setting names/values. Nested or cyclic replacements fail.
- Duplicate relevant module identities or settings fail with `Conflict`, even
  when their text matches. Unselected module contents are not semantic acceptance
  inputs, but still count toward bounds.
- Relevant module versions use the software profile above. Malformed relevant
  versions, source facts, toolchains and settings fail rather than becoming a
  release. Missing values remain permissible.
- Settings use the private normalizer's allowlist: `GOOS`, `GOARCH`, `GOAMD64`,
  `GOARM`, `GOARM64`, `GO386`, `GOMIPS`, `GOMIPS64`, `GOPPC64`,
  `GORISCV64`, `CGO_ENABLED`, `GOFIPS140`, `-compiler`, `-race`,
  `-buildmode`, `-asan`, `-msan`, `-cover`, `-trimpath`.
  Only bounded 128-byte ASCII letter/digit/`-._+` tokens are projected.
  Rich comma-separated ARM variant values are not exposed by that normalizer.
  An omitted setting is unknown, not disabled; this is not an exhaustive target
  feature or native-library inventory.

`FromBuildInfo(nil, request)` succeeds with missing facts. Neither this function
nor Inspect fills missing toolchain/target fields from `runtime.Version`,
`runtime.GOOS` or `runtime.GOARCH`. A supplied metadata value is borrowed, not
authenticated. The bounds above apply after the standard library has decoded a
binary; they are not a hostile-executable parser sandbox or a file-size guarantee.
Callers own file acquisition, native-reader errors, resource limits and close duties.

## Ownership, errors and compatibility

Inputs are borrowed only during each synchronous call; do not mutate request
slices, BuildInfo settings/modules or replacement pointers concurrently.
Returned strings own retained bytes. Build and Version copies support concurrent
reads. Each `Build.Snapshot` call owns new dependency/setting slices; copying an
already-returned Snapshot shares those slices and does not authorize concurrent
mutation. No goroutines, connections, timers or open files are retained.

Failures return zero results, not partial release claims. Match the stable
`failure.Code` values using `errors.Is`: `InvalidVersion`, `Unordered`,
`InvalidTimestamp`, `InvalidRequest`, `InvalidMetadata`, `LimitExceeded`,
`InvalidDeclaration`, `MissingDeclaration`, `Conflict` and
`SerializationUnsupported`. Occurrences have no raw native causes or input
diagnostics, preventing errors from becoming a disclosure path.

Structured records, Version and Timestamp explicitly refuse JSON marshal/unmarshal.
There is **no persisted record/stamp schema**; nil-pointer JSON behavior is not a
round-trip promise. Consumers must design their own explicit transport projections.
The lexical version profile, Go API, linker symbols and
[presentation resource contracts](presentation/interface.md) are distinct public
compatibility obligations. Changes to identity, missing-state semantics or
injection validation require an explicit compatibility decision.

## Executable evidence

[Version tests](../../../version/version_test.go) cover lexical boundaries and
precedence; [build tests](../../../version/build_test.go) cover normalization,
conflicts, ownership and limits. The
[independent consumer test](../../../version/consumer_test.go) packages actual
product files into a local module proxy with an isolated module cache, uses only
public imports, runs native binaries and reads actual native/cross-target artifacts.
It also verifies local framework and versioned dependency replacements.

~~~sh
go test -race -count=1 -timeout=6m ./version/...
~~~

The native execution witness is Linux/amd64 on Go 1.27.0; Windows/arm64 is a
cross-built file-read witness, not Windows runtime acceptance. Tests use disposable
local Git fixtures, not the product repository's tags or release state.
No general reproducibility, CGO/native dependency or release-attestation
qualification is claimed. Scope: [Issue #75](https://github.com/frost-leo/fathomry/issues/75).
