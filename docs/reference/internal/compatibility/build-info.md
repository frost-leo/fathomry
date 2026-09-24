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

# Consuming-build facts

[Documentation](../../../README.md) / Internal package reference

**Audience:** integration and build-evidence maintainers.
**Status:** implemented metadata inspection, not attestation.
**Package:** `github.com/frost-leo/fathomry/internal/compatibility`.

Start with the [compatibility interface](interface.md). Requirements, linked
packages and verified behavior are different evidence.

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
deployment, or all shared-code effects on a Workflow. Workflow/deployment execution consistency remains a separate framework obligation.

## Executable inspection boundaries

The [consuming-build test](../../../../internal/compatibility/consumer_test.go) executes
synthetic SDK behavior in an independent application's binary, then uses
`debug/buildinfo.ReadFile` on that **same binary** and private `FromBuildInfo`.
Reading the parent test executable would confuse the actual consumer with its
inspector. Fathomry is deliberately required/replaced but contributes no public
package, so its consumer metadata must be absent. This is not real linked-framework
dependency coverage; constructed BuildInfo tests cover dependency normalization.
A separate in-module probe executes `Inspect` directly with Fathomry as main and
VCS stamping enabled/disabled. Neither test
requires an external application to import private machinery. Their selected JSON
output is test transport, not a public diagnostic serialization contract.
