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

# Build declarations and artifact verification

[Documentation](../../README.md) / [Version interface](interface.md)

**Audience:** application owners and future external build-tool authors.
**Status:** implemented linker/caller seam; no build runner or release pipeline.

## Choose a producer, not an implicit discovery policy

Native Go BuildInfo supplies consuming module/source/toolchain/target metadata.
It does not necessarily supply a product's release label or build timestamp.
A producer can add exactly four main-application declarations:

| Supported linker target | Accepted value |
| --- | --- |
| `github.com/frost-leo/fathomry/version.release` | Complete ordered software version, including prerelease/build suffixes |
| `github.com/frost-leo/fathomry/version.gitRevision` | Full lowercase 40- or 64-hex Git object ID |
| `github.com/frost-leo/fathomry/version.tree` | Exactly `clean` or `dirty` |
| `github.com/frost-leo/fathomry/version.buildTime` | Present UTC whole-second timestamp, `YYYY-MM-DDTHH:MM:SSZ` |

These private string variables are deliberately supported injection symbols, not
exported mutable variables or a SetVersion API. They describe the **application**,
even when Fathomry is a replaced dependency. There is no framework-release override.

The fields are all-or-none. No fields means a valid ordinary unstamped build with
no declared software release. Any nonempty field requires all four valid fields.
An explicit declaration cannot use unknown or development release syntax. Dirty
declarations are allowed but explicitly dirty; valid syntax never confers official
release status. Empty injection values are indistinguishable from absent variables.

`Inspect` validates the declaration on every read. A declared revision/tree must
agree with available native main-source facts; a Git declaration conflicts with a
reported non-Git source. Missing native source data remains unknown, not silently
promoted to reported evidence. The application release label may legitimately
differ from its Go module/pseudo-version. Build time and commit time are different
facts, so they are neither equated nor ordered.

A four-field linker seam was chosen over a stamp document to avoid an unnecessary
durable schema, decoder and migration commitment. A typed caller declaration
(`WithDeclaration`) is useful for prepared/generated inputs but remains marked
`caller`; it is not artifact read-back.

## Build in the consumer module

The application must call `version.Inspect`; a package/symbol omitted by the linker
cannot consume a declaration. From the consumer module root, with four already
prepared, non-secret values:

~~~sh
package=github.com/frost-leo/fathomry/version
go build -trimpath -ldflags "-X $package.release=$release -X $package.gitRevision=$revision -X $package.tree=$tree -X $package.buildTime=$build_time" -o application .
~~~

The variable values above are producer inputs, not values discovered by this
package. Their grammars exclude whitespace, so no extra value-level linker quoting
is needed. Go's `-X` applies to string variables with zero/constant initializers;
compilation alone does not validate the semantic value or prove a target existed.
[Go linker contract](https://pkg.go.dev/cmd/link).

Do not pass credentials, user identities or private build paths in linker flags.
This API redacts raw flags from snapshots; it cannot remove strings that a producer
has embedded elsewhere in the binary. `-trimpath` is not a general secret scrubber.

No Git command, tag collection, current clock, `SOURCE_DATE_EPOCH`, `GITHUB_*`
variable or CI provider is consulted by the package. The producer decides how to
derive the four values. A reproducible epoch is a producer-selected timestamp,
not proof of observed compilation time. No reproducibility claim is made.

## Verify the produced artifact

For a native executable, run a test-owned or future application-owned read-back
entry that calls Inspect and compares all four expected values:

~~~go
build, err := version.Inspect(request)
if err != nil {
    return err
}
return build.CheckDeclaration(expected)
~~~

`expected` is a complete `version.Declaration`. Checking only the release field
misses silently dropped revision/time/tree injection. The expected values must
come independently from the producer, not be reconstructed from the actual claim.

| Outcome | Result |
| --- | --- |
| All symbols absent/misspelled | Ordinary read succeeds without a claim; expected-claim check returns `MissingDeclaration` |
| Some symbols missing/misspelled, others set | Inspect returns `InvalidDeclaration` |
| Invalid version/revision/tree/time | Inspect returns `InvalidDeclaration` |
| Native source contradicts declaration | Inspect returns `Conflict` |
| Complete but unexpected declaration | Expected-claim check returns `Conflict` |
| Complete matching declaration | Equality established, not release authorization or authenticity |

The [independent consumer fixture](../../../version/testdata/consumer/main.go)
demonstrates this read-back as **test transport**, not a shipped CLI or public JSON
format. Its [tests](../../../version/consumer_test.go) verify actual outputs,
including stripped binaries, not just compiler exit status.

For another or cross-target artifact:

~~~go
info, err := buildinfo.ReadFile(path) // standard debug/buildinfo
if err != nil {
    return err
}
build, err := version.FromBuildInfo(info, request)
~~~

The standard reader supports its native executable formats. The public normalizer
preserves artifact-reported toolchain/target data and never fills from the inspector.
The tested Windows/arm64 artifact is read on Linux without execution.
**BuildInfo does not decode the four linker variables.** This path must therefore
return no declaration even for a stamped binary. Raw `-ldflags` is neither parsed
nor trusted as a substitute. Do not attach `expected` with WithDeclaration and call
that successful injection verification. Cross-target declaration verification
requires an appropriate native execution environment or a separate future
artifact-reader contract; neither is supplied here.

Missing VCS data can be normal for archives, disabled stamping or non-repository
builds. Even `-buildvcs=true` does not manufacture a Git checkout. The independent
tests cover that case, clean/modified Git fixtures and conflicting declarations.
Shallow/detached/tag collection policy remains a producer concern.
