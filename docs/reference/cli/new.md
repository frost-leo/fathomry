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

# Local-source project creation

[Documentation](../../README.md) / [CLI interface](interface.md)

**Audience:** developers starting an independent Go project with a local Fathomry checkout.
**Status:** implemented four-file bootstrap, qualified on Go 1.27.0, Linux/amd64.
This is not an application runtime, workflow/Worker template or all-SDK distribution.

## Generate, then explicitly build

Use an existing parent directory and an absent destination:

```sh
fathomry new ./collector \
  --module example.org/collector \
  --fathomry-source ../fathomry
cd -P collector
GOWORK=off go mod tidy
GOWORK=off go mod tidy -diff
GOWORK=off go build -mod=readonly -o ./bin/app .
./bin/app --help
GOWORK=off go run -mod=readonly . --help
```

All three creation inputs are required. `new` creates projects only: there are no
`create`/`init` aliases, project-name option or `new <kind>` namespace. Flags follow
`new` and may be interspersed with its single directory operand. For a directory
starting with a dash, put flags first and use `--` before the directory.

Generation creates exactly `go.mod`, `main.go`, `README.md` and `.gitignore`.
The entry calls the existing public `cli.Main()`; its executable inherits the
compiled commands, including `new`, and the root name `fathomry`. It does not
discover project code or initialize services. No `go.sum`, binary, Git metadata,
empty directories, test placeholders, configuration or workflow files are generated.

Creation runs no Go, Git, formatter or shell subprocess, downloads nothing and
starts no service. The Go commands above are separate user actions and may
download dependencies or a compatible toolchain. Offline generation is not an
offline-build guarantee. Tidy creates the consumer's sums and indirect requirements;
keep both module files in version control. Build creates `bin/app`.

Use `cd -P <directory>` to enter the project, or `cd -P .` if already inside it,
before invoking Go. Go may otherwise preserve a logical shell `PWD` through a
directory alias; aliases at a different depth can make its lexical resolution of
the relative replacement select the wrong source. Generation anchors the binding
to the project's physical directory. The generated README includes this step.

## Module and source requirements

- Module syntax is checked with `golang.org/x/mod/module.CheckPath`. Exact
  collisions with `github.com/frost-leo/fathomry` and its `/cli` import path refuse.
  This is not a graph-wide namespace collision resolver.
- The selected source must have a regular `go.mod` of at most 1 MiB, accepted by
  strict `x/mod/modfile.Parse`, declaring the exact Fathomry module identity and
  `go 1.27.0`. Its `toolchain` must be absent, `default` or `go1.27.0`. A `cli`
  directory must exist. Other Go/toolchain profiles are not yet accepted.
- These metadata checks are not proof that an arbitrary mutable fork implements
  `cli.Main()`. Consumption tests qualify this repository's supported source.
  No source compilation or dependency resolution is performed during generation.
- The emitted `go 1.27.0` comes from this qualified source/template profile, not
  the generator's `runtime.Version()`. Run subsequent Go commands with a toolchain
  satisfying that requirement.
- The seed has one `require github.com/frost-leo/fathomry v0.0.0` and exactly one
  local `replace`. The version is a source-only placeholder, not a released
  version, source checksum or immutable pin.

Relative inputs use one captured physical invocation directory, not the logical
`PWD` alias. Path components are traversed before lexical normalization:
`link/../project` follows `link` before visiting its parent. Intermediate components
must exist and be directories, even when followed by `..`. The existing destination
parent and selected source are resolved before safety checks or creation.

Replacement paths are computed from the physical destination. A normalized source
alias is retained only when it resolves to the same selected source. If cleaning
would change that selection, the replacement binds the resolved physical source
instead; retargeting a removed alias does not update that binding. This is necessary
because Go cleans local replacement paths. Native module formatting quotes spaces
and syntax-sensitive characters, and the complete rendered module is parsed again
before creation. Unrepresentable replacement paths (for example literal backslashes
in Linux directory names) refuse. Cross-volume relative paths that the OS cannot
represent also refuse. On Windows, drive-relative (`C:project`) and root-relative
(`\project`) spellings are rejected; use an absolute or ordinary relative path.

The local source binding is live and unchecksummed; source or symlink changes can
affect later builds. Moving the project or checkout may require editing its
replacement. Neither source files, Go environment files nor `go.work` are changed.
Framework requirements, sums, SDK replacements and `third_party` are not copied.
Dependency-module replacements are not inherited: the tested CLI-only consumer
does not need patched technical SDKs. Adding them requires separate qualification.

## Ignore template

The generated ignore file embeds the CC0-1.0
[GitHub Go template at `b06d69d5a0b8`](https://github.com/github/gitignore/blob/b06d69d5a0b82a187180dac3d46a4ebe1e40bce5/Go.gitignore)
with its comments, immutable provenance and an additional `/bin/` rule. It ignores
binary/test/coverage artifacts, `go.work`, `go.work.sum` and `.env`, not `go.sum`.
The upstream `vendor/` and editor rules remain commented out.

This owner-approved refinement replaces #90's original `/bin/`-only requirement.
The upstream template is embedded at development time, never fetched during
generation. Updating it is a deliberate source change, not an automatic download.
Template notices do not select the business project's authorship or project-wide
licensing policy.

## Effects, refusals and cancellation

Constructors, help and static argument validation do not inspect source paths or
read stdin. Valid execution validates the source, paths and all four rendered
outputs before mutation. The parent must exist, and existing files, empty/nonempty
directories and symlinks (including dangling ones) refuse without modification.
Targets inside the selected source, including resolved symlink-parent aliases,
refuse. Ordinary same-target creators compete through exclusive directory and
file creation; they do not merge.

After directory creation, failed writes/closes, cancellation or process loss may
leave partial output. Relevant errors are preserved for `errors.Is/As`; safe,
resource-backed diagnostics do not print raw paths or error strings. Partial
files are retained. There is no target deletion, retry, repair, merge or upgrade.
Inspect the destination before deciding what to do next.

Completion is reported only after every write and close succeeds. Failed stdout
or late cancellation can return nonzero even with all four files complete; this
does not undo generation. Language selection affects command prose, not the
generated module, Go code, English README or ignore rules.

The supported filesystem profile is trusted existing parents and ordinary
concurrent creation, not hostile pathname replacement, an atomic directory
transaction, crash durability or forced interruption of blocked OS calls.
Other-platform cross-compilation is not runtime qualification.

## Ownership and evidence

The private owner is
[`cli/internal/command/project`](../../../cli/internal/command/project/doc.go);
`cli/commands.go` only composes the leaf. No public generator API, filesystem
Adapter or project-specific branch in shared Run/Main/parser/output/language
machinery is added.

[Actual generated-module acceptance](../../../cli/internal/command/project/acceptance_test.go)
builds the real command, generates from an unrelated directory with source/parent
aliases and spaces, then performs ordinary tidy, stable tidy-diff, readonly
build/run and binary help with `GOWORK=off`. It checks the exact package/build
closure and preserved source files. [Operation tests](../../../cli/internal/command/project/create_test.go)
exercise exclusive writes and controlled partial/close/cancellation failures.
[Public-command tests](../../../cli/internal/command/project/command_test.go) inspect
complete output after delivery failure and concurrent language/module isolation.
The [path regressions](../../../cli/internal/command/project/paths_test.go) preserve
raw `symlink/..` operands and check physical-CWD, source selection, overlap and
existing-target refusals. Their [compiled-command journey](../../../cli/internal/command/project/paths_acceptance_test.go)
also hydrates, builds and runs the genuinely generated projects.
The [earlier consumer](../../../cli/consumer_test.go) still independently exercises
the existing public entries and private-import rejection.
