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

# Local Fathomry project

This is a local-development Go executable using Fathomry's public `cli.Main()`.
It inherits the compiled first-party commands and the root name `fathomry`,
including `new`. It is not a workflow application or Worker and does not discover
project code, configuration, services or SDK integrations.

## Explicit next steps

Enter this project with `cd -P <directory>`, using Go 1.27.0 or a compatible newer
toolchain. If already inside the project, normalize the shell's physical directory
before invoking Go:

```sh
cd -P .
GOWORK=off go mod tidy
GOWORK=off go mod tidy -diff
GOWORK=off go build -mod=readonly -o ./bin/app .
./bin/app --help
GOWORK=off go run -mod=readonly . --help
```

Generation did not execute any of these commands, initialize Git, install a
toolchain, download dependencies or start services. These explicit Go commands
may download dependencies or a compatible toolchain: offline generation does not
guarantee offline builds. Tidy creates `go.sum` and may add indirect requirements;
retain both `go.mod` and `go.sum` in version control. Build creates `bin/app`.
The embedded GitHub Go ignore template excludes binaries, test/coverage artifacts,
workspace files and `.env`; the additional `/bin/` rule covers this build output.
It does not ignore `go.sum`. `app` is just an example binary filename.
Use the corresponding executable filename and environment syntax on other shells
or operating systems; runtime acceptance is initially Linux/amd64.

## Ignore template

The ignore template is vendored from
[github/gitignore at `b06d69d5a0b8`](https://github.com/github/gitignore/blob/b06d69d5a0b82a187180dac3d46a4ebe1e40bce5/Go.gitignore)
under CC0-1.0. Its upstream comments and provenance are retained in `.gitignore`.
Generation never downloads it; upstream changes require an explicit template update.

## Local source contract

The single relative Fathomry replacement is resolved from this project's
`go.mod`, not from the shell working directory. It is a live, unchecksummed
source binding. Source edits affect later builds; moving this project or its
source checkout may require changing the replacement path. `v0.0.0` is a
source-only placeholder, not a published release, checksum or immutable pin.

Go can preserve a logical shell `PWD` through a directory alias and lexically
resolve relative replacements against it. If the alias has a different depth
from the physical project directory, this can select the wrong source. The
`cd -P` step above ensures Go uses the physical location that anchors the binding.

Generation interprets relative paths from the physical invocation directory and
follows symlinks before `..`. A source alias is retained only when normalizing it
still selects the same source; otherwise the replacement uses the resolved physical
source. Retargeting an alias removed during that resolution does not update the
stored binding. Refer to `go.mod` for the actual source path used by later builds.

The generator accepts a regular source `go.mod` of at most 1 MiB declaring
`module github.com/frost-leo/fathomry`, `go 1.27.0`, and an absent `toolchain`
directive or `toolchain default` / `toolchain go1.27.0`. A `cli` directory must
exist. These metadata checks do not certify `cli.Main` compatibility in arbitrary
mutable forks. This template is qualified against Fathomry's issue #90 source
on Go 1.27.0, Linux/amd64, not every same-named module.

Dependency-module replacements are not inherited. This CLI-only entry does not
need Fathomry's patched technical SDK replacements. No framework dependency list,
sums, patches, `third_party`, or workspace settings were copied. Adding SDKs later
requires separate consumption checks; this is not an all-SDK distribution scheme.

## Creation and later changes

Creation requires an absent target and an existing trusted parent. It never
merges, overwrites, repairs, upgrades or deletes an existing target. After creation
starts, failure or cancellation may leave partial files; inspect them before any
manual retry. A confirmation-output failure or late cancellation can leave all
four files complete despite a nonzero exit status. No atomic directory
transaction, crash durability or hostile-filesystem isolation is promised.

Future templates do not auto-upgrade this project. Notices on template-derived
material are preserved; generation does not choose the business project's
authorship or project-wide licensing policy.
