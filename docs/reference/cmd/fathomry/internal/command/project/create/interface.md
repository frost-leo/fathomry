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

# Offline project creation

[Documentation](../../../../../../../README.md) / [Project commands](../interface.md)

**Audience:** developers creating projects and maintainers of the official CLI.
**Status:** implemented first profile; no configuration or workflow runtime.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project/create`.

## Invocation and supported profile

```sh
fathomry project new --name ordersync \
  --module example.com/acme/ordersync \
  --framework-version v0.0.0-20261028081050-b28e37d55e53
```

All three flags are required. There are no positional arguments, inferred versions,
latest lookup, arbitrary destination or overwrite option. The existing root
`--output text|json` selects presentation, never a path.

Name is 1–63 ASCII bytes matching `[a-z][a-z0-9-]*`, also checked for portable
import-path component restrictions. Module is at most 1,024 bytes and passes
the pinned x/mod `module.CheckPath` domain-qualified profile. Do not confuse the
module path with the directory name or require a new business repository to exist.

The module must not equal Fathomry, Cobra, pflag, mousetrap or x/text's exact
module identity. These collisions shadow dependencies; shared prefixes alone are
not refused. Framework version must equal the explicit supported pin above, with
Go floor 1.27.0. This is one qualified template/library pair, not arbitrary-version
compatibility. The command rejects non-Linux operating systems before mutation.

## Exact output and template ownership

The target is exactly ./<name> under the caller's existing cwd:

```text
<name>/
  go.mod
  README.md
  .gitignore
  cmd/<name>/
    main.go
    main_test.go
  config/
    README.md
```

No go.sum initially: resolve dependencies explicitly with Go afterward. No Go/Git
process, network call, Git initialization, parent workspace change, configuration
load, service probe, registration or Worker startup occurs during generation.
The config README documents the absence of a loader/schema/profile.

The main uses existing public cli.Run/ExitCode. The executable/directory use the
project name; shared help still says fathomry. No display-name API or public project
registry is added. Generated source is project-owned and future templates do not
overwrite it.

Templates mirror target directories, with PROJECT_NAME as the fixed cmd component
and .tmpl suffixes. Explicit embed paths and a six-file manifest—not a recursive
runtime copy—select output. Go source is formatted and go.mod identities are checked
before mutation. Total rendered content is bounded to 1 MiB.

The gitignore body is preserved verbatim from
[GitHub's Go.gitignore at 74cf715](https://github.com/github/gitignore/blob/74cf7151d96a6eaacd4f66fd59b01a140036d857/Go.gitignore),
after Fathomry's notice and upstream CC0-1.0/source attribution. It is embedded,
not fetched when creating a project. The exact source hash is a maintained test
control; a future upstream update requires deliberate review.

## Filesystem, cancellation and failure

The caller must control a trusted, stable parent and relevant ancestry/tree on
an ordinary Linux local filesystem. No external rebinding, relocation, mount
change or concurrent tree modification is supported. Permission bits do not
establish this trust. os.Root contains relative access, but Mkdir then OpenRoot is
not an atomic identity claim against hostile namespace mutation.

Competing creators that only attempt the same exclusive target are supported:
one may proceed; losers never open/adopt the existing target. Every expected
directory uses exclusive Mkdir (0755 requested), and every file uses
O_CREATE|O_EXCL|O_WRONLY (0644 requested), subject to umask. Existing directories,
files, links and partial targets are never overwritten, chmodded or recursively
removed. There is no rollback, publication transaction, fsync or crash guarantee.

Creation is serial. Cancellation is checked before claims/new operations, after
writes and before return; observed cancellation stops new work but still closes
handles and reports acknowledged effects. Blocking synchronous syscalls/writers
are not forcibly interruptible. Complete files may coexist with a late cancellation
or root-close/output failure.

## Bounded creation report

After admitted inputs, the leaf uses the shared MutationResult boundary for both
successful and failed creation attempts. The JSON envelope operation is
`project.new`; data contains name, module_path, framework_version, target,
directories, files, state and an optional failed_path.

- target is intended relative to cwd, not an immutable locator after external rename.
- directories contains at most four target-relative acknowledged creations:
  ., cmd, cmd/<name>, config.
- files contains at most six exclusive creations, each with path, bytes_written
  and complete. A file is complete only after its full write and successful Close.
  Empty/partial files remain in the report.
- Lists are empty arrays, not null. The input and fixed manifest bound the encoded
  report to less than 16 KiB before any mutation.
- State is not_started, partial or complete. Summary effect is none or created.
  None is no acknowledged creation by this invocation, not an absent destination.
  Complete concerns file writes/closes, not dependency resolution or output delivery.
- failed_path is a known relative manifest path. Raw OS messages, file contents
  and directory scans are not report fields.

Write/close and report-output causes are retained. A failed output stream does
not undo files; in-process ExecutionError.ReportCopy retains JSON custody. If both
streams fail, an external process may receive only failure status—there is no
durable recovery journal or delivery acknowledgement.

New creation failure codes occupy 0x0101–0x0106 in the reserved project-creation
facility, rather than repurposing the withdrawn command's old numbers. Invalid
inputs use the common usage exit policy; filesystem failures remain failures and
native causes deliberately inspectable.

## Verification and distribution limits

Maintained render/input/filesystem/report tests cover exact template inventory,
pinning, aliases, collisions, exclusive claims, partial writes, close/cancel/output
failures, umask and concurrent creation. Actual-command consumer tests generate
without Go/Git, then resolve from a fresh external module cache, run readonly
vet/race/build, compare successful source/binary help and relocate source/cache
before standalone launch. The Go launcher's failure exit is not the binary's policy.

Only this minimal CLI dependency profile is qualified, not every Adapter.
Versioned go install of the official tool currently rejects the supplying module's
local replacements; use the supported source-checkout build path. No SDK distribution,
new public API, other native OS, network filesystem or hostile-directory qualification
is implied. Scope and preparation provenance: [#163](https://github.com/frost-leo/fathomry/issues/163).
