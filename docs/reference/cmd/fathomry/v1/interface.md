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

# Shared CLI and project build entry

[Documentation](../../../../README.md) / Public package reference

**Audience:** independent project authors and official CLI maintainers.
**Status:** implemented fixed command entry and native build gate; no Worker,
Temporal admission, remote registration, deployment or generic plugin SDK.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/v1` (package `cli`).

## One command implementation

The official executable calls `Run` with its explicit full error/message catalog.
A generated project's root main calls `Handle` before its existing application
startup, supplying a `Project` with its name and workflow declaration set.
Known commands use the same private engine and handlers, including
`project/new`, `project/build`, and `workflow/vet`. No per-command public shim,
second parser or separate management executable exists.

`Handle` returns false for no arguments or an unrecognized first word, without
starting anything. The application then owns its existing arguments. When true,
the application must return immediately, including on error. Known root commands,
`help`, `--help`, `-h`, `--lang` and `--output` are reserved CLI inputs.
The versioned `--fathomry-project=v1` argument is an offline machine protocol,
not a business startup option. Unknown protocol versions fail closed.

`Options` borrows streams, context-independent display preferences, and optional
prepared catalogs. All three streams are required. Empty language means English.
A nil Errors/Messages pair selects core plus fixed-command metadata; supplying only
one catalog is invalid. `Components` supplies command metadata for explicit wider
catalog assembly. The public entry imports no concrete Adapter catalog, so using
it does not compile every Adapter into the project. This is not arbitrary command
registration, a service locator, or an application host.

## Build a project

After dependency preparation, run either:

```sh
fathomry build /path/to/health
# Or, from the generated project:
go run . build
```

The root package must be a native main in a directory containing a regular
`go.mod`. No filename scan, inherited old exporter, or existing bundle file can
satisfy the inspection step. Projects generated before the root-entry change need
to adopt the public entry before using this command.

| Option | Meaning |
| --- | --- |
| `PROJECT` | Root project directory; defaults to the current directory |
| `--out FILE` | Artifact file, relative to the project directory; defaults to `bin/<declared-name>` |
| `--timeout DURATION` | Total deadline; default/zero selects 10 minutes, otherwise 1 ms–30 minutes |
| `--lang LOCALE` | CLI display language, independent of application configuration |
| `--output text\|json` | Restrained terminal banner or the usual `fathomry.cli/v1` envelope |

The build path:

1. Checks native target selection and root-module presence.
2. Compiles `.` to a unique temporary executable with `GOWORK=off`, empty
   `GOFLAGS`, `-mod=readonly`, `-buildvcs=false`, and `-trimpath`.
3. Runs that executable's bounded offline inspection, without application Boot.
4. Revalidates every returned bundle through the existing
   [declaration core](../../../framework/workflow/declaration/v1/interface.md)
   and compares supplied identities across the full returned set.
5. Checks that the executable did not change during inspection and stages identical
   bytes beside the output. Only successful admission permits final rename.

No second compilation occurs after inspection. The result identifies the published
file, its SHA256, native target, workflow/mode counts and declaration entries.
The digest describes executable bytes, not the workflow declaration fingerprint.
An empty project is permitted for configuration-only skeletons and reports zero
workflows; it does not claim that a workflow was checked.

## Ownership, bounds and limitations

Current execution support is native Linux/macOS; cross-target and other-host builds
reject. Linux process behavior is tested separately from cross-compilation checks.
The builder owns process groups for Go and inspection, bounds output, limits
inspection to 15 seconds within the total deadline, and uses a one-second pipe
wait bound. It requests group termination on cancellation and exit, retaining
the unreaped group leader until group signaling is disabled. The host must not
reap these children or enable automatic child reaping (such as ignoring
`SIGCHLD` or setting `SA_NOCLDWAIT`). Trusted code must not escape its process
group or intentionally leave background work. Filesystem calls and OS process
termination remain subject to native scheduling and I/O; the deadline is not
a hard wall-clock return guarantee.

The inspection envelope admits at most 16 bundles, 64 MiB aggregate bundle bytes,
and 4 KiB envelope allowance. Each bundle retains the declaration core's tighter
limits. Go diagnostics have a 64 KiB bound per stream; oversized output fails.
Executables are regular, executable files limited to 1 GiB. Native compiler causes
and independently observed output-limit failures remain inspectable through the
error chain even when caller cancellation wins; default CLI diagnostics do not print
compiler stderr, source contents, environment variables or private payloads.

Only trusted source is supported. Go package initialization runs before main:
it and declaration assembly must be free of service/business side effects.
This mechanism is not a sandbox and cannot prove that every executable handler
has a corresponding declaration. Keep declarations deterministic; do not choose
different definitions based on invocation arguments, environment or current time.
Do not modify source during Go compilation or concurrently mutate output paths.
The build does not create an immutable filesystem source snapshot.

The result is an **artifact-publication gate**, not semantic validation before
any Go compilation. Native `go build` does not invoke it. It does not enforce
Temporal registrations, replay compatibility, remote identity conflicts or worker
permissions. Runtime handlers and deployment admission remain separate work.

## Failure and publication boundaries

Failed compilation, malformed/unsupported inspection, invalid/conflicting
declarations, timeout, cancellation or pre-publication file errors cannot publish
a new artifact. Previous output is not removed at build start. The final output
must be absent or a regular file; a final symlink, directory or source alias rejects.
Existing regular outputs are atomically replaced on supported hosts after checking.
The new file preserves the freshly compiled executable's ordinary permission
bits, including restrictions imposed by the caller's umask; set-ID and sticky bits
are not copied. Existing destination permissions are not inherited.
Serialize builds targeting the same output; no cross-process locking is provided.

Publication is one file rename, not a crash-durable multi-file transaction.
A failure after rename, such as cleanup or output delivery failure, may leave the
new artifact present. The shared CLI output-error diagnostic reports delivery
failure, not whether publication completed; inspect the destination rather than
assuming that a nonzero exit left it unchanged. Build errors belong to facility
`0x00B`, component `project_build`; shared invocation/output errors retain the CLI
facility. No raw error is treated as a completed successful check.

## Executable contracts

- [Public entry and shared dispatch](../../../../../cmd/fathomry/v1/cli.go).
- [Public invocation equivalence and offline inspection tests](../../../../../cmd/fathomry/v1/cli_test.go).
- [Native build, process lifetime and unchanged-artifact integration](../../../../../cmd/fathomry/internal/command/project/build/integration_test.go).
- [Declaration inspection, conflicts and fuzz seeds](../../../../../cmd/fathomry/internal/command/project/build/inspection_test.go).
- [Localized resources and terminal presentation](../../../../../cmd/fathomry/internal/command/project/build/command_test.go).
- [Artifact publication, symlink and alias controls](../../../../../cmd/fathomry/internal/command/project/build/artifact_test.go).
