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
**Status:** fixed command entry, explicit schema operations, project catalog
registration and native build gate are implemented; no Worker, Temporal admission,
deployment or generic plugin SDK.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/v1` (package `cli`).

## One command implementation

The official executable calls `Run` with its explicit full error/message catalog.
A generated project's root main calls `Handle` before its existing application
startup, supplying a `Project` with its name, workflow declaration set and optional
lazy `Control` configuration resolver.
Known commands use the same private engine and handlers, including
`project/new`, `project/build`, `workflow/vet`, `workflow/register`,
`workflow/list`, `workflow/show`, `workflow/compare`, and separate
`schema/status`, `schema/plan`, `schema/apply` leaf packages. No per-command public shim,
second parser or separate management executable exists.

`Handle` returns false for no arguments or an unrecognized first word, without
starting anything. The application then owns its existing arguments. When true,
the application must return immediately, including on error. Known root commands,
`help`, `--help`, `-h`, `--lang` and `--output` are reserved CLI inputs.
The versioned `--fathomry-project=v1` argument is an offline machine protocol,
not a business startup option. Unknown protocol versions fail closed.

## Project workflow checks

`Project.Workflows` is the combined registry: each `cli.Workflow` contains a
`declaration.Declaration` for one Namespace/code (possibly several versions) and
optional `*catalog.Metadata`. Declare each logical workflow once. A nil metadata
pointer means declaration-only, not catalog publication readiness. The legacy
`Project.Declarations` form remains supported; supplying both forms rejects.
Each form admits at most 16 inputs and retains existing declaration/aggregate
budgets. The new form counts logical registration units, while a legacy declaration
input can contain several workflows.

On the project executable, `workflow vet` checks all registrations. `--namespace`
and `--workflow` narrow the set. `--version` requires Workflow, `--mode` requires
Version, and `--revision` requires Mode. Unspecified versions/mode revisions mean
all selected descendants, never latest. A mode check retains its parent specification.
Ambiguous Namespace selection, no match, explicit empty selectors and selectors
combined with Bundle paths reject. Standalone CLI vet still requires explicit files.

Project results use `data.format=fathomry.project-workflow-check/v1`, report the
selection and full_project flag, total/selected/omitted workflow counts and
per-workflow declaration and metadata outcomes. Metadata is not_supplied,
not_checked, passed or failed; absence never claims metadata validation. Diagnostic
identities identify versions/modes; paths do not expose declaration values.
One failed workflow does not suppress checks for other valid registry entries.
Malformed registry structure and operation/input-budget failures remain errors.

Empty project vet is an explicit failed zero-work check. Configuration-only build
remains valid. Build's offline inspection always validates all registrations and
supplied metadata before emitting bundles; it accepts no selection parameter.

One private selected-analysis pass produces the checked declarations and prepared
metadata. Report-only vet does not retain build/publication outputs; registration
and comparison reuse the prepared snapshots. Build export preserves original-source
order and grouping. Mixed legacy sources still require full-source admission even
when their logical groups pass separately, so grouping cannot bypass the per-source
bundle envelope. No cache or second validator is introduced.
Selected success cannot authorize publishing an otherwise invalid artifact.
The inspection wire format remains v1; metadata admission happens in the project's
shared entry before export. This is not an attestation of arbitrary project code.

No project vet, metadata preparation, help or build path invokes Control or Boot.
`go run` still compiles the project; selectors do not hide Go compiler errors.

`Options` borrows streams, context-independent display preferences, and optional
prepared catalogs. All three streams are required. Empty language means English.
A nil Errors/Messages pair selects core plus fixed-command metadata; supplying only
one catalog is invalid. `Components` supplies command metadata for explicit wider
catalog assembly. The public entry imports no concrete Adapter catalog, so using
it does not compile every Adapter into the project. This is not arbitrary command
registration, a service locator, or an application host.

## Register project workflows

`./project workflow register` selects all project workflows by default. The exact
namespace/workflow/version/mode/revision selectors follow vet's rules, including
parent retention and ambiguity refusal. No Bundle input, project scanning or
second workflow list is introduced. Declaration-only entries need catalog Metadata
before publication; an empty selection/project is an error, never a successful no-op.

All selected declarations and metadata are checked and copied before Control runs
or any database resource is acquired. One invalid selected workflow prevents every
database write. Selecting a valid subset does not qualify omitted workflows or
weaken full-project build inspection.

The separate register leaf owns flags/localization/presentation; shared workflow
preflight remains offline. Private control.go composes the existing Resource,
Adapter and catalog API, not another database owner. Schema/create do not depend
on catalog execution. Registration checks current schema and never auto-installs it.

The JSON envelope uses command workflow.register and
data.format=fathomry.project-publication/v1. It includes scope, total/selected/omitted,
checked/attempted/complete, and bounded per-workflow diagnostics/results. A missing
per-workflow result means unattempted, not successful. Full vet remains available
for detailed declaration diagnostics; executable documents and metadata text/tags
are not emitted in registration reports.

Each workflow aggregate is one transaction, not the whole project. A later failure
does not roll back earlier acknowledged aggregates; the batch stops and reports
partial or unknown with individual outcomes intact. Lost COMMIT acknowledgement
stops the batch and never triggers automatic replay. The
[catalog contract](../../../framework/workflow/catalog/v1/interface.md)
defines native acknowledgement, committed counts and retained cleanup ownership.

Known effects use the existing mutation-output custody path even on error. A failed
stdout write is not rollback or delivery success: ExecutionError.ReportCopy retains
the bounded JSON report, and its Close/CleanupComplete preserve overall invocation
cleanup ownership. Catalog result cleanup concerns its transaction; source/runtime
cleanup remains a separate invocation responsibility.

## Runtime control-database binding

`./project workflow compare` compares all locally authored workflows with stored
state. The namespace/workflow/version/mode/revision selectors match vet/register;
all selected declarations/metadata pass the same offline preflight before Control.
It never opens a separate registry, reads an ambient Bundle, or publishes data.

The existing CLI envelope contains data.format=fathomry.project-comparison/v1,
selected/omitted counts and per-workflow validation/comparison results. Completed
missing/different selections emit the report and return the existing failed-check
policy (nonzero), while corrupt/unsupported/unavailable storage returns an execution
error rather than a completed non-match. Ordinary read/check output gating applies.
Workflows are independent read observations, not one cross-project snapshot.

`./project workflow list --namespace NS` lists definitions; --workflow lists
versions, --version lists modes and --mode lists revisions. --code, --category
(definitions only) and explicit --enabled/--enabled=false are exact filters.
--limit accepts 1..64 and --cursor resumes the same owner/filter/locale scope.

`./project workflow show --namespace NS --workflow CODE` reads a definition;
version/mode/revision select exact descendants. --document opts into verified
canonical bytes for a version or complete mode revision, never an inferred default.
JSON canonical document fields use base64. --locale chooses database display text,
independently of global --lang for CLI messages.

These read commands require Control but do not require local declarations or
registration metadata: a restarted configuration-only application can inspect the
stored catalog. Shape/locale validation precedes Control. Help remains offline.
No source, pool or schema policy is constructed in either leaf; ordinary read-output
gating applies, including refusal to emit successful data after cleanup failure.

`Project.Control` returns validated data and released configuration records,
never a client or a project-owned runtime. Direct commands and commands preceded
by `--lang`/`--output` retain the same Project. Help, vet, build and machine
inspection never invoke this resolver. Standalone online commands without it
fail explicitly; there is no ambient project scan or implicit target discovery.

Private `v1/control.go` owns this entry composition. It registers fixed target
and, only for creation, administrative Handles in the existing Resource scope.
Adapter Using clients borrow those instances; source construction is confined to
binding Build callbacks and source Release callbacks stay with Resource.
Schema, creation and catalog execution use these clients, not direct Owner.Client handles.
Leaf presentation receives typed operation callbacks and no pool shutdown authority.
There is no `control` command, parallel database manager or command-plugin API.

Common database diagnostics, operation bounds and physical-target admission belong
to the existing private `internal/command/database` package. Its namespace does
not import a command executor; this entry attaches the create leaf, just as it
attaches schema/workflow leaves. Creation does not depend on the schema executor.
Each operation supplies its own workspace charge. Workflow publication/comparison
files own batch sequencing, partial/unknown outcomes and aggregate reports through
borrowed operation callbacks; this entry retains resource cleanup and final output
gating rather than duplicating batch policy.

Generated `internal/setting/database.go` declares `database.fathomry`; it contains
no deployed values. `internal/bootstrap/control.go` performs one finite Boot
using existing project environment/location inputs. `setting.go` remains the
aggregate. The project-owned `setting.FathomryDatabase` specifies exactly one
dialect/provider database and, for PostgreSQL only, a physical schema. Its data
declaration uses public provider Settings and offline Validate only, never clients,
pool constructors or private packages. The finite Control callback copies these
values into the in-process `DatabaseConfiguration` handoff, with at most 128
released/resolved configuration records. The handoff itself is not a durable DTO.
Credentials and addresses belong in protected runtime configuration, not Go source.

The generated project is the application host; Fathomry is a library compiled into
the same executable, not a second database service. CLI and application paths use
the project's configuration model but acquire only the resources they need. Separate
process invocations do not share connection pools or start a Worker for migration.

`./project schema status|plan|apply` uses that binding. Status/plan are read-only;
apply explicitly installs a positively empty target or verifies a current no-op.
Neither creates a physical database/schema. Missing/unmanaged/incomplete targets
are not adopted. Resource construction and normal project startup never auto-migrate.

`./project database create --admin-config FILE` is the separate explicit
[physical creation operation](../internal/command/database/create/interface.md).
It uses the same selected endpoint and separate administrative authentication,
then verifies access with the target role. It does not install tables, grant
privileges, adopt an existing database or replay an unknown mutation.

Schema apply has a separate mutation-output path. Known effect/state can be
emitted alongside execution/cleanup errors; ordinary offline Result gating remains
unchanged. Failed output may be partial and is not retried. A returned
`*ExecutionError` (use `errors.As`) retains a detached JSON `ReportCopy`, original
failure identity/causes, and `Close(ctx)`/`CleanupComplete()` for local cleanup
continuation. Completed cleanup never rewrites original effect or error evidence.
The JSON diagnostic adds bounded `mutation.effect`/`mutation.state` machine facts.

For build-once/deploy-many use, reuse the same supported-platform artifact and vary
external configuration. Runtime schema commands require neither Go nor source.
A compatible pure-Go project can request `CGO_ENABLED=0 go run . build`; the
existing actual-artifact inspection gate still runs. This is not forced on projects
that require native libraries. Such projects need an explicit runtime-library
baseline or image. Cross-platform variants need separate native qualification;
containerization does not erase OS, architecture or ABI constraints.

For PostgreSQL, explicitly align the container's HOME with the selected
`parser_home` authorization. The directory need not exist or be writable; for
example, a scratch image can use `HOME=/nonexistent` and matching runtime
`parser_home`. Do not assume that requesting an empty HOME prevents the container
runtime from supplying a default. Mismatch is an environment refusal, not a reason
to disable the provider's ambient-input protections.

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
