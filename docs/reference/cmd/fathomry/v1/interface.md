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

# Public CLI entries

[Documentation](../../../../README.md) / Public package reference

**Audience:** official-tool and compiled-project entry authors.
**Status:** implemented minimal shell and project configuration-check profile.
The generator retains its existing shell; integration of the new profile follows
publication of this API as an independently consumable module revision.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/v1` (package `cli`).

## Shared entry and command structure

`Run(ctx, args, options)` retains the minimal help/language/output shell.
`RunProject(ctx, args, project, options)` adds `config check` for the supplied
compiled schema. Both use the same private invocation engine. Project configuration
follows the existing command-group/leaf layout in
[config](../../../../../cmd/fathomry/internal/command/config/config.go) and
[check](../../../../../cmd/fathomry/internal/command/config/check/command.go).
There is no parallel public command tree, process proxy or alternate cleanup owner.

The [official tool](../interface.md) separately composes its private project creator.
It cannot load arbitrary later Go declarations. A project binary installed/named
fathomry uses these same library commands; the library version is compiled into it.
Use root-package `go run .` during development and the compiled binary for exact
automation exit codes. Go run has its own launcher status.

## Caller inputs

Supply non-nil streams and a caller-owned context. Callers synchronize shared
writers. Optional catalogs must be supplied together and contain the selected
entry's metadata. `Components` describes the minimal shell; `ProjectComponents`
describes the explicit configuration profile, excluding `i18n.CoreComponents`.

`Project[T]` supplies a non-nil configuration.SchemaFactory, which runs once after bootstrap
admission with the selected environment and returns a nonzero schema and a bounded,
pure validator. The factory is startup-budgeted and must not create resource owners.
It is not invoked for help or invalid syntax. Other inputs are
configuration-loading policy, optional allowlisted LookupEnv and already captured application
configuration.Variable bindings. The application declaration is a Framework
contract; project code does not need the technical configsource preparation types.
Lookup is synchronous/non-panicking and called once for each bootstrap
key only when config check executes. Nil authorizes no process overrides.
Inputs may not be concurrently mutated; validators and evidence callbacks remain
borrowed until actual cleanup completes.

The public package now imports the configuration profile. Selecting minimal Run
does not promise a provider-free dependency closure, but it still performs no
configuration acquisition. Metadata/command construction and help are offline.

## Configuration check

The command supports `--bootstrap` (default ./.env), `--env`, and the shared
language/output flags. Dotenv paths are resolved once; the
[configuration owner](../../../framework/configuration/v1/bootstrap.md) owns all
literal bootstrap rules, source restrictions and application validation.

The leaf uses the existing Invocation Scope, Managed and CheckResult methods.
Framework binds the configuration capability; Resource owns construction,
borrowing and cleanup, including failed construction. No Runtime/Inbox/session
lifecycle is implemented in the CLI leaf.

The selected first snapshot supplies safe output: source mode/provider/environment,
document kinds, schema version, application name and locale. Application name is
a nonempty ASCII identifier of at most 128 bytes; application/i18n must have the
exact existing i18n.Preferences type. Normal text uses that snapshot's locale unless
explicit --lang or Options.Language overrides it. Command diagnostics retain the
shared CLI language policy, including safe early diagnostics before data exists.

Watch=true uses the real observer initial batch and closes when the finite command
ends. It is not changed to false. Configuration acceptance is not service readiness.
No source path, endpoint, credentials or complete config is printed.

Normal results are staged and emitted only after successful cleanup by the existing
executor. They do not contain a prematurely encoded cleanup-complete assertion.
On failure, original causes and the Scope continuation remain reachable through
ExecutionError. If configuration was already accepted, ConfigurationCheckError
also retains its exact data capability through Configuration().InitialView().
The command obtains a read-only accepted-result Reader before Apply, so cancellation
at the post-adoption lease boundary cannot discard already accepted data. A
cancellation error at that admission boundary still fails without success output.
If cancellation wins the initial publication fence, no accepted result is created.
This preserves the shared CLI completion policy: a later cancellation does not,
by itself, retract an already completed check.
Use errors.As to obtain either value; retaining data does not create another
cleanup owner or replace the original failure occurrence. The implementation is
owned by the existing config/check leaf and exposed through the public entry.
ReportCopy retains mutation-report semantics: this read-only check does not
manufacture a mutation report or durable journal. Output/custody retries do not
reacquire sources, and explicit access to retained configuration may expose secrets.

## Cleanup and compatibility

Minimal Run defaults its outer cleanup budget to 5s. RunProject defaults its outer
Scope/wait ceiling to one minute; the configuration binding uses its early or
accepted-bootstrap inner budget. An explicit smaller outer ceiling constrains it.
Budgets are cooperative, not forced termination of native work.

ExecutionError.Close continues the same Resource scope; CleanupComplete observes
actual completion independently of the original error. ExitCode retains the
existing policy. No settings default, global locale or process environment is set.

Project.Configuration supplies the configuration-loading Policy. This entry
selects commands; it is not the formal application-start operation.

The generic Project[T] is a narrow configuration declaration, not restoration of
the removed Workflow/Handle/catalog/build-inspection APIs. There is no command
plugin registry or application/Worker host.

## Evidence and limits

[Public entry tests](../../../../../cmd/fathomry/v1/project_test.go) cover local
finite/Watch paths, validation, overrides, privacy, output failure and required
evidence cleanup continuation. [Independent consumer](../../../../../cmd/fathomry/v1/project_integration_test.go)
builds a hand-authored root project against the candidate, then runs its binary
without source/Go at the original location. It is not generated-output or
published-module distribution acceptance.

The existing [minimal-entry tests](../../../../../cmd/fathomry/v1/cli_test.go)
and [invocation tests](../../../../../cmd/fathomry/internal/command/run_test.go)
continue protecting the unchanged CLI foundation. The
[actual generated consumer](../../../../../cmd/fathomry/internal/command/project/create/consumer_linux_test.go)
continues qualifying the existing six-file shell against its exact published
dependency, including cold resolution and standalone execution. The new root-entry
generator and its published-module acceptance are a subsequent integration;
the independent API fixture above is not a substitute for that gate. Production
Nacos and real Kubernetes remain outside these claims.
