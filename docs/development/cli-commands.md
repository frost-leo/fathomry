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

# Adding first-party CLI commands

[Documentation](../README.md) / Development

**Audience:** Fathomry maintainers implementing an approved command feature.
**Status:** executable maintainer procedure for the root/help foundation.
No custom-command, plugin or public registration API is provided.

## Place the feature with its owner

1. Implement a cohesive family in `cli/internal/command/<family>/`, with its
   `doc.go`, native command construction, operation handlers, adjacent tests and
   owned text resources. Do not create this tree before a real feature is approved.
2. Import and explicitly compose a new top-level family in
   [cli/commands.go](../../cli/commands.go). Keep that file as wiring, not business
   logic. Family packages must not import their parent `cli` package.
3. Add subsequent leaves within the existing family. Root composition normally
   does not change. Neither addition should require feature branches in Run,
   Main, checked output, status handling or language machinery.
4. Extend the narrow package-inventory guard for actual new private family paths;
   do not allow arbitrary public packages or remove independent private-import and
   technical dependency-direction checks.

The [separate test family](../../cli/internal/testdata/commandfamily/commands.go)
and [A/B test](../../cli/run_test.go) exercise this boundary. The same host rejects
the omitted family and executes it when composed. A family-owned leaf addition
leaves root composition and all shared mechanics unchanged. Test fixtures are not
shipped or advertised as domain functionality.

## Construct native metadata, acquire only during execution

Use native Cobra commands and pflag flags rather than introducing another command
description language. Constructors accept ordinary typed dependencies and create
fresh metadata; they do not read stdin, load configuration, open connections or
perform operations.

- Every command has an explicit `Args` function, including groups. Use native
  validators where possible. Custom flag values and validators must be local/static.
- Root and groups must be non-runnable; they render help after rejecting residual
  operands. Only leaves use `RunE`. A runnable root or a command combining `RunE`
  with children is rejected as an invalid definition, even for help. No native
  `Run`, pre/post hooks, disabled flag parsing or unknown-flag
  allowlists are supported. Required and native group checks run before `RunE`.
- Routing root/groups define options with `PersistentFlags`, not local `Flags`.
  Identical pointers already merged by native metadata helpers are allowed. Leaves
  can define local options. Do not pre-parse either FlagSet in a constructor;
  parsing and interspersed-option policy belong to Run. Both legacy and current
  unknown-option allowlists are rejected.
- No `ExecuteC` lifecycle, native global initializer/finalizer or automatic version/
  completion command is installed. Reserve `help`, `completion`, `__complete`,
  `__completeNoDesc` names/aliases and `help/-h`, `lang` flags.
- Sibling names/aliases and effective inherited long/shorthand flag collisions
  refuse deterministically. Repeated identical flag pointers from native merging
  are not collisions. Native duplicate registration within one FlagSet can panic:
  this is trusted constructor code, not a sandboxed extension boundary.
- Native alias normalizers must be pure, name-only functions preserving all stored
  own/inherited canonical flag names and `help`/`lang`. Compatible input aliases
  remain usable; inherited renaming and inconsistent name/key state are rejected
  before native merging can mutate shared flags or silently shadow them. Both
  the parsing FlagSet and Cobra's saved command-wide normalizer are checked.
  Stateful or FlagSet-dependent normalization is outside this private profile;
  constructors must not collapse distinct definitions before host validation.
  Configure normalization before native discovery, flag-group or help helpers
  merge flags, and do not replace/reset it afterward: Cobra can retain older
  normalizers in private helper sets even after its public normalizer is cleared.
- Do not mutate native globals or share command/FlagSet objects across invocations.
  Avoid native deprecation warnings, which are rejected to prevent raw native
  presentation from escaping the resource-backed path.

For a real operation, validate CLI syntax first, then call its typed framework
operation or selected public capability. Domain invariants must also hold for
non-CLI callers. Do not reimplement SDK/resource management in a command.

The actual resource owner uses structured cleanup, normally a defer with
`errors.Join`, on success, operation failure, partial initialization and cooperative
cancellation. A borrowed capability does not grant closing authority. Pure commands
need no fabricated Open/Close lifecycle. Join owned goroutines before returning;
native parser post-hooks are not cleanup guarantees.

Run parses each routing node's prefix with native pflag before asking Cobra to match
one command token. Do not replay those prefixes: counters, slices and custom values
must see each supplied occurrence once. Inherited bound values and `Changed` remain
shared across the path; the terminal command's `Args` and `ArgsLenAtDash` are leaf-local.
For an effective explicit-flag inventory use `VisitAll` with `Changed`, not `Visit`
or `NFlag`: the latter track a native FlagSet's local parsing history, not the whole
invocation. Flag aliases are interpreted by the node at their argument position.

## Keep presentation and operation facts separate

Resources travel with the family, as the fixture demonstrates. Bind complete English
prose to native `Short` and flag `Usage`. Bind optional Chinese text to command
annotation `fathomry.short.zh-CN` and flag annotation `fathomry.usage.zh-CN` (first
entry). Missing Chinese text falls back to those English fields.

These annotations are a private first-party convention, not a public catalog API.
The renderer consumes `Use`/command paths, `Short`, native flags, shorthands and
types; it does not mirror flag definitions or display values/defaults automatically.
Maintain localized prose in resource files, not handler literals or parser-message
substring substitutions. Machine schemas and literal machine bytes remain stable.

Use `command.Context()`, `InOrStdin()`, `OutOrStdout()` and `ErrOrStderr()`:
the host binds them to explicit borrowed inputs and checked writers. Do not replace
them with process globals or close them. Raw, finite and incremental output are
allowed; a stream's prefix must be visible before its source finishes.

Return causes without embedding them in ordinary diagnostics. Preserve both
operation and cleanup errors. Never retry because output failed, and never describe
cancellation as proof that an accepted mutation did not occur. The
[calling contract](../reference/cli/interface.md) owns status, language, native
grammar exceptions and process guarantees.

## Verify from the repository root

Use the repository's configured disposable build area for explicit binaries and
test scratch data. On the maintainer workspace:

```sh
job=$(mktemp -d /home/frost/tmp/codex-build/jobs/build.XXXXXX)
export TMPDIR="$job"
go test -race -count=1 -timeout=3m ./cli ./internal/conformance
go vet ./cli/... ./cmd/fathomry ./cli/internal/testdata/commandfamily
go build -o "$job/fathomry" ./cmd/fathomry
"$job/fathomry" help
"$job/fathomry" --lang zh-CN --help
```

Then run the applicable repository-wide checks from
[CONTRIBUTING](../../.github/CONTRIBUTING.md). The CLI tests explicitly import the
testdata family; `go test ./...` does not discover that fixture package separately.

Required witnesses include:
- A/B composition and a family-local leaf; help/invalid/pre-canceled calls with
  poison inputs and zero acquisition, including constructor-side evidence.
- Actual unrelated-module Run/Main consumption and selected dependency graph.
- Short writes, ignored errors, help/usage/diagnostic failures and cleanup joins.
- One independently readable effect before delivery failure/cancellation; gated
  prefix visibility without whole-result buffering.
- Language parsing order/fallback and invariant machine bytes; sequential/concurrent
  fresh state and borrowed stream ownership.
- Real fd1/fd2 SIGPIPE, INT/TERM, mixed failures, blocked I/O and subsequent forced
  exit, with children always terminated and reaped.

Service acceptance, Temporal replay and new project-loading behavior belong to their
actual future feature scopes, not to this root/help proof.
