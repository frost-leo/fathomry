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

# CLI interface

[Documentation](../../README.md) / Public package reference

**Audience:** Go callers embedding Fathomry's first-party command entry.
**Status:** implemented root/help and local-source project creation; runtime acceptance
targets Go 1.27.0 on Linux/amd64. No domain commands or complete runtime.
**Package:** `github.com/frost-leo/fathomry/cli`.

## Responsibilities and use

The public surface is `Run`, `Main`, `Streams`, `Catalogs`/`CatalogSet` and the
documented host/project condition values below. It exposes usage and definition
inspection, not command registration. Native Cobra
objects, construction and composition remain private.
Root/help and [`new <directory>`](new.md) ship; test command families never enter
the executable.

A standalone process entry is simply:

```go
package main

import "github.com/frost-leo/fathomry/cli"

func main() { cli.Main() }
```

An existing process can call `Run(ctx, args, streams)` and inspect its status and
error without changing signal policy or exiting. See the
[executable consumer](../../../cli/testdata/consumer/main.go) and
[Go declarations](../../../cli/run.go) for exact signatures.

- Arguments exclude the executable name. Nil and empty slices request root help,
  never ambient `os.Args`. The slice is copied; do not mutate it concurrently.
- Context and all stream fields must be non-nil, including dynamic pointer values.
  Invalid inputs return status 1 without substituting process streams or attempting
  diagnostics. Supply an empty reader or `io.Discard` deliberately.
- Input, output and diagnostic streams are borrowed and never closed or flushed.
  Success means the supplied writers accepted bytes, not durability or flushing
  of caller-owned buffers. No background
  reader, signal handler, output buffer or abandoned I/O goroutine is added by Run.
- Each invocation constructs fresh parser and language state. Concurrent calls are
  safe with independent inputs; callers must synchronize shared streams and code.
- Discovery and help construct metadata only. Parsing and static argument,
  required-flag and group checks precede selected operation acquisition. Commands
  own their actual resources and structured cleanup.

## Query definitions without executing commands

`Catalogs()` returns fresh handles for shared immutable `Errors`, `Messages`
and `Bindings`. It prepares embedded owner declarations/resources once, without
reading argv/environment, project paths or input streams, choosing a locale,
starting services or executing Run. No new CLI query command is added.

The error/module inventory includes `fathomry.cli`, its direct
`fathomry.cli.project` child, and the separate `fathomry.i18n` dependency
conditions. The 14 CLI host/project conditions, 33 presentation bindings and all
38 English/Chinese resource entries remain inspectable independent of language.
Use exact error/module/binding queries and resource `Lookup`, rather than
inferring translation existence from fallback text. Queries return owned snapshots.

The private output occurrence records its core and the observed stream. Project
creation returns one private `creationResult` containing observation and error:
unknown zero, not-started, completion-unconfirmed, or completed writes/closes.
The definition catalog marks these contracts owner-only; it does not invent a
public progress accessor or claim operation observations are fields of every
output/rendering failure. Conditions and presentation are registered explicitly
by their owning feature, not discovered from previously encountered errors.

## Commands, help and language

```text
fathomry
fathomry --help
fathomry -h
fathomry help
fathomry help help
fathomry --lang zh-CN help
fathomry new --help
```

Unknown command paths, extra help targets, malformed flags and unsupported language
values fail with status 2. `missing --help` is not successful help. Help bypasses
execution-only required/group checks and arbitrary semantic argument validation;
it does not bypass flag syntax errors. Runnable leaf arguments are not semantically
validated when help is requested. Non-runnable groups reject leftover operands.
Root/groups are help-only routing nodes; only leaves may execute. A runnable root
or parent with children is an invalid first-party definition, not a supported
ambiguous operand/help grammar.
Visible and hidden completion names are unsupported, including help variants.

The finite selector accepts `en` and `zh-CN` case-insensitively. English is the
default and fallback; no environment locale, configuration or remote lookup occurs.
The last successfully parsed valid selector before a parsing error controls the
diagnostic. Thus `--lang zh-CN --bad` differs from `--bad --lang zh-CN`.
`--` ends flag interpretation; it is not a locale-selection escape hatch.

Root and project text use one explicitly shared immutable
[`i18n/v1` catalog](../i18n/v1/interface.md), prepared from feature-owned
resources. English contracts and control-free literal text are checked before
effects; metadata is resolved at help-render time, not frozen to a construction
locale. Public catalog locale support does not widen this CLI allowlist.
The first hard presentation failure is retained even when native Help/Usage
callbacks discard errors. It is an independent failure (status 1), not invalid
usage or authorized English fallback. Original causes and completed effects remain.
If the host generic diagnostic cannot render, one fixed
`fathomry.cli.presentation_failed` line is attempted; a failed writer is never
called again. Raw errors and joined trees are not searched for a primary to translate.

Help reads native command and flag metadata with resource-backed human labels.
Parsed values and defaults are not displayed. Command/flag names, machine keys,
raw bytes and effect facts do not change with language. The host does not impose a
JSON envelope, a global output mode or whole-result buffering.

### Native grammar and global-state limits

The private parser is Cobra v1.10.2 with pflag v1.0.10. At each routing node,
pflag consumes the option prefix before Cobra matches the next positional command
token. A flag value is never sent to command discovery. Inherited scalar options,
including shorthand clusters such as `-vc value`, may precede a command path;
they are parsed using the namespace of the node at that position. Leaf-local
options must follow the complete path; using them earlier is a usage error.
Leaf options may be interspersed with its operands. `--` stops both option parsing
and further command routing at that node, unless consumed as a flag's value.

Prefixes are consumed once, not replayed at the leaf. Shared inherited values and
`Changed` state retain earlier assignments; later occurrences follow their native
value semantics. Help is an inherited flag, so a prefix `--help` reaches the
selected command without executing it. Routing groups define persistent options,
not non-inherited local options. See the maintainer guide for the native introspection
and fresh-state requirements of this private integration profile.

**Observed native exception:** pflag's Go-test shorthand compatibility ignores
`-test.*`, including a remaining shorthand suffix such as `-htest.foo=x`.
These inputs can succeed; `--test.foo=x` is an ordinary unknown long flag and fails.
This is explicitly tested, not a claim of strict rejection of every unknown token.
No second parser, argv-rewriting shim or local parser fork is added.
This retains the parser's documented
[Go-test shorthand compatibility](https://github.com/spf13/pflag/blob/v1.0.10/README.md#using-pflag-with-go-test),
not a Fathomry option or a blanket unknown-flag allowlist.

Cobra implicitly merges `pflag.CommandLine`. The host refuses a nonempty global
pflag set instead of importing ambient options or modifying that set. Trusted
first-party code must not mutate native global settings or install native global
initializers/finalizers. Unrelated linked code changing parser globals concurrently
is outside the isolation guarantee. The host is not a callback or panic sandbox.

## Output, causes and effects

All host output paths use checked writers, including help, usage and diagnostics.
A short write becomes `io.ErrShortWrite`; the first failed write on each stream is
retained and later writes to that stream are suppressed. Already-written bytes are
not retracted. A handler ignoring an output error cannot turn it into success.

Normal diagnostics are bounded resource-backed projections, never raw parser or
operation `Error()` strings. A command may add owned explanatory text; the host's
failure summary is attempted once without recursive reporting.
Host and project failures use values of the shared
[`failure.Condition`](../failure/v1/interface.md) type. A sole suitable semantic
occurrence is preserved, including its own deliberately retained cause tree.
Multiple independent failures are explicitly joined; a raw join has no implicit
primary and need not be directly inspectable. There is no universal invocation
wrapper or condition derived from the numeric exit status. Success returns nil.

| Public condition | Meaning established by its owner |
| --- | --- |
| `ErrInputs` | Required Go context/stream inputs are absent, including typed nils |
| `ErrDefinition` | The first-party command/metadata or root presentation definition is invalid |
| `ErrUsage` | CLI parsing, command/operand selection or required/group argument checks refused the invocation |
| `ErrLanguage` | Native parsing reached an unsupported value of the host's language selector |
| `ErrOutput` | A supplied output/diagnostic writer failed or did not accept the complete write |
| `ErrProjectArguments` | The project command's required/static inputs are invalid |
| `ErrProjectIdentity` | The requested project module conflicts with the framework's reserved identity |
| `ErrProjectSource` | The selected local source could not be resolved/read or does not satisfy the supported source profile |
| `ErrProjectDestination` | Destination preparation/admission failed, except the specific existence/overlap cases |
| `ErrProjectExists` | Destination existence was observed or exclusive directory creation refused it as existing |
| `ErrProjectOverlap` | Resolved destination is inside the selected source |
| `ErrProjectPreparation` | Other pre-creation preparation, such as working-directory resolution or file rendering, failed |
| `ErrProjectCreation` | After directory admission, a file open/write/close failure prevented establishing complete creation |
| `ErrProjectPresentation` | Required command-owned presentation definitions violate the command's contract |

Exact code values are in [the declarations](../../../cli/error.go). Project
conditions are declared with the feature and exposed through cli as constant
values, not aliases of private runtime types. Conditions identify supported
semantics, not namespace authenticity, retry safety or a complete effect receipt.
Standard context cancellation/deadline causes remain supported; a semantic
occurrence does not become pure cancellation merely because its causes include it.

The host keeps origin-known usage classification and its first presentation/
writer/cancellation observations separate from the semantic errors. It composes
the final return from current slots rather than repeatedly appending the same
snapshot. Repeated direct observations of the same valid core pointer are retained
once, as are repeated exact owned output-occurrence pointers; distinct stream failures get distinct occurrences even if their native
cause/code is identical. No foreign equality/matching/accessor hooks or graph
traversal are used for this bookkeeping. Opaque returned graphs remain intact:
if one already contains a retained writer failure, it may expose that cause again.
Cause edges are not a count of physical failure events. No flattening or global
deduplication is promised.

Native `errors.Is/As` cause promises remain. Inspect the explicitly selected
occurrence for its condition and approved facts; generic recursive membership does
not choose a primary or authorize borrowing another same-code occurrence's facts.
Owned core output is code-only and refuses runtime JSON as documented by failure/v1.
That protection does not make arbitrary raw aggregates, native causes or foreign
wrappers safe to print/serialize. Diagnostics use resources, not returned Error
text. No public project progress/recovery DTO or durable error protocol is added.
Cancellation is observed again after a diagnostic write completes, so cancellation
or a caller cause arriving during blocked diagnostic delivery is retained. Final
status is recomputed without another diagnostic or an operation retry. Cancellation
racing after the final observation is not an atomic return-time guarantee.

| Status | Meaning |
| --- | --- |
| 0 | Successful delivery or help |
| 2 | Invalid user invocation, with successful diagnostic delivery |
| 1 | Invalid invocation configuration, execution, deadline, output or cleanup failure |
| 130 | Otherwise pure cooperative caller/INT cancellation |
| 143 | Main-only otherwise pure cooperative TERM cancellation |

Independent failures outrank cancellation. Usage plus failed stderr delivery is 1
with both causes retained. TERM plus EPIPE or an independent cleanup/operation error
stays 1. A custom non-cancellation `context.Cause` is retained and counts as an
independent failure; an ordinary context-only wrapper remains pure cancellation.
A semantic failure occurrence remains independent even when its only native cause
is `context.Canceled`. Output and hard presentation failures likewise remain failures.

No operation is automatically retried. Cancellation, a failed response, broken pipe
or unsuccessful status does not prove no effect, rollback or remote termination.
The test backend's independently readable scratch effect demonstrates this boundary;
it does not qualify any production service.

## Process ownership and blocked I/O

`Main` consumes process arguments and standard descriptors, subscribes separately
to SIGPIPE so fd1/fd2 failures can reach checked I/O, and exits with the resulting
status. Its first observed INT/TERM requests cooperative cancellation. Normal return
waits for the command's owned cleanup, not just cancellation notification.

A later observed termination signal exits immediately with that signal's 130/143
status, without writing a diagnostic or waiting on blocked I/O. Physical signals
can coalesce: this policy concerns observed notifications. Forced exit does not
prove cleanup or complete delivery, even when its numeric status matches normal
cancellation.

Run cannot interrupt arbitrary borrowed readers, writers or noncooperative handlers.
When it is given real fd1/fd2, the embedding process owns SIGPIPE policy; Run does not
promise to turn a process-killing SIGPIPE into an error. No timeout is simulated by
abandoning an operation goroutine. These restrictions are tested in reaped Linux
subprocesses, including full stdout/stderr pipes and subsequent forced termination.

## Dependencies and qualification

The production package graph contains the CLI, its private project command, native
Cobra/pflag, x/mod's modfile/module/semver/internal-lazyregexp packages,
`i18n/v1`, `failure/v1`, the ten selected x/text language/plural dependency
packages and the standard library on Linux. Linking the CLI includes these dependencies;
file-level separation does not make them optional. It does not import Fathomry's
technical SDK integrations.
An unrelated module actually builds and runs both public entries with `GOWORK=off`
and exactly one source replacement, for Fathomry itself. It copies no SDK replacement
graph or host mechanics.

Checkout builds and source-replacement consumption do not qualify
`go install ...@version`, an installable release, all-SDK consumption, automatic
discovery of future project code, or another OS's runtime behavior.

[Issue #88](https://github.com/frost-leo/fathomry/issues/88) defines the entry foundation;
[issue #90](https://github.com/frost-leo/fathomry/issues/90) adds local-source creation.
[Issue #93](https://github.com/frost-leo/fathomry/issues/93) adds shared resource
preparation and rendering without changing the CLI's operation/exit policy.
[Adjacent tests](../../../cli/run_test.go),
[independent consumption](../../../cli/consumer_test.go) and
[Linux process acceptance](../../../cli/process_linux_test.go) provide executable
evidence. Maintainers should follow [Adding commands](../../development/cli-commands.md).
