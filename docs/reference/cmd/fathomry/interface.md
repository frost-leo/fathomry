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

# Fathomry command-line interface

[Documentation](../../../README.md) / Command-line reference

**Audience:** users and maintainers of the official Fathomry executable.
**Status:** implemented offline help, error definitions/explanations and translation
resource/coverage queries. Version, project generation, deployment, project
configuration loading and Worker commands are not included.
**Executable source:** [cmd/fathomry](../../../../cmd/fathomry/main.go).
The private implementation is not a public command-extension SDK.

## Run the implemented commands

From the repository root with its selected Go toolchain:

```sh
go run ./cmd/fathomry --help
go run ./cmd/fathomry error list
go run ./cmd/fathomry error list --module fathomry --component configuration
go run ./cmd/fathomry error explain 0xA0450001 --lang zh-CN
go run ./cmd/fathomry error explain fathomry.settings.not_configured
go run ./cmd/fathomry error components --output json
go run ./cmd/fathomry i18n list --output json
go run ./cmd/fathomry i18n show fathomry.failure.invalid_code --locale zh-CN
go run ./cmd/fathomry i18n coverage zh-CN --output json
```

The executable, not the go run launcher, owns the documented exit codes. For
binary/process testing, build with an explicit output path in a disposable job:

```sh
job=$(mktemp -d /home/frost/tmp/codex-build/jobs/build.XXXXXX)
go build -o "$job/fathomry" ./cmd/fathomry
"$job/fathomry" error explain 0xA0450001 --lang zh-CN --output json
```

No project, application settings, configuration source or live service is needed.
The command catalog explicitly gathers all currently implemented public component
declarations and translations, plus the command-line capability. Framework
components contribute static metadata only; no Framework loading/runtime API runs.
It is not an inventory of every SDK's private native errors or arbitrary business
projects. Unknown well-formed codes are not silently assigned meanings.

| Command | Result |
| --- | --- |
| help [command], --help | Help for the selected command |
| error list | Static error definitions; --module and --component must occur together |
| error explain CODE_OR_IDENTIFIER | Hex/decimal Code or symbolic Identifier, its definition and localized explanation |
| error components | Registered owners, capability/facility allocation and exact codes |
| i18n list | Message/resource metadata, including arguments, forms and baseline digest |
| i18n show MESSAGE_ID | Exact lookup; --locale overrides the query language without changing interface language |
| i18n coverage LOCALE | Exact missing translations for each registered component |

A known message with no exact translation is a successful inspection with
translation_exists=false. Unknown messages, errors or owners return not-found.
Coverage does not count fallback as an existing translation.

## Language, output and failure identity

--lang overrides FATHOMRY_LANG; absent/empty environment preference defaults to en.
The selected preference is explicitly bound through i18n.Presenter.WithLocale,
never settings.Default or a resource-held Presenter. CLI and application display
languages are independent. Valid untranslated locales fall back according to the
owning catalog, without becoming usage failures. Invalid explicit preferences fail;
a valid --lang can replace an invalid environment preference.

There is no implicit interpretation of LANG/LC_* or conversion of POSIX locale
strings. Locale values follow the existing BCP 47 profile. For invalid invocations,
only flags consumed before the parse failure can affect presentation; invalid
preferences fall back to safe baseline diagnostics without exposing their input.
Put global automation options before the command.

--output accepts text (default) or json. stdout contains finite results only;
stderr contains final diagnostics. There are no implicit prompts, color, progress
spinners, native error dumps or extra library usage banners.
Successful finite results are staged within 16 MiB and emitted only after successful
execution/cleanup. A failed output write is not retried and can have delivered a
prefix; consumers must check the process exit code and complete JSON framing.

Machine result envelopes contain schema=fathomry.cli/v1, command and command-owned
data. Error envelopes contain schema, code, identifier, message, requested_locale,
locale and optional fallback. Code is the canonical hexadecimal string. Stable
identities, numeric values and field names do not change with display language;
human description/message fields may. error list retains the code-owned baseline
Definition, while error explain adds a separate localized message.

| Exit code | Meaning |
| --- | --- |
| 0 | Command/help/inspection completed successfully |
| 1 | Not found, execution, output or cleanup failure |
| 2 | Usage or invocation/output-bound refusal |
| 130 | Caller cancellation, unless a cleanup/output failure also occurred |

Exit categories are not truncated 32-bit failure codes. Command-owned failures
occupy capability facility 0x007, fathomry/command_line. Original lower-layer
occurrences keep their code and native cause graph; unknown native errors receive
a safe command-level presentation. Neither translated text nor exit status proves
that a remote mutation did or did not happen.

## Ownership and internal extension

The process entry alone reads argv/FATHOMRY_LANG, handles signals and exits.
Private command construction performs declarations only. Per-invocation flag state,
streams and language selection are not process globals. Catalogs are immutable.
Only actual command execution may request a lazily created public resource Scope.
The common executor attempts bounded cleanup on success, errors and panic unwinding, outside
Cobra's success-only post hooks. Cleanup uses a fresh context with a bounded wait;
expiry reports incomplete cleanup and never invalidates a held borrow as proof
of completion.

The process treats a closed output pipe as an output failure rather than an
uncontrolled SIGPIPE exit. The first interrupt/termination signal cancels work;
signal handling then returns to the OS default so a second signal can force
termination. Forced termination cannot certify cleanup. Borrowed I/O is synchronous:
arbitrary blocking writers cannot be forcibly canceled by an io.Writer API.

Add official command implementations under cmd/fathomry/internal/command and
register them in the private app assembly. Reuse common language, errors, streams
and resource ownership; do not duplicate configuration/native engines or call
os.Exit in a handler. No business-developer plugin registration is exposed.
A command may directly compose public Adapters/resource or implement ordinary local
Go operations. Do not add a Framework API solely to mirror a command.

## Executable verification

- [Common admission, resource ownership, cancellation and output tests](../../../../cmd/fathomry/internal/command/run_test.go).
- [Real command families, locale/JSON behavior, privacy, concurrency and argument fuzzing](../../../../cmd/fathomry/internal/app/app_test.go).
- [Built executable, broken pipe, process exit and signal cleanup tests](../../../../cmd/fathomry/main_test.go).
- [Import and private-package boundaries](../../../../internal/conformance/imports_test.go).

```sh
go test -race -count=1 ./cmd/fathomry/...
go test -race ./cmd/fathomry/internal/app -run '^$' -fuzz '^FuzzArguments$' -fuzztime=30s
```

POSIX signal behavior is verified on Linux; Windows compilation alone is not
runtime certification. This executable does not recreate the withdrawn public
cli package or qualify project-generated programs.
