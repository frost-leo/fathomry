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
**Status:** implemented offline help, searchable error definitions/explanations,
translation inspection/rendering/coverage checks and project generation. Generated executables perform finite configuration
loading. Version, deployment and Worker commands are not included.
**Executable source:** [cmd/fathomry](../../../../cmd/fathomry/main.go).
The private implementation is not a public command-extension SDK.

## Run the implemented commands

From the repository root with its selected Go toolchain:

```sh
go run ./cmd/fathomry --help
go run ./cmd/fathomry error list
go run ./cmd/fathomry error list --module fathomry --component configuration
go run ./cmd/fathomry error list --component configuration --query invalid
go run ./cmd/fathomry error explain 0xA0450001 --lang zh-CN
go run ./cmd/fathomry error explain fathomry.settings.not_configured
go run ./cmd/fathomry error components --output json
go run ./cmd/fathomry i18n list --output json
go run ./cmd/fathomry i18n list --component failure --locale zh-CN
go run ./cmd/fathomry i18n show fathomry.failure.invalid_code --locale zh-CN
go run ./cmd/fathomry i18n locales --component failure
go run ./cmd/fathomry i18n render fathomry.failure.invalid_code --locale zh-CN
go run ./cmd/fathomry i18n coverage zh-CN --strict --output json
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
| error list | Baseline definitions and localized explanations; independent --module/--component filters and --query search |
| error explain CODE_OR_IDENTIFIER | Hex/decimal Code or symbolic Identifier, baseline definition, code structure and localized explanation |
| error components | Registered owners, capability/facility allocation and exact codes; independent --module/--component filters |
| i18n list | Message/resource metadata; --module, --component, exact --locale and --query filters |
| i18n show MESSAGE_ID | Exact lookup; --locale overrides the query language without changing interface language |
| i18n locales | Actual resource languages, baseline language and translated/total counts per owner; --module/--component filters |
| i18n render MESSAGE_ID | Render with --locale, repeatable --arg NAME=VALUE and optional --count; report language/plural provenance |
| i18n coverage LOCALE | Summary and per-owner exact missing translations; --module/--component filters and optional --strict |
| new PROJECT | Create an absent project directory with the selected configuration profile |

A known message with no exact translation is a successful inspection with
translation_exists=false. Unknown messages, errors or owners return not-found.
Coverage does not count fallback as an existing translation.

## Search and inspect registered catalogs

Owner filters are case-sensitive exact namespaces. Either may be supplied alone;
both together select their intersection. Unknown explicit owners return not-found,
including for coverage, so a misspelled filter cannot produce a successful empty
check. Invalid owner syntax returns usage failure. Omitting a selector leaves that
dimension unconstrained. The component name alone may select several modules.

`--query` is a case-insensitive literal substring, not a regular expression.
Error search covers canonical hexadecimal codes, identifiers, baseline messages,
descriptions and localized explanations. Translation search covers message IDs,
baseline context and authored form patterns. Search is applied after owner/locale
selection; no matching text is a successful empty list (`[]` in JSON).

Errors remain ordered numerically; owners are ordered by module/component;
translation entries by ID, with baseline first and then the other locales.
`i18n list --locale` canonicalizes a valid language tag and selects that exact
resource language. It does not match a related locale or fall back. Without this
flag the list includes all actual resource languages, regardless of `--lang`.

`error list` returns an array of the same explanation records used by `error
explain`: `definition` retains the code owner's baseline contract, while `message`
is localized. `domain`, `facility` and `number` describe the actual code structure.
`requested_locale`, `canonical_locale`, `matched_locale`, `locale` and optional
`fallback` distinguish the request, matching and actual resource. Text explanation
also includes revision, owner, optional baseline description and details contract.
None of these static records claims a runtime root cause, retryability or effect
certainty. The unreleased list payload is now explanation records, not bare
definitions; consumers should read baseline identity from `data[].definition`.

`i18n show` retains exact-lookup semantics. Its text and JSON outputs expose the
message contract, context, baseline digest, declared argument names/types/meanings,
cardinal requirement and authored forms. A missing translation has no substituted
definition. `i18n locales` reports each owner's declared resource languages and
exact entry counts; a language present for one owner is not support for another.

## Render and check translations

Rendering delegates selection, validation and plural rules to the public
[i18n engine](../../i18n/v1/interface.md). `--lang` controls CLI labels
and diagnostics; render/show `--locale` controls the requested message language,
defaulting to `--lang`. An explicitly empty or invalid locale is a usage error.
Render may match or fall back; show and coverage never do. Rendering includes
`category`, `variant` and `form_fallback`, distinct from whole-message `fallback`.
Matching a related regional tag need not be a baseline fallback.

Each repeatable `--arg NAME=VALUE` supplies one declared parameter. Split occurs
only at the first equals sign; commas, later equals signs and empty string values
are preserved. Values are typed by the selected resource's declaration, not guessed:

- `string`: literal UTF-8, at most 4,096 bytes.
- `int64` and `uint64`: base-10 integers with full 64-bit range; no floating point,
  implicit octal, hexadecimal, separators or exponent notation.
- `bool`: exactly `true` or `false`.

Unknown, duplicate or missing parameters reject. The limit is 16 parameters and
16,384 total rendered argument bytes; the engine also limits rendered text to
65,536 bytes. Cardinal messages require `--count` in unsigned decimal uint64 range;
zero is a supplied count, not absence. Noncardinal messages reject any count.
The engine validates the exact parameter set; the CLI does not add a second
template formatter. User-supplied values may appear in successful rendered text:
do not put credentials in command arguments, shell history or rendering input.

The shipped catalog currently uses parameter-free, noncardinal resources.
Argument/plural behavior is exercised against explicitly registered test catalogs;
these flags do not load arbitrary business resources or change catalog assembly.

Coverage JSON contains `locale`, `total`, `translated`, numeric `missing`,
`complete` and `components`. Each component includes its total, translated count
and an exact `missing` ID array. The denominator is the owner's baseline message
set, not the number of resource files. This unreleased summary object replaces
the previous top-level component array. Zero missing resources means complete;
fallback never increases the translated count.

Ordinary coverage inspection returns 0 even when incomplete. `--strict` instead
returns 1 with `fathomry.command_line.check_failed`, retaining the complete report
on stdout and placing the safe diagnostic on stderr. Both streams honor
`--output`. For example, with the built executable above, this reports missing
French resources and intentionally exits 1:

```sh
"$job/fathomry" --output json i18n coverage fr --component failure --strict
```

A completed negative check is not an execution failure. Handler, presentation,
cleanup or output failure still takes precedence; an unqualified staged report
is not emitted. A failed stdout write may deliver a prefix, and a failed stderr
write may occur after a complete report: check framing, diagnostics and exit code.

## Create an independent project

From the Framework checkout, explicitly select development dependencies:

```sh
go run ./cmd/fathomry new /path/to/demo --module example.org/demo \
  --framework-source "$PWD" --config-source local --config-format yaml
go run ./cmd/fathomry new /path/to/remote-demo --module example.org/remote-demo \
  --framework-source "$PWD" --config-source remote --config-provider nacos --config-format toml
```

The parent must exist and the destination must not exist. Generation is offline;
it does not run Go, Git, dependency downloads, services or configuration publication.

| Parameter | Meaning |
| --- | --- |
| `PROJECT` | Destination, separate from module identity |
| `--module` | Required valid Go module path, with no exact lowercase `vendor` component |
| `--name` | Application/command name; defaults to destination basename |
| `--config-source` | local (default) or remote |
| `--config-provider` | Defaults independently to viper; remote requires explicit nacos |
| `--config-format` | yaml (default) or toml |
| `--framework-source` | Explicit development checkout; otherwise require a versioned CLI build |

Names are lowercase ASCII slugs starting with a letter, at most 64 characters.
Reserved filesystem names, vendor and testdata reject. CLI `--lang` is independent
of generated application locale. Unsupported combinations reject before creation.
Creation flags choose explicit Framework provider declarations and document encoding.
Generated runtime options select only that provider's paths/environment/credentials;
they cannot implicitly switch the provider. Framework receives an explicit
[Provider dependency](../../framework/configuration/v1/interface.md#explicit-declarations-and-dependencies).

Each profile contains 21 files:

```text
go.mod, .gitignore, .env.example, README.md
cmd/<name>/main.go, main_test.go
internal/bootstrap/boot.go, boot_test.go
internal/bootstrap/options.go, options_test.go, inputs.go, inputs_test.go
internal/setting/setting.go, setting_test.go, application.go, application_test.go
configs/base.<format>
configs/environments/{development,testing,production}.<format>
```

Local adds `configs/local/development.example.<format>`. Remote instead adds
`bootstrap.example.<format>` with explicitly replaceable endpoint/namespace/key
settings. Examples are inert: remote use requires deliberate bootstrap selection
and authorized document publication. No live services or secrets are created.

Settings owns aggregate fields, defaults and validation. Application contains
identity and the existing i18n preference type. Project ParseOptions declares
authorized input names and calls Framework ReadInputs. Boot imports no Adapter or
Internal API: it passes project Schema/Variables and an explicit Provider to
configuration.Load. Framework owns source/operation/evidence lifecycle, without
copying native parsers, Watch loops or resource engines. A generated Watch function
uses the same project declarations and returns one explicitly owned watcher plus
released startup records. Main handles signals, emits a safe finite result and
exits; it reports worker_started=false, not a placeholder runtime host. Remote
options call Framework PrepareNacos rather than implementing bootstrap parsing,
validation, selection and result extraction in the generated project.

Runtime flags are `--env` and `--dotenv`, plus `--config-dir`/`--local-config` for
local projects or required `--bootstrap` for remote projects. Corresponding explicit
bindings use FATHOMRY_ENV, FATHOMRY_DOTENV, FATHOMRY_CONFIG_DIR,
FATHOMRY_LOCAL_CONFIG or FATHOMRY_BOOTSTRAP. Arguments override process values,
an explicitly selected literal dotenv, then declared project defaults. The project
binds FATHOMRY_APP_LOCALE separately as a process-only application override.
Remote projects also bind Nacos username/password/root-CA overrides explicitly.
Nothing scans parent directories or loads examples automatically. Relative local
paths are resolved in project input/dependency code before reaching Framework's
absolute-file declaration. Every file/key carries its own format and layer policy.

The unreleased library Bootstrap/Startup, raw Source/Runtime dependencies and
generated runtime source/provider selector flags are withdrawn without compatibility. Input encoding remains an
explicit Options field with a generated DocumentEncoding constant, not native
format inference. Joining cleanup errors preserves causes; the generated report
explicitly selects the first public occurrence for its code rather than treating
failure.Inspect as an implicit cause-graph search.

Development `go.mod` uses live confined checkout replacements and a local v0.0.0
placeholder. This is not a deployable version pin. A versioned CLI instead selects
its exact supplying Framework module version and maintained versioned SDK pins;
Go does not inherit dependency replace rules. Unversioned builds without an
explicit checkout reject. No go.sum or external dependency-manifest input is
fabricated. Run `GOWORK=off go mod tidy`, tests and the explicit build command from
the generated README. Building that project may download dependencies.

The repository still contains local SDK replacements, so a direct
`go install .../cmd/fathomry@VERSION` is not promised. Build the CLI from a checked
out revision, or from a main module that supplies the documented replacement
policy. No installer or release publication is added by project generation.

Creation renders and admits dependencies before an exclusive directory claim.
It uses rooted relative writes and exclusive files, never merges an existing
directory or symlink. Cancellation/write failure retains partial output for
inspection; successful creation followed by output failure retains the project.
There is no destructive rollback, overwrite or force flag. Rooted operations
protect traversal, not against arbitrary filesystem stalls or hostile parent
renames; concurrent creators cannot both claim the same destination.

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
human description/message fields may. Error list/explain retain the baseline
Definition separately from the localized message. Strict negative coverage checks
are the explicit report-plus-nonzero-exit case described above.

| Exit code | Meaning |
| --- | --- |
| 0 | Command/help/inspection completed successfully |
| 1 | Failed strict check, not found, execution, output or cleanup failure |
| 2 | Usage or invocation/output-bound refusal |
| 130 | Caller cancellation, unless a cleanup/output failure also occurred |

Exit categories are not truncated 32-bit failure codes. Command-owned failures
occupy capability facility 0x007, fathomry/command_line. Project-creation dependency,
destination and incomplete-write failures use 0x00A, fathomry/project_creation.
Original lower-layer
occurrences keep their code and native cause graph; unknown native errors receive
a safe command-level presentation. Neither translated text nor exit status proves
that a remote mutation did or did not happen.

## Ownership and internal extension

Each official command family owns its declarations and resource bundle, following
the project command's assembly pattern. `errorcatalog/resources/` and
`messages/resources/` each contain `en.json` and `zh-CN.json`, embedded by their
own `resources.go`. Their `Resources()` and `Component()` are composed explicitly
by the private app alongside `command.Component()` and `project.Component()`.
Neither family implicitly imports the other's resources or a global registry.

The command entry files only bind flags, invoke catalog operations and stage
results. `options.go` owns request admission; `catalog.go` and `render.go` operate
on explicit immutable catalogs without Cobra, Invocation or text formatting;
`output.go` owns human-readable presentation. Coverage incompleteness remains a
domain result, with `CheckResult` applied only at the command edge. The common
package retains generic invocation errors, help, streams, cleanup and check-exit
policy, not command-specific labels or translation behavior.

The two resource-only owners are `fathomry/error_catalog` and
`fathomry/message_catalog`. They declare no new error meanings or facilities.
Consequently, translation owner inventories are not identical to `error components`
or the failure allocation manifest. Command-specific message identities formerly
under `fathomry.command_line` have moved to the corresponding new owner, without
compatibility aliases; shared invocation/help messages remain under `command_line`.
This changes resource discovery IDs, owners and source digests, not the command
names, flags, report DTOs or process exit policy.

The process entry alone reads argv/FATHOMRY_LANG, handles signals and exits.
Private command construction performs declarations only. Per-invocation flag state,
streams and language selection are not process globals. Catalogs are immutable.
Before entering a handler, common admission validates its declared static `Short`
message with the selected presenter. Missing command resources are invalid setup,
even for JSON and unlabeled list output; no operation or success result is allowed
to precede this check. Pure catalog operations do not require CLI message bundles.
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
- [Partial catalogs, exact filters, per-owner languages and strict coverage](../../../../cmd/fathomry/internal/command/messages/catalog_test.go).
- [Typed scalar arguments, plural rendering, fallback and argument fuzzing](../../../../cmd/fathomry/internal/command/messages/render_test.go).
- [Built executable, broken pipe, process exit and signal cleanup tests](../../../../cmd/fathomry/main_test.go).
- [Import and private-package boundaries](../../../../internal/conformance/imports_test.go).
- [Exclusive generation and failure/retention controls](../../../../cmd/fathomry/internal/command/project/project_test.go).
- [Four independent profiles and cold versioned-module-cache qualification](../../../../cmd/fathomry/internal/command/project/integration_test.go).
- [Linux FIFO/racing-file and generated repeated-signal controls](../../../../cmd/fathomry/internal/command/project/integration_linux_test.go).
- [Generated remote YAML/TOML programs against a loopback Nacos protocol fixture](../../../../framework/configuration/v1/integration_test.go).

```sh
go test -race -count=1 ./cmd/fathomry/...
go test -race ./cmd/fathomry/internal/app -run '^$' -fuzz '^FuzzArguments$' -fuzztime=30s
```

POSIX signal behavior is verified on Linux; Windows compilation alone is not
runtime certification. The controlled module-proxy test is not a published release;
Nacos protocol-fixture execution is not production service qualification. This
executable does not recreate the withdrawn public cli package.
