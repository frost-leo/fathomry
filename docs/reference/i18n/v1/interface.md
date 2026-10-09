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

# Internationalization interface

[Documentation](../../../README.md) / Public package reference

**Audience:** component authors, framework composition and diagnostic tooling.
**Status:** implemented explicit resource catalogs, error-code explanations and
settings-backed presentation. Public SDK Adapters, Framework scenarios and CLI
commands are not supplied by this package.
**Package:** `github.com/frost-leo/fathomry/i18n/v1`.

## Ownership and dependencies

Components own resources, error definitions and any runtime argument projection.
Prepare gathers explicitly supplied Component bundles into one immutable Catalog.
Nothing registers through package init, scans ambient directories, reads language
environment variables or installs clients/watchers. Each bundle names its Module,
Name, BaseLocale, Resources filesystem and Directory.

The dependency direction is i18n -> failure/settings/x/text/standard library.
Failure and settings expose immutable embedded files through Resources using only
standard fs.FS; neither imports i18n. CoreComponents explicitly gathers those owners
and i18n itself. It is a convenience, not automatic registration.

The [error code allocation](../../failure/v1/code-allocation.md) remains unchanged:
i18n owns core facility 0x003. It does not infer retry policy or business outcomes
from translation success, native statuses or presentation language.

## Distribute resources and gather the atlas

A component supplies a flat directory of `<locale>.json` files. Prepare discovers
the files; there is no supported-language list to edit when adding another language.
Non-JSON files are ignored. Directory entries and documents are bounded, opened
files are closed on success/failure, and original I/O errors remain inspectable
behind safe resource errors. Custom filesystems own their blocking/concurrency
behavior; this API does not turn an arbitrary FS into a sandbox or cancelable I/O.
Do not mutate input declarations or files during preparation.

Resources use schema `fathomry.messages/v1` and profile `scalar-cardinal/v1`.
Every document declares `module`, `component`, `locale` and `messages`;
an optional license_notice array holds the project's complete license notice.
See the [baseline fixture](../../../../i18n/v1/testdata/resources/en.json) and
[Chinese translation](../../../../i18n/v1/testdata/resources/zh-CN.json).

Each message needs the component's explicit BaseLocale, which is not necessarily
English. A baseline entry declares:

- `id`: owner-qualified dotted identity;
- `contract`: its resource contract revision;
- `context`: author-facing meaning;
- `args`: named scalar kinds and safe meanings;
- `cardinal`: whether an exact unsigned integer count is required;
- `forms`: whole-message patterns, always including `other`.

Translations contain only id, source and forms. Source is the baseline digest,
available through Lookup/Inspect after preparing baseline resources alone.
To add a language, create its locale file, copy each selected baseline digest and
write the translated forms. English/base wording or contract changes invalidate
stale translations atomically. Digests identify content, not author authenticity.

Patterns use `{name}`, with `{{` and `}}` for literal braces. Every declared
ordinary argument must occur in every variant. Cardinal messages may use the
implicit count argument. No template methods/functions or arbitrary object
formatters run. The resource grammar deliberately rejects duplicate/unknown fields,
lossy UTF repair, null/numeric metadata and malformed contracts rather than choosing
an override order.

Error definitions are optional for ordinary resources. Each declared public error
requires a static, argument/count-free baseline resource whose ID equals the failure
Identifier and whose rendered text equals Definition.Message. This prevents a
localized baseline from silently contradicting the code-owned explanation.

## Query without application state

- Inspect returns detached resource records, including source/document digests.
- Components reverses registration into module/component, baseline, locale,
  message and code lists.
- Coverage lists exact missing translations for an explicit locale. Fallback does
  not count as a translation.
- Lookup performs exact canonical-locale lookup, distinguishing unknown identity
  from a missing translation.
- Resolve returns an immutable resource selection and matching/fallback provenance.
- Explain takes a numeric Code and an explicit locale, returning stable error
  metadata plus a localized static meaning. It never reads settings or calls a
  runtime projector. A well-formed unknown code reports found=false.

An English runtime log can therefore be explained in Chinese independently.
The [official CLI](../../cmd/fathomry/interface.md) consumes these offline queries
without application settings or runtime resource construction.

Matching is isolated per component. Another owner's French resources cannot change
which fallback a component without French uses. The selected native matcher index
identifies a real resource; synthesized matching tags are not mistaken for files.
Whole-message fallback uses that component's BaseLocale and reports unsupported
locale versus missing translation. Rendered.Locale is the actual resource language.

## Runtime presentation without locale parameters

Include the component-owned `Preferences` type at `/application/i18n` in the
project's own settings root. Locale is required and is one BCP 47 tag, not an
Accept-Language list. The data has JSON/YAML/mapstructure locale tags; Settings
selection uses the containing fields' explicit JSON tags.

NewPresenter captures a Catalog without requiring application settings to exist.
By default each Render/Present call captures current application settings once.
WithSettings returns a presenter tied to an independent populated reader;
WithLocale returns a fixed-language presenter independent of settings.
Neither changes process globals. Bind a Presenter once at the presentation/log
boundary rather than adding a language parameter to each Adapter operation.
Changing a settings-bound locale does not require rebuilding an Adapter or the
Presenter. A resource-held Presenter is optional for instance/catalog replacement,
not a prerequisite for constructing an error.

`presenter.Present(err)` can be passed as a slog attribute: Presented.LogValue
emits stable code/identifier, frozen message, actual/requested locale, fallback
and a separate presentation-issue code. Plain logging of the original error does
not automatically translate it. Concrete providers return semantic occurrences;
foreign errors are not automatically sanitized. [Framework ErrorLog](../../framework/v1/interface.md)
provides an explicit safe logging boundary. The [public operation integration](../../../../adapters/v1/integration_test.go)
exercises this logging path without coupling operation execution to i18n.

Render selects an ordinary message and requires successful preference/argument
resolution. Present handles a directly supplied failure occurrence:

1. Check its complete Definition against the prepared code definition.
2. Resolve one preference.
3. Use the static explanation, or the component's admitted runtime Binding.
4. Capture rendered text and provenance in a Presented error.

Binding pins Code, Details contract, Message ID and MessageContract. Project receives
the direct occurrence and returns explicit Input scalars/count. The common library
does not search descendants with errors.As or reflect fields out of details.
The [independent component](../../../../i18n/v1/testdata/consumer/component/component.go)
projects private-field/time.Time-containing typed details. Custom Occurrence types
are also supported; Detailed is not mandatory.

The scalar renderer's accepted argument types are builtin string, int64, uint64 and
bool. This is a text-rendering protocol, not a restriction on failure detail types.
Components own conversion of other data into truthful, non-sensitive arguments.
Callbacks must be synchronous, bounded and concurrency-safe; captured dependencies
remain component-owned. No timeout can make arbitrary callback work safe.

## Failure behavior and native evidence

Presented.Unwrap returns the exact original occurrence and Failure returns its
same common core. Numeric errors.Is and native errors.As continue to work.
Ordinary formatting/slog use frozen text, never the original Error method or cause
formatters. Changing settings cannot rewrite an older presentation. Re-presentation
unwraps only this package's known wrapper, not arbitrary wrappers/joins.

Configuration, binding, projection or rendering failure retains the original static
developer message. Issue separately exposes the presentation problem, with native
projection/settings causes when applicable. A projector panic becomes ErrProjection
without formatting or retaining its panic payload. Localization issues are not
inserted into the operation's cause chain or interpreted as its retry outcome.

If the matching catalog proves the baseline text/definition, fallback records that
base locale. Otherwise its language stays unknown, rather than falsely claiming
English or the requested locale. nil stays nil. Foreign errors are returned unchanged
and never formatted/classified automatically; their caller still owns safe handling.
The failure Occurrence contract requires a stable, bounded non-panicking accessor.

Catalog, Selection, Presenter and Presented hide runtime state in fmt/slog and reject
JSON reconstruction/persistence. Explicit inspection/text may be sensitive according
to the author's resource/projection policy. Plain text is not HTML/SQL/shell-safe:
channel owners must escape it appropriately.

## Engine and bounds

The profile is qualified against pinned
[x/text v0.42.0](https://pkg.go.dev/golang.org/x/text@v0.42.0/language), with language
and plural tables declaring CLDR 32. Engine is a qualification baseline, not a
consuming-build report or a claim of current CLDR coverage. Dependency overrides/
upgrades require requalification. No timezone, currency/date formatting, general
ICU template language or automatic locale negotiation is implied.

Counts use exact uint64 formatting. The pinned MatchDigits primitive loses relevant
large-integer residues, so cardinal selection preserves the positive-large class
and residues in a bounded representative before MatchPlural. Independent
English/Chinese/Russian/Arabic oracles and wide-native comparisons qualify it;
upstream implementation changes require revisiting this profile.

| Boundary | Limit |
| --- | --- |
| Components / resource documents / distinct locales | 128 / 512 / 64 |
| One document / total raw input | 64 KiB / 8 MiB |
| Message IDs / localized entries | 4096 / 16384 |
| JSON depth / aggregate nodes | 8 / 1,048,576 |
| Arguments / forms per message | 16 / 6 |
| Pattern bytes / references / segments | 8192 / 64 / 129 |
| Aggregate compiled segments / inspection charge | 262,144 / 16 MiB |
| One string argument / total argument bytes | 4096 / 16 KiB |
| Rendered output | 64 KiB |

Every expansion is charged before output allocation. Limits do not certify process
RSS, a malicious FS/projector's resource use or arbitrary native cause-graph size.
Nil/zero handles reject operations; callers must not overwrite shared handles or
mutate borrowed arguments/counts during rendering. No input arguments are retained.

## Executable evidence

Run `go test -race ./i18n/v1`. Tests cover discovery, partial coverage, owner-local
matching, exact/large cardinals, atomic malformed/stale-resource rejection, native
I/O and close paths, typed/custom projections, panic/privacy containment, preference
updates and concurrent use. Fresh-process tests isolate application defaults.
The [independent module driver](../../../../i18n/v1/integration_test.go) runs a real
component bundle and verifies dependency direction without Internal imports.

Public Adapters, configuration-source Watch composition, CLI commands, real services
and Temporal failure/wire/replay integration remain outside this module. Mutable
process preferences do not belong directly in deterministic Workflow logic.
