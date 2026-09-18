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

# Resource-first i18n interface

[Documentation](../../README.md) / Public package reference

**Audience:** independent SDK consumers and presentation/resource owners.
**Status:** implemented bounded foundation; not a full product language catalog.
**Package:** `github.com/frost-leo/fathomry/i18n`.

## Responsibilities

The package prepares external resources and renders whole plain-text messages
with explicit language selection. English is the authoritative source and final
resource fallback; auxiliary Chinese and Russian grammatical witnesses use the
same mechanism. Business applications own their message namespaces, resources
and presentation policy. No central message enum or global registration is needed.

`failure` does not import localization. A presenter can use both packages, choose
its own message ID and project approved typed facts. Optional diagnostic attributes
are not required template parameters. Rendering cannot change machine identity,
cause matching, effects, timezone, currency, frozen windows or execution facts.
A presentation failure may fall back to the existing code/ID; do not invent inline
English emergency prose or recursively localize the broken catalog.

This is not a CLI/report platform, channel renderer, remote language-pack loader,
hot-reload service, general formatting language, durable message DTO or workflow
integration. It does not translate arbitrary business text or certify full locale
coverage, production throughput, replay or historical output reconstruction.

## Prepare, render and retain provenance

1. Keep source/translation files in the owning project's reviewed version control.
   Supply explicit `Resource{Name, Data}` values, commonly using `embed.FS.ReadFile`.
2. Call `Prepare` once for a complete composition. A failure returns no catalog;
   it does not mutate an already prepared instance.
3. Retain `catalog.Snapshot()` and the consuming application's build provenance.
4. Call `Render(locale, messageID, arguments)` at the presentation boundary.
   Inspect the error and, on success, actual resource language and fallback reason.

The [independent consumer](../../../i18n/testdata/consumer/main.go) embeds its own
[English](../../../i18n/testdata/consumer/en.json) and
[Chinese](../../../i18n/testdata/consumer/zh.json) resources. Its
[artifact test](../../../i18n/consumer_test.go) builds a separate module from a real
module zip through a local file proxy with a fresh cache, no consumer replacements,
and only localization/failure/standard-library package dependencies. It also runs
an executable that reports the actual linked module versions. This is not a claim
that every other Fathomry package can be independently consumed without SDK setup.

## Resource profile

`fathomry.i18n/v1` selects one UTF-8 JSON envelope. Filenames do not infer locales.

| Envelope field | Meaning |
| --- | --- |
| `profile` | Required exact profile identifier; unsupported/missing profiles are refused |
| `locale` | Required language-script-region tag; canonicalized before collision checks |
| `license` | Optional array of metadata strings, never rendered; maintained first-party files carry the full project notice |
| `messages` | Nonempty array of definitions for that locale |

Each message has a namespaced `id` and `forms` object. Source `en` additionally
defines a nonblank `description`, optional `parameters`, and optional `count`.
Each parameter declares `type` and a nonblank `description` explaining meaning,
units and privacy expectations. A translation instead carries a `source`
fingerprint and may not declare parameters, a count role or nonempty description.
Source messages may not carry a translation fingerprint. Omitted parameter maps
normalize to empty maps; omitted count roles normalize to empty strings.

IDs have two or more dot-separated segments, starting with lowercase ASCII letters;
remaining segment characters are lowercase letters, digits, hyphens or underscores.
Parameter names start with an ASCII letter, followed by letters, digits or underscore.
IDs are resource identities, not failure codes. The `fathomry` namespace is reserved
by convention, not authorization. Names and IDs must not carry secrets.

Duplicate JSON members (including escape-equivalent names), filenames, or canonical
(locale, ID) pairs are errors, even when identical or stale. There are no overrides.
Every translation ID needs an English source definition. Unknown members, wrong
types/casing, null anywhere, invalid UTF-8, unpaired escaped UTF-16 surrogates and
trailing JSON values are refused. JSON object order is not composition policy.

### Template and cardinal profile

A `forms` object accepts only `zero`, `one`, `two`, `few`, `many`, `other`.
`other` is mandatory. A nonplural contract has only `other`; a plural English
source also requires `one`, and its `count` names a declared `number` parameter.
Translations can omit categories; a requested unavailable category is an explicit
English degradation, **not** a successful native substitution of `other`.

Form values are complete nonblank messages in restricted standard Go-template
syntax: literal text and single named field actions such as `{{.Name}}`.
Standard comments and whitespace trimming are accepted, but an entirely empty
parsed template is refused. No functions, pipelines, variables, method/field
chains, loops, conditions, definitions, template invocation or custom delimiters.
Every accepted variant is parsed and checked before publication, not just a sample.

Translations may reorder, repeat or omit declared parameters. Current translations
cannot introduce undeclared ones. Stale entries are still syntax/bounds validated,
but their old parameter references are not treated as current contracts or installed.

One private native bundle per resource locale uses go-i18n v2.6.1's CLDR 48 cardinal
rules; x/text v0.41.0 provides tag matching without a version upgrade. Fathomry does
not expose native types, loaders or per-call parser options. Its fixed parser only
returns already validated templates. This avoids native permissive loading, parser
first-use policy changes and silent duplicate replacement. Resources are trusted
within this restricted profile; this is not an arbitrary-code security sandbox.

### Arguments and quantities

`Arguments` is a caller-owned map, but only these exact dynamic types are accepted:

| Resource type | Go value | Presentation |
| --- | --- | --- |
| `string` | `string` | Valid UTF-8, unescaped; empty is a legitimate explicit value |
| `integer` | `int64` | Signed base-10, unchanged magnitude |
| `boolean` | `bool` | Machine scalar spelling, not localized labels |
| `number` | `i18n.Number` | Exact nonnegative decimal spelling |

Every declared parameter is required; extra names are errors even if a selected
translation ignores them. Nil, floats, other integer types, named types, objects,
containers, errors and callbacks are rejected without invoking their methods.
Convert deliberately at the presentation boundary instead of passing raw SDK data.

`Number` permits 1–9 integer digits, optionally followed by 1–6 fractional digits.
Only zero itself may have a leading integer zero. No signs, exponents, whitespace,
NaN, infinity or implicit rounding. Its zero value (empty string) is invalid.
Visible decimal zeros are significant: `1` and `1.0` can select different forms.
One validated value supplies both selection and interpolation; there is no separate
display count that can contradict it. This is cardinal selection, not ordinal,
date/time, unit, currency, grouping or general locale-sensitive numeric formatting.

## Locale and fallback policy

The API accepts one language tag, not preferences or an Accept-Language header.
Empty rendering locale means `en`, regardless of environment. Resources require
an explicit locale. Parsing rejects malformed/unrecognized tags, underscores,
whitespace, `und`, private-use tags, extensions and variants; the initial profile
supports language, optional script and optional region. Canonical aliases share
one identity. Resource locales must have native plural-rule support.

For valid input, exact canonical resource-locale matches win. Otherwise x/text
matches using at least High confidence; lower confidence is unsupported and selects
English. Matching has deterministic supported order: en first, then lexical locale
order. This is x/text matching, not a claim of RFC 4647 lookup. Related regional
matching is not itself a degradation; metadata exposes the distinction.

After selection, a missing/stale/failed translation falls directly to English,
not another regional catalog. There is no preference-list traversal. Source
English is complete for all accepted IDs.

| Result | Outcome |
| --- | --- |
| Exact/related usable resource | Nil error; `NoFallback` |
| Valid locale with no acceptable match | English; `UnsupportedLocale` |
| Selected locale lacks the ID | English; `MissingTranslation` |
| Translation source fingerprint differs | English; `StaleTranslation` |
| Selected translation cannot render, including missing plural category | English; `TranslationFailure` |
| Invalid input, missing source ID, excessive output, unusable catalog/source | Error and entirely zero result |

Successful results carry requested canonical locale, matched catalog locale,
actual resource locale and snapshot ID. Never label an English fallback as Chinese.
Rendering failures are safe public `failure.Code` identities; `errors.Is/As`
work without exposing input, resource text or native parser diagnostics/cause.
They describe presentation refusal, not the underlying business operation's effect.

## Bounds, ownership and concurrency

| Boundary | Limit |
| --- | --- |
| Files / file bytes / aggregate resource bytes | 64 / 1 MiB / 8 MiB |
| Total source and translation entries / locales | 2,048 / 32 |
| Parameters per message / parameter-name bytes | 16 / 64 |
| ID / locale / resource-name bytes | 128 / 64 / 256 |
| Description bytes, both message and parameter | 1,024 each |
| Template bytes / parsed top-level nodes per form | 8,192 / 128 |
| String argument / rendered output bytes | 4,096 / 65,536 |
| JSON value nesting depth | 16, with the root at depth zero |

Resource names must be valid relative slash paths, not `.`, and contain no control
characters. They are provenance labels, not paths the package opens. All files and
entries count, even stale ones. Resource limits are checked before decoding/copying.
Template byte limits bound parser work; unsupported nested syntax is rejected.
The exact output size is computed from validated nodes and scalar strings before
allocating the output buffer; a bounded writer additionally guards execution.
Refusal is not post-render truncation. These bounds do not impose process-wide
memory/admission limits on unlimited caller concurrency or the caller's own input
allocation; the caller still owns aggregate scheduling and admission.

Do not mutate resource bytes during Prepare or argument maps during Render.
After either call, input storage can be reused. Catalogs expose no mutable native
state or handles; Snapshot returns independent metadata slices. Concurrent first
use, subsequent rendering and metadata reads are supported. Catalogs start no
goroutines and need no Close. Operations are synchronous and bounded, so there is
no context or hidden cancellation policy. There is no per-render resource I/O.

Plain text remains untrusted for its destination. The presentation channel owns
HTML/Markdown escaping, terminal controls, length/layout and notification-card
rules; do not cast a result to trusted HTML. Optional presentation failure must
not erase the original outcome or interfere with essential cleanup.

## Compatibility, freshness and rollback

Keep these axes distinct:

- **Module/API:** the Fathomry module's public Go contract, not a separate i18n module.
- **Profile:** JSON/template/composition/hash interpretation; unknown required
  semantics are refused, not guessed.
- **Message contract:** capability-owned ID, meaning, parameter types/units and count
  role. Changed meaning or required arguments needs a new contract/ID and an explicit
  consumer migration; do not reuse an ID because the English sentence looks similar.
- **Source review:** fingerprints include all source variants, context descriptions,
  parameter meanings/types and count role. A digest detects edits, not semantic truth.
- **Resource snapshot:** exact packaged resources and composition, independent of SDK
  versions. Translation-only wording corrections change this without changing failure
  identity or an unchanged source fingerprint.
- **Actual renderer build:** consuming Go, go-i18n and x/text/locale data. Minimal
  version selection and replacements can differ from the framework's tested build.
  Retain `runtime/debug.ReadBuildInfo` or `go version -m` output separately.

To prepare a translation, first prepare its English resources and obtain the
corresponding `Snapshot.Sources` fingerprint. Put that value in the external
translation's `source` field only after review. A mismatch excludes the translation
and appears in `Snapshot.Stale`; it never silently approves itself. If an English
edit does not invalidate the translation, a reviewer may explicitly update its
fingerprint. Corrupt templates and unknown source IDs are errors, not stale success.

For profile v1, all digests use SHA-256 encoded as `sha256:` plus 64 lowercase hex
digits. Source fingerprint input is UTF-8 `Profile + "\n"` followed by canonical
JSON of the normalized source: fields in order `id`, `description`, `parameters`,
`count`, `forms`; parameter fields are `type`, `description`. Maps sort keys;
omitted parameters/count become `{}`/`""`; all accepted forms participate.
Canonical strings follow Go encoding/json escaping, including HTML characters
and U+2028/U+2029, without Unicode normalization or insignificant whitespace.

Each resource digest hashes its exact original bytes. Snapshot ID hashes the same
profile/newline prefix followed by canonical JSON of the name-sorted resource
array, each object containing `name`, then `sha256`. Input file order cannot
change the snapshot; names, whitespace and license bytes can. Neither digest is
authentication, a signature or proof of compatible semantics.

The [version tests](../../../i18n/version_test.go) exercise old/new consumers against
old/new resources, additive IDs, changed required parameters, singular-only edits,
stale review, translation corrections, unknown profiles and compatible rollback.
Rollback means retaining compatible code/contracts/resources together, not reverting
one translation file underneath incompatible callers. Construct a complete new
catalog separately; this package provides no runtime swap/update service.

A resource revision does not version Workflow history. Current presentation of old
facts can differ from the originally delivered report. Exact historical wording
also depends on inputs, renderer/locale data and channel behavior; archive artifacts
under the report owner's policy rather than claiming this package reconstructs them.

## Executable evidence and upstream basis

- [Resource, locale, bounds and concurrency tests](../../../i18n/catalog_test.go),
  [resource/license policy](../../../i18n/resource_policy_test.go),
  [typed failure projection](../../../i18n/presentation_test.go), and
  [fuzz boundaries](../../../i18n/fuzz_test.go).
- [Independent artifact consumer](../../../i18n/consumer_test.go) and
  [its concurrent consumer test](../../../i18n/testdata/consumer/main_test.go).
- [Standard Go templates](https://pkg.go.dev/text/template) supply the parser and
  executor; [Go language matching](https://go.dev/blog/matchlang) explains the
  language/script/region boundary.
- [go-i18n v2.6.1 localizer](https://github.com/nicksnyder/go-i18n/blob/v2.6.1/i18n/localizer.go),
  [parser cache](https://github.com/nicksnyder/go-i18n/blob/v2.6.1/internal/template.go),
  and [CLDR rules](https://github.com/nicksnyder/go-i18n/tree/v2.6.1/internal/plural).
  Native text-plus-error, permissive parsing and partial source hashes are not the
  public contract. go-i18n carries MIT and x/text carries BSD-3-Clause notices; retain
  their license obligations in distributions. First-party resources retain the
  project's full GPL notice.
- [Issue #73](https://github.com/frost-leo/fathomry/issues/73) owns the implemented
  scope; private investigation and raw execution records stay outside product docs.
