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

# Internationalization catalog interface

[Documentation](../../../README.md) / Public package reference

**Audience:** CLI, capability and independent business-project authors.
**Status:** implemented v1 in-process Go contract; scalar/cardinal plain text only.
**Package:** `github.com/frost-leo/fathomry/i18n/v1` (package name `i18n`).

## Responsibilities and call sequence

Load feature-owned resource bytes under your own I/O limits, then call `Prepare`
with explicitly named `Source` values. A single immutable `Catalog` supplies
complete inspection, exact lookup and resource selection. Call `Resolve`, then
`Selection.Render` with one explicit locale and the approved scalar facts.
[Executable example](../../../../i18n/v1/example_test.go) and
[independent consumer](../../../../i18n/v1/testdata/consumer/consumer_test.go)
show composition without CLI, Framework, configuration or service initialization.

Resources stay with their feature. There is no global registration, implicit
catalog choice, ambient locale, discovery, reader callback, hot reload or override
hierarchy. The library does not log, write output or retry operations.
The current CLI root/project consumers explicitly share one prepared catalog;
their parsed language is invocation-local.

The production graph is `failure/v1`, the standard library and ten selected
x/text packages. Importing native plural also links its catalog/catmsg dependencies;
this implementation does not use their printer/template APIs. `failure/v1`
remains standard-library-only and does not import internationalization.

## Definitions, exact presence and selection

- `Inspect` returns every localized definition sorted by exact ID, English first,
  then canonical locale bytes. Arguments and declared forms are separately sorted.
  No pagination, silent omissions or current-locale filtering.
- `Lookup` canonicalizes a **nonempty** explicit locale but never matches/falls
  back. Its two presence booleans distinguish missing ID from missing translation.
  The definition is zero when translation is absent. Real text may equal its ID.
- Query IDs are at most 256 bytes and are never trimmed, case-folded or normalized;
  bounded unknown spellings report absence. `Resolve` instead returns
  `ErrMessage` for an unknown ID.
- `Resolve` treats an empty locale as English. Invalid locale is an error;
  authorized whole-message English fallback is success.
- `Selection` is opaque. Its owned `Metadata` distinguishes raw/canonical
  request, native matched tag, declared candidate/confidence, fallback reason and
  actual resource provenance. Mutating metadata cannot forge another selection.
- `Rendered` records rule category, used variant and same-resource form fallback.
  Those are independent of whole-message fallback.

Nil/zero catalogs and zero selections return `ErrCatalog`. A failed preparation
returns no catalog. A failed render returns a zero result, never partial/truncated
text. Public input/projection structs support keyed literals; do not rely on
positional layout as an extension mechanism.

## Resource schema and source freshness

Each document is strict UTF-8 JSON with required exact members `schema`,
`profile`, `owner`, `locale`, `messages` and optional `license_notice`.
Schema is `fathomry.i18n-resource/v1`; profile is `scalar-cardinal/v1`.
The notice is at most 32 strings of 256 bytes; third parties need not use the
project license. First-party resources retain the full project notice.

Owner and logical source name use `[A-Za-z][A-Za-z0-9_.-]*`, 1..128 ASCII
bytes. Source names are unique labels, not paths, authentication or publisher
identity. IDs are exact `owner:local`, where local uses that grammar with at
most 127 bytes and the complete ID is at most 256 bytes.

Every English entry has exactly `id`, `contract`, `context`, `args`,
`cardinal`, `forms`. Contract is an owner-managed 1..64-byte identifier using
the same grammar. Context is nonempty UTF-8, at most 1024 bytes.
Argument names use that grammar up to 64 bytes; `count` is reserved.
Each argument has exactly `kind` and nonempty `meaning` (up to 256 bytes).
Kinds are `string`, `int64`, `uint64`, `bool`.
Cardinal is a required JSON boolean, not null.

A translation has exactly `id`, `source`, `forms`. It inherits the English
contract/context/arguments/count mode. Source is the lowercase 64-hex SHA-256 of
the current English definition. Every declared ordinary argument must occur in
every declared form, including translations; authors cannot silently elide
required facts. This intentionally rejects some natural-language elisions.

Messages must be a nonempty array. Composition is atomic and order-independent.
Every ID needs exactly one canonical `en` source in the same call. Canonical
ID/locale duplicates reject, even identical entries. Disjoint documents may share
owner/locale. An English-only catalog is valid; translation-only/empty catalogs
are not. A missing auxiliary entry may fall back, but a supplied malformed or
stale entry rejects the whole catalog even if never displayed.

Reject decoded duplicate object names at every level (including escape aliases),
unknown/case-mismatched fields, wrong types, null/numbers, invalid UTF-8, unpaired
surrogates, BOM, trailing documents and unsupported profiles. No decoder repair
or last-wins merge is accepted.

### Digest algorithms

The source digest is SHA-256 of compact Go `encoding/json.Marshal` output for:

`[schema, profile, owner, id, contract, context, cardinal, args, forms]`

Args are name-sorted `[name, kind, meaning]` triples; forms are category-sorted
`[category, pattern]` pairs. Empty arrays are `[]`, not null. Marshal's HTML
and U+2028/U+2029 escaping applies; there is no trailing newline or Unicode
normalization. This is a Go-defined algorithm, not RFC 8785/JCS. Prepare English
alone and inspect `SourceDigest` when authoring translations; no private helper
is required. See [real resource fixtures](../../../../i18n/v1/testdata/resources/en.json).

The entry digest hashes compact JSON of
`[schema, profile, owner, id, canonicalLocale, sourceDigest, formsObject]`;
object keys follow Go's deterministic string-key ordering. Document digest hashes
the exact supplied bytes, including whitespace and license text. Inspection
exposes all three hashes, logical source name, definition and engine/data metadata.

Any English wording/context/argument meaning/contract/forms/count-mode change
invalidates the old source hash. A caller may deliberately omit a stale auxiliary
entry to permit fallback; the library never silently drops it. Hash equality
proves neither semantic compatibility, translation review, authenticity nor age.
Message owners must change contracts/IDs when semantics require it.

## Locale and cardinal rules

The qualified engine is **x/text v0.41.0**, source revision
`acdba6655fd45cdb5ab73c9d6a8981333bd65a39`, with language and plural **CLDR 32**
tables. It is not a latest-CLDR or general MessageFormat claim.
[Native cardinal API](https://pkg.go.dev/golang.org/x/text@v0.41.0/feature/plural)
and [native language API](https://pkg.go.dev/golang.org/x/text@v0.41.0/language)
are private implementation choices, not exposed public types.

Locales are 1..128 ASCII letters/digits/hyphens, successfully parsed by
`language.BCP47.Parse`. Unknown/partial tags, underscores, whitespace, `und`,
variants and all extensions in resources reject, including private-use `x`
sequences. Requests may carry a
Unicode `u` extension only, retained in request metadata; it changes no business
values, number syntax, timezone or currency. Exact lookup keeps that extension,
so it is not evidence of an extension-free entry.

Native-recognized reserved base/script/region identifiers (for example `qaa`,
`en-Qaaa`, `en-QM` and `sq-XK`) follow the same parsing/matching policy.
They are not `x` sequences and are not blanket-filtered using `IsPrivateUse`;
the pinned native region set includes codes classified as both private-use and
CLDR countries. Acceptance promises neither linguistic coverage nor namespace
authority. `x-private` and `en-x-private` remain invalid.

Canonical aliases collide before insertion (`iw` → `he`, `en-Latn` → `en`).
Matching never merges definitions. The native matcher uses English first, then
sorted canonical locales, with `PreferSameScript(false)`. Only High/Exact,
same canonical raw base and same inferred script are accepted. Weak,
cross-language or script-changing candidates use English with
`unsupported-locale`. If the accepted catalog-wide candidate lacks this ID,
English is selected with `missing-translation`; no per-key dialect search.
Changing the composed locale set can change matching, not entry provenance.
A synthesized matcher tag is never used as an authored resource key.

Allowed forms: `zero`, `one`, `two`, `few`, `many`, `other`.
All messages require `other`; noncardinal messages permit only `other`.
Declared forms need not all be reachable under native rules and remain inspectable.
Choose the whole resource first, then apply its actual language's cardinal rules.
An undeclared category uses that same resource's `other`, never an English fragment.

Count is a separate optional `*uint64`: nil means absent, zero is present.
Cardinal messages require it; other messages reject it. Selection and optional
`{count}` display use the same captured value over the full range
0..18,446,744,073,709,551,615, including above floating-point exactness.
The native integer operand is N below 10^7, otherwise 10^7 + N%10^7.
Pinned interpreter exact tests are below 10^7 and all moduli divide it; the
large/nonzero class and relevant residues remain unchanged, fitting 32-bit int.
Display formats N, never the representative. Engine upgrades must requalify this
proof, aliases/matching and rule data. Native equivalence is not an independent
linguistic oracle; tests include separate en/zh/ru/ar witnesses.

## Grammar, bounds and ownership

Patterns are nonempty UTF-8; whitespace-only content is valid. `{name}` expands
one declared scalar. `{{` and `}}` emit literal braces; percent signs are inert.
Unmatched braces, unknown names, specifiers, functions/methods, loops, recursion,
nested expressions, decimals, ordinals and general selects reject.

Arguments accept **exact builtin** string/int64/uint64/bool types. Distinct named
types, machine-width int, floats, json.Number, nil, pointers, objects and formatter
callbacks reject without invocation. A Go alias identical to a builtin is that
builtin. Names must be unique and exactly match the declared set.
Strings must be valid UTF-8, may be empty, and are copied into final output.
Formatting is ASCII base-10 integers and true/false, without grouping/conversion.

All limits below are inclusive, fixed profile boundaries, not tuning knobs:

| Boundary | Limit |
| --- | --- |
| Source documents / canonical locales | 32 / 32 |
| Bytes per document / aggregate raw input | 65,536 / 524,288 |
| JSON depth / aggregate values plus member names | 8 / 32,768 |
| Decoded string / member name bytes | 8,192 / 256 |
| Distinct IDs / localized entries | 512 / 1,024 |
| Arguments / forms per entry | 16 / 6 |
| Pattern bytes / references / compiled segments per form | 8,192 / 64 / 129 |
| Aggregate compiled segments | 16,384 |
| Dynamic string / total ordinary argument bytes | 4,096 / 16,384 |
| Rendered bytes | 65,536 |
| Complete inspection conservative byte envelope | 4,194,304 |

Preflight raw sizes/sums before decoding; bounded buffers also bound intermediate
token allocations. Charge nodes/depth during parsing, cardinalities before
insertion, references during compilation and aggregate segments before retention.
At most one individually bounded program is transient before aggregate rejection.
Before publication, each localized definition is charged 1024 fixed bytes,
six times the nine definition/provenance string lengths, 128 plus six times
name/kind/meaning bytes per argument, and 64 plus six times category/pattern bytes
per form. This bounds the complete owned projection including JSON escaping,
not exact allocator overhead. Every repeated expansion is charged using
remaining-capacity arithmetic **before** growing the final builder.

Inputs are borrowed only during calls; do not mutate them concurrently. Prepared
catalogs own resource data; callers may overwrite source bytes afterward.
Inspection/lookup/selection metadata owns nested slices. Catalog/selection copies
share immutable internals and support concurrent reads with different locales;
do not overwrite shared handles. Dynamic arguments are never retained.
There are no goroutines, streams, timers or cleanup handles to release.
These per-call/catalog bounds are not a process-wide quota or a concurrency SLA.

## Explicit modules and checked bindings

Optional `Catalog.WithModules` returns a fresh handle sharing immutable resources
with a complete explicit grouping. Each resource owner must be assigned exactly
once. Module IDs follow failure's semantic namespace hierarchy; exact `Owners`
retain their existing spelling/grammar, including uppercase, and may deliberately
differ from the module ID. This mapping changes neither message IDs, provenance,
locale matching nor fallback. No implicit longest-prefix membership or overrides
exist. Empty grouping modules are allowed; plain `Prepare` remains independent
of error definitions. `Modules` and `Module` return ordered owned snapshots;
ungrouped valid catalogs return an empty module inventory.

`PrepareBindings` combines a caller-chosen
[`failure.DefinitionCatalog`](../../failure/v1/interface.md#explicit-module-and-condition-atlas),
resource catalog and explicit `Binding` declarations. Each ID is `module:local`.
It verifies declaring modules, optional condition/revision, input contract/revision,
English message and message contract, exact argument mappings and cardinal mode.
Bindings can deliberately select another owner's resources. An ErrorFacts input
must belong to the selected condition; a PresentationInput belongs to the binding
module and is not advertised as a field on every returned error. Condition-free
help/success/operation-state bindings do not invent error identities.

Use `Bindings.Inspect`/`Lookup` for definitions and `Resolve` for a
`BoundSelection`. Its `Metadata` preserves ordinary actual-resource guarantees.
`Render` accepts exactly the mapped subset of known scalar fields, not an error
object or every field in the input contract. Required message fields must map to
required, known-by-contract inputs; optional/unknown inputs need an owner-defined
narrower projection. Enum inputs use exact declared string tokens. `CountFact`
must identify one required known uint64; its captured value is the only selection
and display count. There is no second count argument, implicit zero or callback.
Mapped field names, including CountFact, use i18n's argument identifier grammar.
The ordinary-argument byte budget excludes the separate cardinal count and charges
every ordinary mapping, including several mappings of one input field.

Compilation does not extract facts, choose a primary error, prove observed effects
or certify semantic equivalence of independently edited contracts. The producing
owner must select the same occurrence/result for identity and facts. Runtime
primitive/enum/set/budget checks supplement, not replace, that obligation.

Bindings are immutable and ordered by ID. Nil/zero handles and incompatible
declarations return `ErrBinding`; an unknown binding at Resolve returns
`ErrBindingMissing`, distinct from unknown messages. A valid empty binding set is
allowed. At most 1,024 declarations and 524,288 aggregate declaration plus retained
field string bytes are admitted. Binding strings are at most 256 bytes overall;
local IDs at most 127, surface/role and argument/field names at most 64. Declaration
inspection separately charges 512 bytes per binding, 128 per argument mapping,
and six times declaration string bytes against a 4,194,304-byte envelope.
Counts/bytes are checked before excessive allocation, with no partial publication.
Module grouping uses failure's 64-module/depth-8/126-byte-ID bounds and at most
32 supplied owners per group. All returned slice layers are independently mutable.

`i18n.Definitions` supplies this package's own condition declarations without
loading resources or translating errors. Neither it nor these optional APIs
introduces a mutable global registry, plugin system or mandatory bootstrap.

## Failures, presentation and compatibility

Package rejection errors are real directly inspectable `failure/v1` occurrences.
Conditions distinguish invalid catalog/resource/locale/arguments, capacity and
missing message/binding and incompatible bindings. Diagnostics contain static identities only, no rejected data,
native parser errors or arbitrary formatter output. They are not recursively
translated through the failed catalog. Detailed precedence among simultaneously
invalid inputs is not a caller contract.

An operation/presenter owns condition-to-resource bindings. Select an occurrence
explicitly and get both its direct condition and required typed facts from that
same object. Do not pair outer `failure.Inspect` with recursive `errors.As`.
Equal-code/equal-type inner or sibling failures may describe different facts.
Unknown errors/raw joins use host-owned generic presentation, never a guessed
primary. Caller-selected catalogs and bindings can customize wording while
leaving identity, effects and original causes untouched. The
[independent occurrence tests](../../../../i18n/v1/testdata/consumer/consumer_test.go)
exercise outer/inner, reversed siblings, explicit cause selection and customization.

Plain text is not automatically safe HTML, Markdown or terminal output. Channels
own approved argument selection, escaping/layout and their extra expansion budget.
The CLI accepts only control-free literal resources and retains the first hard
presentation error even when Cobra ignores Help/Usage callback returns. It joins
that error independently of usage failures, does not retry business effects,
and never calls a failed underlying writer again. If normal host diagnostic
rendering fails, it attempts one fixed `fathomry.cli.presentation_failed` line
(under 96 bytes including newline), then stops.
CLI host and project conditions have owner-local resource bindings. A sole i18n
failure retains its semantic identity rather than acquiring a generic invocation
condition. Independent rendering, operation and writing failures are explicitly
joined; no member becomes primary by traversal order or recursive localization.

Go API v1, resource schema/profile, owner message contract, content/source digest,
root-module revision and engine/data revisions are different axes. Current
catalogs do not retain historical resources. Runtime handles/projections are not
a durable message/error protocol, Temporal codec or replay guarantee. No nested
module, latest facade, automatic cross-version conversion, decimal/unit/currency
formatting, remote packs, UI or configuration subsystem is provided.

Run `go test -race ./i18n/v1 ./cli/...` using the module-selected Go toolchain.
The i18n suite includes a genuine offline unrelated module and exact production
dependency guards. Linux/amd64 consumer/process checks do not certify other OS
runtime behavior; short fuzz runs do not prove absence of defects.
Scope: [Issue #93](https://github.com/frost-leo/fathomry/issues/93).
