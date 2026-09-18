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

# Public failure contract

[Documentation](../../README.md) / Public package reference

**Audience:** independent Go consumers and public capability authors.
**Status:** implemented in-process foundation; no public operations, full error
catalog, translator, execution runtime or durable error protocol.
**Package:** `github.com/frost-leo/fathomry/failure`.

## Responsibilities and use

Define constant, namespaced `Code` values in the capability that owns their meaning.
Construct an immutable `Error` using `New(code, cause, optionalAttributes...)`. Pass `nil` when native
implementation details must remain internal; retain those details in the independent
result/evidence path. Match an intentionally promised identity with `errors.Is`.
No registration, SDK, Provider or localization dependency is required.

`Error` is a value handle, not a mutable envelope. Its `Code`, `Diagnostic` and
`Failure` methods describe that occurrence. `Inspect(err)` calls only the supplied
error's `Failure() Error` method; it never traverses wrappers or joins. The returned
boolean must be checked. An ordinary `fmt.Errorf` wrapper has no described occurrence
unless its owner explicitly implements that contract.

See [executable examples](../../../failure/example_test.go) and
[source documentation](../../../failure/doc.go). Effects, attribution, policy and
presentation have separate [owners](../../architecture/errors-and-evidence.md).

## Identity and compatibility

A code has at least two dot-separated lowercase ASCII segments and is bounded by
`MaxCodeBytes`. Segment grammar is specified by [Code.Valid](../../../failure/errors.go).
The `fathomry` namespace is framework-owned by convention. Other authors choose
namespaces they control; this is neither authentication nor a registry.

| Input or observation | Meaning |
| --- | --- |
| Valid unfamiliar code | Preserved failure identity; no catalog lookup or substitution |
| Zero/malformed code passed to `New` | `failure.Invalid`; never success |
| Zero `Error` | `failure.Invalid`, no cause or optional attributes |
| Nil error or direct typed nil passed to `Inspect` | No occurrence; boolean is false |
| Bare `Code` passed to `Inspect` | Matching target, not an occurrence; false |
| Unmapped native error | Capability chooses a documented public fallback; original evidence stays internal |
| Unknown external effect | Separate result/evidence fact, not any of the above |

Matching is exact: prefixes do not imply classification. `errors.Is` also searches
causes, so matching a cause's code does not make it the outer code. Code text is not
a localized message; equal free-form error messages do not establish identity.
Malformed bare `Code` values are not supported matching targets; native Go equality
can still make any identical comparable values match themselves.

### Version owners

The package uses the existing framework Go module version and top-level import path.
This issue adds no release tag, separate submodule, `failure/v1` directory or
per-occurrence version field. Such a field would not preserve behavior on its own.
[Versioning architecture](../../architecture/versioning.md) separates these axes:

- **Public code/operation semantics:** never reuse a code for a different meaning.
  Adding a code does not permit replacing the code returned for an existing
  condition. Preserve promised `Is/As`, cause visibility and machine details, or
  explicitly version the changed operation/API. An SDK upgrade alone changes none.
- **Typed machine details:** the defining capability owns names, types, units,
  presence, bounds and completeness. Additive refinements must preserve old-client
  observations. Incompatible details need an explicitly different owner-defined
  type/operation contract and a migration or refusal path, not silent reinterpretation.
- **Presentation resources:** different channels/catalog revisions can select
  different resource keys for one code without changing machine meaning.
- **Future durable protocols:** wire/schema versions and unknown-version handling
  belong to a future protocol owner, independently of Go module and SDK versions.
  There is no wire reader or migration engine here.

[The old-client fixture](../../../failure/extension_test.go) checks exact code,
a promised public cause and common `errors.As` behavior before/after a typed
refinement. Its changed-identity control is rejected. This is not a promise of
compatibility with the withdrawn pre-framework Definition/Identity/Attribution API.

## Machine details versus optional diagnostics

Required machine details belong to typed capability errors, not an unrestricted
bag or a closed common scalar union. A capability can embed an **unexported alias**
of `failure.Error` to inherit identity, safe formatting/logging, JSON refusal and
`Failure` inspection without exporting a mutable base field. The promoted `As`
method preserves inspection as the common `failure.Error` value without introducing
a fake cause. Native Go `errors.As` still discovers the capability's concrete type.

The independent [validation consumer](../../../failure/testdata/configcheck/errors.go)
freezes a bounded collection of field/rule violations, clones input and output,
and returns explicit completeness. Invalid/oversized required details retain the
primary identity but provide **no supposedly complete partial list**. A caller
requiring those facts must refuse to act when they are unavailable. This pattern
does not restore missing evidence or prove success. The separate
[orders consumer](../../../failure/testdata/orders/errors.go) validates a typed server
delay and distinguishes absent, present-zero and unavailable detail; that hint is
not a retry decision. These are test-owned consumers, not production capability APIs.

Capability authors must validate their schemas, document limits, freeze storage
and test their actual method sets; overriding promoted methods can remove safety.
The core never runs a generic deep copy or arbitrary detail serializer.
There are no transforming instance methods that, when promoted, could silently
return only an extension's base and discard its required typed detail. `Failure`
and `Inspect` deliberately project the common occurrence; retain the original
typed error when inspecting capability details.

The constructor's attributes supply **optional public diagnostic** data.
They must not carry facts on which program correctness depends:

- Attribute count, name/value byte lengths and total name-plus-value bytes are
  limited by the exported `MaxAttributes`, `MaxAttributeNameBytes`,
  `MaxAttributeValueBytes` and `MaxDiagnosticBytes` constants.
- Names have a fixed ASCII grammar; values must be valid UTF-8 without control
  characters. Syntax validation does not recognize secrets or make arbitrary text
  safe for a target channel. The caller must approve all attributes as public.
- An invalid, duplicate or oversized set is **entirely omitted** and
  `Diagnostic.Omitted` becomes true. The same code and cause survive. Required
  details have their own validity contract; this flag says nothing about them.
- Empty values are present empty strings; missing names are absent. Nil and empty
  sets both mean no diagnostics requested. Occurrences have no mutation/merge API.
- Valid inputs are copied, including string backing bytes, then sorted by name.
  `Diagnostic` returns independent slice storage; callers may mutate the snapshot.

## Cause visibility, aggregation and ownership

`New` borrows one deliberately public cause. It never calls its `Error`, `Is`,
`As`, `Unwrap`, formatter, logger or serializer. Direct nil/typed-nil causes
are absent; nested external graphs, including nested typed nils, are not normalized.
Use `errors.Join` for deliberate multi-cause matching and preserve `ctx.Err()`
and `context.Cause(ctx)` separately when both are promised.

`Unwrap` is a deliberate raw boundary. Exposing a native type commits the
operation to that supported observation; retaining it internally does not require
making it public. If an operation withholds native causes, neither copying their
text nor reusing their arbitrary codes is a safe substitute.
See [Go's error API guidance](https://go.dev/blog/go1.13-errors#errors-and-package-apis).

A join is not a primary/cleanup model. Reversing joined children changes the first
`errors.As` result. The operation/result owner must retain explicit Primary and
Cleanup references and independent evidence; it may also provide an aggregate for
matching. There is no implicit severity ordering, classification inheritance, retry
flag or Item/Run disposition. Successful empty output and normal filtering are
not manufactured into errors. Handling an error does not acknowledge evidence.

Owned construction/inspection has bounded work and storage for code/diagnostics.
One cause reference can retain an arbitrarily large graph: its lifetime, concurrency,
acyclicity, callback behavior and overall budget remain the caller's responsibility.
No recursive graph walk, deep copy, cycle defense, hard native-memory bound or
callback termination guarantee is provided. Standard `errors.Is/As` invokes external
methods and may block or panic. External `Failure` methods are similarly trusted.

Copying an `Error` value shares immutable state. Owned reads can run concurrently;
assigning to a shared caller-owned variable still requires synchronization.
Caller slices must not be mutated during construction. Borrowed errors and typed
extensions obey their own documented concurrency contracts.

## Safe diagnostics and presentation

`Error()` returns only the bounded code, providing a catalog-free fallback.
All `fmt.Formatter` verbs, including `%#v` and value/pointer forms, use this safe
projection; `%q` quotes it. Width, precision and flags are ignored, so they cannot
expand it or reveal private fields. `%T`/`%p` retain Go's native type/address
behavior and are not semantic output.

Standard `slog` projection exposes only code and the optional diagnostic-omission
flag. Neither default formatting nor logging prints attribute values, required
details or cause text. Formatting/logging destinations are caller-owned; producing
a value does not itself emit a log or call an observer.

Explicitly inspecting a snapshot or raw cause is different. In particular,
`errors.Join(safeOccurrence, rawCause).Error()` can print raw text. Caller wrappers,
reflection, unsafe access, arbitrary callbacks and overridden extension methods
are not sandboxed. The [rejecting tests](../../../failure/errors_test.go) preserve
that counterexample rather than claiming universal sanitization.

A presenter receives explicit code and supported typed details, then selects its
own resource key and locale. Tests use two fixture locales and two resource keys,
missing resources and a renderer refusal. Rendering failure or mutated returned
detail does not change identity or independently retained evidence. No production
locale, catalog, translator dependency or channel-escaping policy is selected here.

## Determinism and transport exclusions

Construction, owned inspection and attribute ordering depend only on explicit
inputs. There is no implicit clock, random ID, environment/locale/catalog read,
I/O, logging, metric emission or goroutine. Arbitrary external callbacks and caller
formatting destinations are not covered by that purity guarantee. These local
properties are not a Temporal replay qualification.

Occurrence methods refuse JSON encoding and decoding, rather than silently encoding
`{}` or traversing causes. Standard JSON may encode a nil `*Error` as `null`
or clear a pointer through a pointer-to-pointer `null` target without calling
occurrence methods; neither is an error round trip.
Explicit diagnostic/detail projections are still not an endorsed durable schema.

The currently selected Temporal SDK's default converter is **not** a semantic
round trip: different codes of one Go type become the same application-failure
type, joined errors do not preserve a structured multi-cause graph, and ordinary
wrapping can change native cancellation's outer classification. An Activity returning
`(partialResult, error)` does not automatically deliver that ordinary result to
its Workflow. A future explicit partial-output/effect protocol is still required.
No converter or Workflow command/payload change is included; service retries,
replay and process-loss recovery are not qualified by this package.

## Executable evidence

- [Core tests and fuzz target](../../../failure/errors_test.go): bounds, nil,
  copy/concurrent-read safety, safe formatting/logging, JSON refusal and raw controls.
- [Two typed consumers, compatibility and presenters](../../../failure/extension_test.go).
- [Independent artifact consumer](../../../failure/consumer_test.go): `GOWORK=off`,
  file-proxy module zip, fresh module cache, no consumer replacements or network,
  build and race behavior; dependency graph contains only `failure` and the standard
  library beyond the consumer's own packages. This is not whole-SDK release packaging.
- [Actual Viper and independent evidence fixtures](../../../internal/conformance/public_failure_test.go):
  native malformed input, hidden causes, primary/cleanup, two frozen owners,
  ended wait/late completion, partial facts, empty results and ordinary filtering.
- [Private dependency guards](../../../internal/conformance/imports_test.go):
  mechanisms and Providers never depend on the public package.

Scope and acceptance originate in [#71](https://github.com/frost-leo/fathomry/issues/71).
Working design and preparation counterexamples stay in the local reference workspace.
