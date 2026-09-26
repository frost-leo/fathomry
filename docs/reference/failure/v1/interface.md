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

# Public in-process failure interface

[Documentation](../../../README.md) / Public package reference

**Audience:** capability, Adapter and independent business-project authors.
**Status:** implemented v1 Go contract; no durable conversion or production mappings.
**Package:** `github.com/frost-leo/fathomry/failure/v1` (package name `failure`).

## Responsibilities and call sequence

Declare a code-owned `Condition`, call `New` with explicitly public causes, and
check its separate construction error. Use `Inspect` for the directly supplied
occurrence; use ordinary `errors.Is/As` for intentional recursive membership.
See the [executable examples](../../../../failure/v1/example_test.go) and
[Go declarations](../../../../failure/v1/error.go).

The production dependency closure is standard-library-only. There is no nested
module, unversioned facade, registry, native-error translation, optional annotation
bag, resource acquisition, logging, locale selection or retry policy.
`internal/fault` remains private and unchanged. Existing CLI return values are
not converted: a raw CLI aggregate need not be a directly inspectable occurrence.

## Identity, absence and bounds

`Condition` is a comparable string type. Its exact ASCII grammar is
`[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)+`, at most 128 bytes. The prefix before
the final dot identifies the owner's namespace; the last component identifies
its condition. There is no normalization, namespace authentication or registration.
A valid unfamiliar code is preserved. Grammar does not establish privacy,
authenticity or safe metric cardinality: never derive codes from secrets,
request IDs, payloads or untrusted dynamic labels.

`New` returns `(nil, ErrCondition)` for invalid required identity, or
`(nil, ErrCauses)` for more than 32 supplied direct cause slots, including nil
slots. Identity validation takes precedence. Rejected inputs remain caller-owned
and are not retained; no accepted cause or semantic detail is silently truncated.
There are no optional diagnostics whose rejection could replace valid identity.

Literal nil causes are omitted. Non-nil typed-nil interfaces, duplicate causes
and caller-supplied SDK objects are retained exactly. Counts describe direct
retained interfaces, not tree size. A large/cyclic foreign graph is not cloned,
normalized or bounded by this package.

| Supplied value | Direct `Inspect` |
| --- | --- |
| Valid `*Error` | The same pointer and `true` |
| Conforming non-nil `Occurrence` extension | Its directly exposed valid `*Error` |
| Ordinary percent-w wrapper, raw join, foreign error or bare Condition | `nil, false`; no primary is inferred |
| Nil interface, typed-nil core/extension, nil or zero extension result | `nil, false`; no descendant fallback |
| Non-nil zero `*Error` | Invalid, not success or a fabricated condition |

Only a nil error interface means no error. A typed-nil interface stays non-nil.
`*Error` is the canonical runtime error; `Error` values do not implement
`error`. Nil pointer methods report absence; a zero value reports invalid
occurrence. Both produce a zero `Diagnostic`, which is not a success result.

`errors.Is(occurrence, condition)` matches its exact valid condition and then
searches deliberate causes. A separately constructed occurrence with the same code
does not match as an occurrence target; pointer identity still follows Go rules.
`errors.As` reaches declared core/extension/foreign types through their supported
method sets. Bare Condition targets are comparable errors, so comparing two equal
invalid Conditions directly with `errors.Is` still follows ordinary Go equality;
this does not construct or validate an occurrence.

`Unwrap() []error` always uses Go's multi-cause contract, even with one cause.
The singular `errors.Unwrap` helper does not traverse it. Traversal order is
not a primary/cleanup policy. Exposing another cause can change control flow and
is an API commitment, not necessarily a compatible extension.

## Ownership and safe output

Construction clones the condition's backing storage and copies the bounded cause
slice. `Unwrap` returns fresh slice storage; `Diagnostic` returns the condition
and direct `CauseCount` as a comparable value-only projection. Supported
projection construction is zero or keyed literals, not positional literals.
Error copies share immutable state; their comparability is state identity, not
semantic-code equality. Do not overwrite shared Errors. Cause objects remain
borrowed; the owner must govern lifetime, mutation and concurrent inspection.

Owned construction, direct core inspection, formatting and projection never invoke
cause formatting, matching or unwrap hooks. They neither traverse arbitrary graphs
nor create goroutines, clocks, global registrations or correlation IDs. Concurrent
owned reads are supported, provided callers do not mutate the occurrence.
Foreign matching may panic, block or cycle; arbitrary `errors.Is/As` is not
a safe or bounded traversal API.

| Surface | Supported behavior |
| --- | --- |
| `(*Error).Error`, `(*Error).LogValue` | Code-only baseline; nil is absence and zero is invalid |
| Valid `fmt` directives dispatched to Format for Error value/non-nil pointer | Same baseline; q quotes, flags/width/precision do not expand that output |
| `fmt` for nil pointer | Standard fmt nil presentation; do not directly call a promoted value-receiver method on nil |
| Standard slog handlers for `*Error` | Code-only LogValue, including nil |
| Standard slog handlers for Error value | Text uses safe Format; JSON reports serialization refusal; use `*Error` for normal logs |
| `encoding/json` for non-nil pointer/value | Explicit `ErrSerialization`; no accidental empty-object protocol |
| JSON nil pointer | Standard `null`, meaning absence, not a serialized occurrence |

JSON reconstruction into an Error is refused without modifying it. This is not an
interception promise for every foreign codec, formatter or embedding method set.
Go's fmt owns `%T`/`%p`, width padding on those paths and malformed-format
diagnostics; these can bypass Format entirely. In particular, an invalid Condition
passed to `%p` or Sprintf's unsupported `%w` can expose its raw rejected string.
Those operations, raw cause inspection, foreign wrappers, reflection/debuggers and
deliberate casts are outside the output safety boundary. Use valid Formatter-
dispatched directives such as `%v`, `%+v`, `%#v`, `%s` and `%q`.
Scalar codes and diagnostics are not
automatically durable error schemas. Do not parse human Error text.

## Capability-owned facts and presentation

An extension explicitly implements `Occurrence` and returns its stable current
core through `Failure() *Error`. Interface assertion discovers the extension;
reflection is used only to reject typed-nil values before invoking its accessor.
No accessor or overriding foreign method is sandboxed.

The owner defines detail fields, units, absence/unknown semantics, bounds, copying,
lifetime and concurrent-read rules. The core does not deep-copy or automatically
print them. Test pointer/value/nil forms, exact method sets, Go matching, fmt,
slog and serialization; simply embedding an Error pointer does not certify a
safe extension. A value-receiver extension's methods cannot be called directly
on a nil pointer. fmt and encoding/json have special nil handling; slog may instead
recover a promoted LogValue panic into a stack diagnostic. The fixture therefore
defines nil-safe LogValue on explicit pointer receivers. Populated pointer forms
log code-only output; value forms use safe Format for text and JSON serialization
refusal for JSON logs. Nil or zero-core pointers log absence and remain uninspectable.

The [independent consumer fixture](../../../../failure/v1/testdata/consumer/consumer_test.go)
contains two unrelated detail owners, copying/accessor controls and explicit
forwarding methods. It is an example of owner responsibilities, not a new catalog
or SDK for extending arbitrary runtime objects.

A presenter must take identity and safe typed facts from the **same supplied
extension**, or from an explicitly owner-selected occurrence-and-facts binding.
Never combine outer `Inspect` with a recursive `errors.As` lookup for
interpolation: same-code, same-type descendants can describe different facts.
A deliberate cleanup/cause selection takes both its identity and its facts.

A test-owned display map demonstrates changed wording with unchanged identity.
Missing presentation preserves the original failure and falls back to its safe
code; an unselected aggregate uses the host's generic safe fallback. A condition
is not a message resource ID: title, explanation and remedy may be separate
resources. No translation catalog or mandatory locale/template fields are supplied.

Partial outputs, effect uncertainty, cleanup obligations, credential generations
and normal empty/filtered/superseded outcomes stay with their operation owners,
not in a universal Error/result model. A timeout does not prove no external effect.

## Versions and compatibility

The `/v1` path versions the complete public Go contract: behavior, method sets,
construction, zero semantics, matching, comparability, ownership and safe output,
not merely struct layout. Supported behavior changes can be breaking without
changing a field type. Additions must preserve old callers, including supported
literal construction and zero values. Never repurpose a condition; its capability
owner governs meaning, fields and newly exposed causes. Replacing an operation's
condition with a more specific code is not automatically compatible.

This directory is an ordinary package under the existing root module, not an
independently released Go module or stable product-release claim. Consumers pin one
root-module revision. The Go contract, root build, condition identity, capability
detail contract and future durable schema are separate version axes.

A future incompatible contract may use a sibling `failure/v2` while supported
v1 remains. No automatic cross-version assignment, matching, conversion or
latest-version alias is promised. Creating v2 would not authorize removing v1.

## Native boundary evidence and limits

[Boundary tests](../../../../internal/conformance/failure_v1_test.go) exercise
malformed JSON through the selected private Viper integration, asserting the native
`decode` phase and actual parser types, with valid-input control. A test-owned
receipt retains private evidence after the mapper returns; it is not a production
Adapter or diagnostic store. Boundary tests live outside the public package so
external `go mod tidy` need not load their native test graph.

On the selected Temporal v1.49.0 local replacement, fresh conversion of actual v1
occurrences with distinct conditions yields the same native ApplicationFailure type
`Error`. Code text survives only as Message, with no structured Details and no
multi-cause graph. Decoding does not restore v1 identity or matching. The converter's
`NonRetryable=false` is not a retry authorization or executed server retry.

A bounded SDK-local Activity test confirms that returning 7 plus an error does not
deliver the ordinary 7, while 7 plus nil does; explicit native failure Details have
a separate successful control. Both actual v1 and native failures are exercised.
No server, dispatch, history replay or durable bridge is qualified.

Go matching deliberately sees independent cleanup cancellation. The selected
[poller](../../../../third_party/temporal-sdk/internal/internal_task_pollers.go)
conditionally uses recursive cancellation matching when `cancelAllowed` is true.
That routing hazard is source evidence, not an executed dispatch witness.
Operation-owned conversion, native control compatibility and replay remain future
work; do not return arbitrary v1 trees across Temporal as a lossless protocol.

Run `go test -race ./failure/v1` and
`go test -race ./internal/conformance -run '^TestPublicFailure'` from the root
using the selected toolchain. The first includes an offline unrelated module,
private-import rejection and a stdlib-only production-closure check. The native
checks use the root's real selected replacements.

Scope: [Issue #92](https://github.com/frost-leo/fathomry/issues/92).
