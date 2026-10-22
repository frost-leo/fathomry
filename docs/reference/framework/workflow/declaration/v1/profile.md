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

# Closed workflow declaration profile

[Workflow interface](interface.md) / Static document contract

**Audience:** authors and consumers of portable workflow declarations.
**Status:** implemented static admission and canonical comparison; execution is not implemented.

## Formats, identity and ownership

A bundle uses `fathomry.workflow-bundle/v1` with a nonempty `workflows` array.
Each entry contains exactly `namespace`, `code`, `version`,
`fingerprint_profile`, `spec_sha256`, `spec` and `modes`.
A mode contains exactly `code`, `revision`, `fingerprint_profile`,
`config_sha256` and `config`. The enclosing workflow supplies its parent identity.

Inner specifications use `fathomry.workflow-spec/v1`, semantics
`fathomry.item-dag/v1` and profile `fathomry.workflow-conflict/v1`.
Mode documents use `fathomry.mode-values/v1` and profile
`fathomry.mode-conflict/v1`. A recognized mode profile is not valid for a specification.
Unknown fields, aliases, formats and semantic extensions reject.

Namespace is exact, nonempty UTF-8, at most 255 bytes, without control characters.
This does not provision a Namespace or certify its service-side acceptance.
Workflow/mode codes match `[a-z][a-z0-9_-]{0,127}`; version/revision labels match
`[A-Za-z0-9][A-Za-z0-9._+-]{0,127}`. No trimming, case folding or SemVer inference occurs.
Inner IDs match `[a-z][a-z0-9_-]{0,63}` across the complete string; a newline suffix rejects.
A binding type starts with an ASCII letter and permits ASCII letters/digits and
`._/-` afterward, up to 128 bytes. Binding revisions use the version-label rule.

Display text, locale, layout, local database IDs, management defaults, enabled
flags, timestamps, artifact bytes and runtime handles are outside these documents.

## Complete specification

Every inner specification contains `format`, `semantics`, `contracts`,
`bindings`, `inputs`, `outputs`, `nodes`, `edges`, `controls`, `relations`,
`limits` and `completion`. Required empty collections are explicit.

| Family | Admitted static behavior |
| --- | --- |
| Contracts | Closed bounded objects, homogeneous arrays, strings/finite enums, exact integers, boolean, null and one nullable wrapper |
| Bindings | One declaration per type/revision; named port signatures, declared retry safety and effect capabilities |
| Calls | Explicit binding and argument references; one logical call site per declared ID |
| Nodes/phases | Finite acyclic outer graph; ordered nonempty sets of phase members; each call appears exactly once |
| Routes | Fixed sole route, or a declared call's value-delivery string enum matching the node's finite routes |
| Joins | `all` or `any`; static dependency and required-input availability checks |
| Gates | `chunk` or `run`, `all_succeeded` or `all_settled`, over declared strict ancestors |
| Effects | Exact call/binding effect and accepted/committed/visible guarantee references |
| Completion | `selected_paths_and_required_effects`; coherent `all_succeeded` or `allow_failed` allowance |
| Controls | Explicit numeric slot references and bounded same-kind `sum_le` relations |

These are declaration meanings, not controllers. There is no runtime root-item
enumeration, batch delivery, join/gate decision, effect reconciliation or terminal
accounting. A required root output is an obligation, not proof all routes produce it.
No arbitrary loops, expressions, recursive subgraphs or first-result racing joins exist.

### Data schemas and ports

Objects declare `properties`, `required` and `additionalProperties:false`.
Property names match `[A-Za-z_][A-Za-z0-9_]{0,63}`; required names must exist.
Arrays declare `items`, `minItems` and `maxItems`; strings declare
`minLength` and `maxLength`, with optional nonempty finite `enum`.
Integer bounds are explicit. Only `anyOf:[non-null-non-wrapper-schema,{type:null}]`
is admitted for nullability. External references, annotations, arbitrary unions,
regex and unknown JSON Schema keywords reject.

Ports specify `contract`, `delivery` and `required`. Delivery is `value` or
`complete_batch`; the latter describes a completed batch reference, not embedded
records. Required presence and nullability differ. Successful binding outputs
are required; missing is never converted into an empty batch or zero value.

Connections use the exact same local contract identity and delivery protocol.
An optional source cannot satisfy a required target. Call inputs use a node input
or a strictly earlier phase's output, not a same-phase producer.
For an any-join with several incoming edges, edge-derived ports may be absent.
Static checking is conservative; it does not infer general schema subsumption.

### Controls, values and budgets

A tunable slot contains exactly `{value:DECIMAL}` or `{control:ID}`.
Controls declare `kind`, `minimum`, `maximum`, optional fixed `default` and
an explicit set of `run`/`item` override permissions. No unused control, contract
or binding is admitted. Run-wide slots cannot permit item overrides.

Kinds are `count`, `duration_ms` and `ratio_milli`. Values are nonnegative
canonical decimal strings within signed int64, never floats or locale numbers.
Zero is not unlimited; execution count and duration slots are positive.
Milliseconds must fit int64 nanoseconds; attempts fit signed int32.
Retry backoff is 1,000–1,000,000 thousandths.

Run slots: `active_chunks`, `items_per_chunk`, `chunk_item_concurrency`,
`run_timeout_ms`, `cleanup_reserve_ms`.
Call slots: `start_to_close_ms`, `schedule_to_close_ms`, optional `heartbeat_ms`,
`maximum_attempts`, `initial_interval_ms`, `maximum_interval_ms`,
`backoff_milli`. Absent heartbeat is disabled; no other SDK default is inherited.

Check known operands independently, including fixed constraints when an unrelated
control lacks a default. Cleanup must leave a positive Run budget; chunk concurrency
must not exceed chunk size; attempt and heartbeat bounds must fit their enclosing
budgets; initial retry interval must not exceed its maximum; a call's total budget
must fit the declared business interval. Every included mode resolves all controls
and checks all joint constraints. `sum_le` uses overflow-safe arithmetic.

Read-only bindings cannot declare mutation effects. A `not_retryable` binding
cannot admit more than one attempt through any allowed value. Declaring
`idempotent` is not proof of idempotency. The validator does not prove all possible
parameter combinations feasible, implement overrides or reserve remaining runtime time.

## Normalization and fingerprints

Byte encoding follows [RFC 8785](https://www.rfc-editor.org/info/rfc8785/) on the
admitted integer/string domain. Reject malformed UTF-8, unpaired surrogates,
duplicate decoded names, extra JSON values, floating/exponent number lexemes,
negative zero and numbers outside ±(2^53−1) **before** canonicalization.
Larger exact control quantities remain strings.

Application normalization precedes JCS. Phase members, enum/required sets,
routes, edge selections, effect guarantees, gate observations, override scopes
and relation terms are sorted sets; duplicates are invalid, not silently removed.
Phase order is a sequence. String contents are not Unicode-normalized.
Object keys use UTF-16 order; HTML-safe escaping is not a different equality rule.

Digests are SHA-256 over `UTF8(profile) || 0x00 || canonical_inner_bytes`.
Wire digests are lowercase 64-character hex. Compare canonical bytes as well;
a digest match alone never establishes equality. The workflow fingerprint excludes
selected mode values and outer identity labels. Changing the control contract or
binding revision changes it; changing a mode value changes only that mode's content.
Different explicit versions may intentionally have identical fingerprints.

Within one file, identities are unique and each mode has a complete enclosing
specification. Across supplied files, equal parent specs may contribute different
mode subsets. Conflicting parents prevent a claim of mode equality beneath them.
File order does not change comparison.

File checking can normalize valid formatting/set order; it is not a database
stored-byte corruption verifier. It cannot discover unexported declarations,
prove a fresh source build, qualify an executable or determine execution readiness.

## Additional bounds

Alongside the [interface bounds](interface.md#ownership-limits-and-concurrency):
schemas nest to depth 16; one specification has at most 1,024 contracts/nodes,
8,192 edges, 4,096 bindings/controls/calls, 256 calls/phases per node, 64 ports/routes/
effects per owner and 256 relations. Object schemas have at most 256 properties;
string enums have at most 256 unique values of at most 1,024 Unicode scalar values.
Array bounds are at most 1,000,000 items and string bounds at most 1,048,576 scalars.
These logical data bounds do not authorize equally large Temporal messages.

See [compatibility and codec tests](../../../../../../framework/workflow/declaration/v1/canonical_test.go)
and [negative/ownership tests](../../../../../../framework/workflow/declaration/v1/bundle_test.go).
The checked health, orders, branching and gate fixtures describe static declarations
only; they are not evidence of successful workflow execution.
