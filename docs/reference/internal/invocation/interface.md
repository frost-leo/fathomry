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

# Invocation interface

[Documentation](../../../README.md) / Internal package reference

**Audience:** in-module capability and Provider authors.
**Status:** implemented technical calls, not a business execution engine.
**Package:** `github.com/frost-leo/fathomry/internal/invocation`.

This package owns one state per accepted technical call: budgets, retained local
use, typed results, independent evidence and optional observation. It does not
own business retry/disposition, durable recording or a public capability API.

## Capabilities and call sequence

| Surface | Calling contract |
| --- | --- |
| `Begin`, `BeginNested`, `Request` | Reserve evidence before resource admission; enter native work only after acceptance |
| `Budget.Context` | Give each actual phase a caller-owned context |
| `Call`, `Scope`, `Guard` | One producer, explicit retained users and a guard before any possible early callback |
| `Resolve`, `Finish`, `Release`, `Complete`, `Execute` | Keep reporting and ending local use separate |
| `Outcome`, `Result`, `*Receipt` | Read typed facts without producer/evidence-release authority |
| `Inbox`, `DeliveryRecord` | Keep bounded evidence independently of the direct caller |
| `Observer`, `Event`, `ExportOne` | Optional lossy observation, not required evidence |

Obtain an `Access` using `resource.AccessFor` with an explicit [resource allowance](../resource/admission.md),
create a bounded `Inbox`, then `Begin`. Drive the SDK using appropriate phase budgets
and retained use, report facts, confirm local termination, and let the independent
receiver inspect/release its delivery. A rejected `Begin` accepts no SDK operation
or receipt. An accepted call cannot be abandoned when the caller stops waiting.

## Ownership and zero values

The receipt is concrete `*Receipt[T]`, not a public interface. `Call`/`Receipt` copies
share one state; `Scope`/`Guard` copies retain the same lease responsibility. Do not
copy owner objects such as `Inbox`, `Observer` or `DeliveryRecord`; use their pointers.

Nil/zero `Receipt.Result` returns a zero value and false; Wait methods return `ErrWait`.
`Scope{}.Hold` fails with a resource admission error. Nil/zero `Call` methods do not
invent acceptance/completion. A nil `Observer` disables observation; exporting needs
a correctly constructed observer, context and callback.

`Result` metadata is copied, while generic values and native errors remain borrowed.
Freeze/bound transferred data and define its concurrent use. `Runtime` call/result/
receipt/evidence values refuse ordinary JSON, not arbitrary nested metadata or
standard Go error wrappers. No durable format is implied.

## Per-call association and attempts

`Request.Name` uses the existing 1-64 character source-label syntax. Each call
supplies `Request.Correlation`, a `fault.Correlation` value:

| Field | Meaning |
| --- | --- |
| `Call` | Required logical call ID; uniqueness belongs to the caller's documented scope |
| `Parent` | Optional logical parent, not the call itself; nested calls must name their actual parent |
| `Owner` | Optional opaque caller association; not quota scope, authorization or proof of effect ownership |

Nonempty correlation IDs use 1-128 ASCII letters, digits, dots, underscores or
hyphens. Validation bounds representation, not identity authenticity or privacy.
Background calls need a logical `Call` ID, not an invented Run. Source identity,
original composition scope and configuration revision come from the actual record;
aliases and user input do not overwrite them. `Scope`-label uniqueness remains
composition's responsibility, not a global registration guarantee.

Run, Item and framework execution attempts do not enter this core. A real boundary
must freeze their attribution before work begins and retain it for independent
evidence reception, including after an early wait return. The
[boundary fixtures](../../../../internal/conformance/boundary_test.go) use an immutable,
bounded typed envelope: envelope presence is distinct from business-data presence.
They do not add a global `Call`-to-Run map or a production Adapter/runtime.

`Call.Attempt` records a genuinely intercepted SDK attempt immediately before its
entry. It does not execute or retry anything. `AttemptsKnown=false, MaxAttempts=0`
means observations are lower bounds. Exact accounting requires proven complete
instrumentation and a positive local maximum. The configured attempt allowance
must include necessary cleanup; exhausting it does not establish rollback or
release. Parent and child calls have separate attempt observations; do not sum
physical batches or terminal Items by counting wrappers.

SDK-owned retries, authentication refresh, batching and background traffic need
explicit interception, native bounds or an unsupported status. A local logical-call
ceiling is not account-wide request rate control. This foundation neither introduces retries nor implements a distributed shared-limit backend.

## Results and failure boundaries

Wait errors describe waiting, independently of `Result`.Err's primary/cleanup facts.
A nil error is not proof of an external effect. Initial publication, final reporting
and local subtree release are separate observations. Cancellation is not rollback
or permission/evidence release. Preserve partial and unknown output explicitly.

[Technical errors](../fault/interface.md) retain native inspection. The
[error/evidence architecture](../../../architecture/errors-and-evidence.md) owns
future framework attribution; no mutable current Run is stored on shared clients.

## Details and executable evidence

- [Budgets](budgets.md), [completion](completion.md), [evidence](evidence.md), [observation](observation.md).
- [`Call` source](../../../../internal/invocation/calls.go), [result source](../../../../internal/invocation/results.go), [evidence source](../../../../internal/invocation/evidence.go).
- [`Call` example](../../../../internal/invocation/example_test.go), [lifetime tests](../../../../internal/invocation/calls_test.go), [evidence tests](../../../../internal/invocation/evidence_test.go), [boundary tests](../../../../internal/conformance/boundary_test.go).

Run `go doc -all ./internal/invocation` for exact declarations. Foundation:
[Issue #5](https://github.com/frost-leo/fathomry/issues/5); private boundary:
[Issue #15](https://github.com/frost-leo/fathomry/issues/15).
