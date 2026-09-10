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

# Resource binding and ownership

[Documentation](../../../README.md) / Internal package reference

**Audience:** composition and Provider authors.
**Status:** implemented cooperative resource management.
**Package:** `github.com/frost-leo/fathomry/internal/resource`.

See the [resource interface](interface.md) and [lifetime architecture](../../../architecture/resource-lifecycle.md).

## Binding and ownership

Factories return a `Resource[C]` to composition. After all factories and optional
readiness checks succeed, `Bind` returns its capability `C` plus actual source
identity. Bind accepts only the exact selection token; a new token with the same
name does not match, and a missing source never falls back to another instance.

The Provider must supply an actual non-owning facade, including its dynamic type,
callbacks, and returned values. Assigning a raw client to a narrow interface does
not hide its other methods. Nil capabilities are rejected during initialization.
The generic manager cannot audit arbitrary compiled Provider code or provide a Go
security sandbox. Its tests verify the supplied facade and callback contracts,
not future SDK compliance.

Mode, account/session, and capability-specific constraints belong to the typed
Provider/capability contract and outer authorized selection. This manager does not
infer them from an endpoint or offer unrestricted business-time lookup. It does
not place mutable per-call attribution on a shared client. The
[controlled-call implementation](../invocation/interface.md) adds scoped admission through
`WithLimits`/`AccessFor`, borrowing-tree completion and typed execution/evidence
handoff. Limits are shared across aliases, not reset by another binding. `Bind`
alone does not automatically wrap capability methods or enforce their constraints.
Retry policy and distributed quotas remain separately scoped.

## One authoritative resource record

`Borrow` and `Delegate` create selection descriptions. The lease acquisition or
ownership transfer happens during the receiving `Assemble`, not when the selection
is declared. Preflight and acquisition remain separate checks.

| Relationship | Construction/acceptance | Scope shutdown |
| --- | --- | --- |
| Owned | A factory transfers its acquired cleanup responsibility to its assembly | Assembly invokes the record's cleanup action |
| Borrowed | `Borrow` acquires a lease on an already-ready record under a local alias | Returns only that lease; never invokes the owner's release/stop callback |
| Delegated | `Delegate` transfers exclusive ownership to the receiving assembly at acquisition | Only the receiver may release it; donor binding is refused |

Actual `Info.Scope`, Provider, name, configuration format, and revision remain
those of the originally constructed resource, even across aliases or transfer.
Current `OwnerScope` is a distinct lifecycle fact. Borrowed views never become
independent physical resource owners.

Borrowing pins the record. Owner shutdown refuses new shares and owner bindings
but cannot release a record with outstanding borrowers. Existing borrowing scopes
remain responsible for their use and may finish independently. Returning one
borrowing scope does not stop another. The owner retries shutdown explicitly after
leases end; there is no hidden background reaper.

Controlled root calls borrow this same record. Their active use prevents returning
their scope's borrowing lease and protects earlier ordered dependencies. Nested
borrowing nodes share one root admission and byte reservation; they are not another
physical resource owner. `Status.Borrowers` counts borrowing assembly scopes;
`Status.Usage` reports controlled root calls and queues separately. Existing borrowed
scopes retain their admission after owner shutdown, until their own scope closes.
Applied call limits are separately inspectable and are not part of the prepared
Provider configuration revision.

The [compatibility contract](../compatibility/interface.md) associates original
source identity, format, preparation revision and applied limits with an explicit
effective Provider profile and consuming-binary facts. It does not equate random
preparation revisions with content equality or claim to inspect native settings.

Delegation is deliberately conservative: the donor must have exactly one entry,
with its entire owned dependency set contained within that record, and no borrowers.
Moving one record out of an ordered dependency set would lose lifetime protection,
so that mode is rejected before construction. A preflight rejection leaves
ownership with the donor. After acquisition, later recipient failure does not
return ownership implicitly: the failed recipient retains cleanup responsibility.

Composition must quiesce previously handed-out capabilities before ownership
transfer and before ending their owning/borrowing scope. Arbitrary Go values cannot
be revoked. Assembly-to-assembly delegation is implemented; a concrete SDK accepting
ownership of an injected component still needs its own explicit acceptance/failure
contract and tests. This foundation does not claim that SDK support.

## Executable evidence

[Resource tests](../../../../internal/resource/resources_test.go) and [cancellation regressions](../../../../internal/resource/resources_cancellation_test.go) cover this contract.
