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

# Resource ownership and lifetime

[Documentation](../README.md) / Architecture

**Audience:** framework and integration maintainers.
**Status:** accepted architecture; implementation coverage is limited.

This topic states accepted cross-package obligations, not a claim that every
SDK or framework feature is implemented. See the [documentation map](../README.md)
for current availability and package contracts.

**Standard sections:** S05.

## Contents

- [Ownership and termination guarantees](#ownership-and-termination-guarantees)

## Ownership and termination guarantees

Every resource must have an accountable owner: clients, pools, borrowed connections,
transactions, streams, buffers, subscriptions, timers, goroutines, exporters, and
native resources. Ownership includes dependencies and background work created by
the SDK, not only handles created directly by Fathomry. Shared mechanisms maintain
one authoritative account of a managed resource's ownership and lifecycle; borrowed
views do not become separate owners of the same client. The Provider supplies the
actual SDK completion/release evidence for that account.

| Situation | Required responsibility |
| --- | --- |
| Creation | Identify the owner before the resource can escape; expose only the usable capability after required validation |
| Partial initialization | Keep ownership of every created resource, report primary and cleanup failures, and retain unresolved cleanup responsibility; do not advertise a complete instance |
| Borrowing | Define scope, allowed operations, concurrency, and return obligations; a borrower cannot close or reconfigure the shared owner |
| Ownership transfer | Make acceptance and failure behavior explicit, including SDKs that take responsibility for closing injected components; avoid double ownership or continued unauthorized sharing |
| Operation lifetime | Pass caller-owned contexts explicitly; define the owner of streams, callbacks, deliveries, and remote tasks that outlive the initiating call |
| Shutdown | Stop new work in the intended scope, retain paths needed to finish or clean up existing work, and release dependencies only when their consumers no longer need them |

Normal completion, cancellation request, stopping admission, resource release, and
reversal of an external effect are distinct facts. An API name such as `Close`
does not select among them: closing may flush writes, cancel a remote query, or
only signal a background task. Record primary completion, cleanup errors, and
remaining/unknown work separately; a later no-op success cannot erase an earlier
uncertain shutdown outcome.

Define budgets per execution shape: finite request and result consumption;
asynchronous enqueue, delivery and waiting; subscription establishment, session
and individual message handling; remote submission, polling, transfer and cancel;
background export and cleanup. A short initiating-call deadline is not automatically
the lifetime of all of these resources. Detached cleanup is not permission for
unowned or unbounded goroutines. Shared resource accounting must not count each
statement in a held transaction as another independent owned connection.

Cleanup budgets also cover aggregate explicit continuation attempts and retained
native errors across resources, not only each callback's deadline. Framework
composition owns this total budget and any accountable handoff when it is exhausted;
business consumers do not manage internal cleanup loops. Successful release does
not erase earlier causes. The implemented [shutdown contract](../reference/internal/resource/shutdown.md)
states history/snapshot costs without automatic retry or silent cause truncation.

Specify whether a deadline bounds waiting, actual work, or both. Do not promise
hard cancellation of arbitrary Go callbacks, readers, native code, or remote
effects. A shutdown timeout does not prove that dependencies are no longer used.
The implemented resource Close is synchronous and cooperative. Whether a future
framework/SDK boundary waits or returns while an owned cleanup operation continues
needs its own explicit contract. Unsupported termination or physical-isolation guarantees
must be disclosed or the corresponding mode rejected, never inferred from an
internal counter reaching zero.

## Implementation references

[Resource ownership](../reference/internal/resource/ownership.md), [shutdown](../reference/internal/resource/shutdown.md) and [call completion](../reference/internal/invocation/completion.md).

This topic carries forward the [accepted integration standard](internal-sdk-integration.md)
from [Issue #3](https://github.com/frost-leo/fathomry/issues/3), including the
[Issue #15](https://github.com/frost-leo/fathomry/issues/15) boundary refinements.
