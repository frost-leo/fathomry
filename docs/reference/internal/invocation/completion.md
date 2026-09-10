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

# Completion and retained local use

[Documentation](../../../README.md) / Internal package reference

**Audience:** invocation integration authors.
**Status:** implemented local contract; native guarantees remain integration-specific.
**Package:** `github.com/frost-leo/fathomry/internal/invocation`.

Start with the [invocation interface](interface.md). This is one part of the same
accepted-call responsibility, not another resource owner.

## Completion, nested work and shutdown

`Begin` owns one root borrowing lease. `Scope.Hold` creates a bounded child
obligation without acquiring another resource permit; `Guard.End` confirms that
use has actually ended. Copies share idempotent release. An ended scope cannot
create fresh children. Retained ancestors count toward the bound, preventing an
endless handoff chain from growing unbounded memory.

`BeginNested` creates a separately attributed result under an existing scope.
It reserves evidence nonblockingly and retains the same root use. A child ends
independently, while its parent cannot release the resource ahead of it. Nested
operations share the predeclared resource/byte envelope; this is not authorization
for extra physical connections or unbounded parallel fan-out.

Synchronous helpers and cleanup can use an already held scope and phase budget
without creating a child. Borrowing-node/evidence exhaustion returns immediately,
never waits behind the same held resource. Required cleanup must not depend on
creating a new root or child at shutdown.

Technical reporting and resource release are separate:

1. `Resolve(outcome)` publishes the first technical value/primary error and any
   already-known cleanup error. It retains the original lease **and** cleanup
   evidence capacity. `Receipt.Wait` may now return with `Final=false`.
2. `Finish(cleanupError)` records final technical/cleanup evidence without erasing
   prior errors **or releasing use**. `WaitFinal` can now receive a cleanup failure
   while local termination is still unconfirmed, even with every capacity at one.
3. `Release()` separately confirms this producer no longer uses the resource. A
   failed or timed-out SDK Close is not sufficient evidence. Descendants still pin
   the resource; no new permit or evidence slot is required for this confirmation.
4. `Complete(outcome)` combines reporting and release only when all technical facts
   and this producer's local-use completion are known. The finite helper uses this
   combined path after its callback returns; there is no automatic cleanup retry.

Resolution, final reporting and local confirmation are each one-shot; duplicates return false
without accepting the new value. `Guard.End` carries lifetime evidence only:
it is not a place to discard late errors. Late error-producing work must use
Resolve/Finish or an independently reserved child result. Finish can append
cleanup, not an unreported primary outcome. Aggregate primary facts before
Resolve or reserve a separate attributable call; a submit guard alone does not
settle all primary facts.

`Result.Final` concerns this operation's final technical report. `Released`
requires positive local-use confirmation and every retained descendant to finish. Neither proves remote
cancellation, external rollback, committed/visible data or durable recording.
`Receipt.WaitReleased` observes local subtree completion. A nil or zero internal
receipt reports invalid state; it is not completion evidence. Ending any wait
never releases work or overwrites the technical error with a wait error.

Closing an assembly stops its new calls but permits existing scopes to finish
nested work and cleanup. Its active calls retain earlier ordered dependencies.
Existing borrowed assemblies continue independently after owner shutdown; their own Close cannot return their protecting lease while calls remain.
Delegation still rejects active borrowers/calls and now rejects queued work too.
Close is synchronous/cooperative and must be retried explicitly after pins end.
If an SDK needs flush/drain/stop work to settle pending callbacks, the integration
must drive that work through an existing held scope or its explicit owner-control
path. Putting the only progress action in the source's final Release callback
would deadlock: that callback is intentionally not invoked while calls still use
the resource. No universal SDK shutdown driver is supplied by these mechanisms.
Arbitrary callback panics, uncooperative native code and process loss are not
recoverable lifecycle guarantees.

## Executable evidence

[Call tests](../../../../internal/invocation/calls_test.go) and [evidence tests](../../../../internal/invocation/evidence_test.go) exercise the contract. See [SDK integration](../../../development/sdk-integration.md) and [testing](../../../development/testing.md).
