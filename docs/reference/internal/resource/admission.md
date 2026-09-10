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

# Admission and borrowing limits

[Documentation](../../../README.md) / Internal package reference

**Audience:** resource and invocation authors.
**Status:** implemented local count/declared-byte bounds.
**Package:** `github.com/frost-leo/fathomry/internal/resource`.

The resource record owns the allowance. [Controlled calls](../invocation/interface.md)
consume it and retain local use; this is not native-memory measurement or a distributed quota.

## Framework configuration and binding

Apply `resource.WithLimits(selection, limits)` to an owned selection before
`Assemble`. Invalid policies fail preflight before constructors. Borrowing and
delegation inherit that record's policy; they cannot attach a fresh allowance.
Selections without a limits policy remain usable for resource composition but cannot obtain controlled
`Access` without an explicit policy.

Framework integration code binds the selected capability with `resource.Bind` and its
scoped admission authority with `resource.AccessFor`. Its public facade uses
`invocation.Begin` before entering native work. `Bind` alone does not wrap methods
or enforce admission automatically; exposing that path directly bypasses this
contract. Each concrete integration must prove its facade applies the mechanisms.

Framework code reads the immutable policy and current usage through
`Access.Limits` and `Assembly.Snapshot`; private results/reports retain copied metadata:

| Field | Unit and behavior |
| --- | --- |
| `Limits.Active` | Positive maximum admitted root uses on this record across its aliases |
| `Limits.Queued` | Maximum waiting root calls; zero rejects overload immediately |
| `Limits.Bytes` | Positive maximum sum of declared active working-byte reservations |
| `Limits.QueuedBytes` | Separate queued-byte ceiling; positive when queueing is allowed, otherwise zero |
| `Limits.MaxLeases` | Positive live borrowing-node ceiling per root, including ancestors retained by descendants |
| `Usage.Active/Queued` | Current logical root uses / queued callers, not Items, rows, connections or SDK attempts |
| `Usage.ActiveBytes/QueuedBytes` | Currently charged declared bytes, never cumulative wire traffic |
| `Status.Borrowers` | Borrowing assembly scopes; active call pins are separately projected in Usage |

Queueing is FIFO, including byte head-of-line blocking. A bounded oversized request
is rejected rather than queued forever. Waiting runs on the caller's stack;
there is no scheduler, worker pool or background reaper. Context is checked at
admission, and the finite helper checks again before native entry. Cancellation
after that entry remains cooperative.

A root's `Request.Bytes` is nonnegative and covers its entire nested working
envelope. Nested calls declare zero additional working bytes, not a new allowance.
Evidence has a [separate positive reservation](../invocation/evidence.md). Framework bookkeeping
and count-bounded metadata are additional memory overhead, not payload bytes measured
by these reservations. Count and declared
byte bounds do **not** measure arbitrary SDK allocation, native memory, prefetch,
decompression, connection pools or external account quotas. Providers must bound
those independently and reject modes that cannot meet a required guarantee.

`Description.Revision` still identifies prepared Provider settings. It is not a
fingerprint of the later admission policy, execution budgets or build. Changing
these policies must not be disguised as an unchanged effective execution contract;
composition freezes the relevant settings and records their separate provenance.

## Lease contract

Access.Acquire obtains one root; Lease.Retain creates a bounded child without
waiting for another allowance. Copies refer to the same node; Release is idempotent.
Done closes only after that node and all descendants end. Retained ancestors count
toward MaxLeases. A nil/zero lease has no completion signal; cancellation does not
release it. See [shutdown](shutdown.md) for borrower behavior after owner shutdown.

[Admission tests](../../../../internal/resource/admission_test.go) exercise shared
ceilings, FIFO expiry and retained descendants.
