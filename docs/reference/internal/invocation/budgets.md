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

# Budgets and execution shapes

[Documentation](../../../README.md) / Internal package reference

**Audience:** invocation integration authors.
**Status:** implemented local contract; native guarantees remain integration-specific.
**Package:** `github.com/frost-leo/fathomry/internal/invocation`.

Start with the [invocation interface](interface.md). This is one part of the same
accepted-call responsibility, not another resource owner.

## Budgets follow the execution shape

`Budget.Context(parent, phase)` produces a cooperative deadline: the earlier of
`now + Limit` and `parent deadline - Reserve`. Durations are Go nanoseconds;
negative values are invalid. A zero Limit inherits a parent deadline. Only
`Lifetime` may instead inherit an explicitly cancellable context with no deadline.
An uncancellable, unbounded lifetime is rejected. Reserve needs a parent deadline.

The caller owns the returned cancel function. Cancel it only when the SDK no
longer needs that context. No context is silently detached or stored as a mutable
instance-wide current caller. Outer cancellation can consume the intended reserve;
it is not a hard cleanup guarantee.
[Go context contract](https://pkg.go.dev/context#CancelFunc).

| Shape | Integration pattern and distinct obligations |
| --- | --- |
| Finite | Begin for admission, then synchronous `Call.Execute` with its execution budget; callback returns all outcomes/cleanup facts and confirms its own local use is over. Remaining users must already be retained; otherwise use manual reporting/release. No goroutine races it against a timer. |
| Stream/handle | Establish separately where the SDK supports it; retain the original call while consuming/finalizing the returned resource. Partial data and cleanup failure can coexist. |
| Async | Keep the receipt until native delivery evidence. Retain a submit-stack `Guard` **before** calling an SDK that can invoke completion before submission returns. Waiting and delivery use independent caller-owned budgets. |
| Session | Establish, own an explicit Lifetime, handle bounded messages with separate budgets/results, then stop/join native work and report cleanup. Establishment expiry is not session lifetime. |

`Execute` returns setup/budget/state errors; the callback's technical failures are
in the receipt's `Result.Err`, not its own return error. While Execute runs, it
exclusively owns completion: other Resolve/Finish/Release/Complete reject mixed producers
instead of silently dropping the callback's later data or cleanup failures.

The phase names describe purposes, not a universal SDK state machine. For example,
an HTTP request context can govern body consumption beyond receiving headers, and
request-body closure may happen asynchronously after `Do`. A Provider cannot
cancel a supposed establishment phase at headers if its SDK still uses that
context; it needs a suitable native timeout or a longer owning phase.
[Go HTTP contract](https://pkg.go.dev/net/http#Client.Do).

## Executable evidence

[Call tests](../../../../internal/invocation/calls_test.go) and [evidence tests](../../../../internal/invocation/evidence_test.go) exercise the contract. See [SDK integration](../../../development/sdk-integration.md) and [testing](../../../development/testing.md).
