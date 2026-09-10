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

# Design tradeoffs and open boundaries

[Documentation](../README.md) / Architecture

**Audience:** framework and integration maintainers.
**Status:** accepted architecture; implementation coverage is limited.

This topic states accepted cross-package obligations, not a claim that every
SDK or framework feature is implemented. See the [documentation map](../README.md)
for current availability and package contracts.

**Standard sections:** S12.

## Contents

- [Decisions and remaining implementation choices](#decisions-and-remaining-implementation-choices)

## Decisions and remaining implementation choices

The established architecture consists of the three roles and two controlled
public paths; shared internal mechanisms with independent Providers; named
multi-source use; future public error meaning over technical causes and truthful evidence; version
accountability; and cohesive professional organization.

The alternatives below explain why these boundaries are necessary. Source
observations disprove unsafe assumptions; they do not prove an unimplemented
mechanism. Earlier exploratory package names, APIs, lifecycle algorithms, and
directory sketches remain unselected. A working mechanism must satisfy this
standard with evidence, not just resemble the previous sketches.

| Alternative | Assessment against the approved outcome |
| --- | --- |
| Independent ad hoc wrapper per SDK | Retains SDK detail, but repeats configuration, ownership, error, and attribution policy; no accountable shared mechanism |
| Universal client/lifecycle/result model | Simplifies a diagram while hiding incompatible completion, transaction, cancellation, and resource semantics exposed in S10 |
| Shared mechanisms with explicit capability semantics and independent Providers | Fits the approved direction, at the cost of maintaining capability-specific contracts and evidence rather than claiming all Providers are interchangeable |

The following remain open, with a responsible boundary and the point at which a
decision becomes necessary. They are not permission to drop the approved guarantees.

| Open decision | Responsible boundary and required checkpoint |
| --- | --- |
| Public permitted capability/source-selection APIs and selection of compiled implementations | Framework composition/capability maintainers; #15 makes assembly/producer authority internal, without implementing a loader or runtime |
| Configuration loading/backend selection, reload and credential rotation handoff | Framework configuration/credential owners; existing preparation precedence, list/null/empty rules and frozen revision semantics remain established |
| Synchronous shutdown versus bounded waiting with continued owned cleanup | Composition and lifecycle owners, before a shutdown guarantee is published; keep actual completion and unknown cleanup distinct |
| Durable evidence acknowledgement, reliable storage, crash/duplicate/late-result handling, and handled-error disposition | Framework/public contract owners with Adapter evidence, before end-to-end business advancement/recovery is claimed; Issue #5 supplies bounded in-process receipt ownership, not a persistence protocol or terminal algorithm |
| Native advanced access, borrowing and zero-copy lifetimes | Relevant public capability and Provider owners, before such an escape hatch is exposed |
| Deployment choice of untested/unknown use policy and future historical formats | Deployment and format owners; the local compatibility policy is implemented, but actual startup choices and historical-reader contracts remain separately owned |
| Initial supported modes, real service combinations, workload/SLOs, and demonstrable resource limits | Explicit product constraints and each implementation issue, before support/performance claims; this source comparison selects no integration order |

These open choices limit implementation claims. This standard establishes
obligations and evidence requirements; package references describe the implemented
subset. Concrete SDK integrations and the full runtime remain absent. A document
or closed issue is not evidence that a service guarantee has passed acceptance.


## Implementation references

[Current package contracts](../README.md#internal-package-reference) and [acceptance workflow](../development/sdk-integration.md).

This topic carries forward the [accepted integration standard](internal-sdk-integration.md)
from [Issue #3](https://github.com/frost-leo/fathomry/issues/3), including the
[Issue #15](https://github.com/frost-leo/fathomry/issues/15) boundary refinements.
