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

# Private adapter error bridge

[Documentation](../../../../README.md) / Private integration package reference

**Audience:** public adapter maintainers.
**Status:** implemented pure error-boundary helpers, not a public consumer API.
**Package:** `github.com/frost-leo/fathomry/adapters/internal/errorbridge`.

## Responsibilities

SQL/SQL-engine/object-storage/broker/Redis providers share bounded native-kind
inspection and safe forwarding. Providers retain their own classification tables,
definitions, defaults and native-error precedence. This package has no SDK client,
I/O, goroutine, global registry, lifecycle authority or business retry/effect policy.

`Inspect` visits at most the caller-selected node budget in cause order and
deduplicates mapped native codes. A public occurrence is already classified:
its private/native descendants are not reinterpreted. An entirely public graph
behind neutral wrappers receives a safe forwarding representation. An explicit
native boundary, unknown leaf or exhausted traversal keeps provider fallback
responsibility with the caller; the original graph must still be retained.
Bridge-owned forwarded aggregates are also already classified. Reinspection
does not expand their translated/original history or reinterpret retained native
causes; repeated phase composition cannot multiply the traversal cost.
Providers check `Classified` before any specialized recursive translation, so
their own unwrap logic cannot bypass this boundary.

`Forward` joins the original inspection graph with an already translated safe
presentation. A single occurrence exposes its exact existing public core.
Aggregates do not arbitrarily adopt one child owner's identity. Sensitive original
Error methods are never used to construct the safe presentation. fmt/slog are
redacted; implicit JSON serialization/reconstruction is refused.

`Details[T]` performs bounded lookup of the typed accessor belonging to the
supplied current core. It never takes a different occurrence's details from a
nested cause merely because its Go type matches.

`Contains` performs bounded identity-based cause containment without invoking
foreign Is or Error callbacks. An uncomparable target or budget exhaustion is
unproven containment, not absence; unmatched originals remain retained. Redis
projection uses it when removing repeated positional causes from aggregate facts.
Comparability is checked on values, including interface-held slices/maps inside
otherwise comparable struct or array types.

## Ownership and failure boundaries

All inputs and foreign cause graphs remain borrowed, immutable and concurrently
readable under their owners' contracts. Forwarding retains exact cause objects for
deliberate inspection; it neither sanitizes nor serializes those objects.
The node budget bounds helper traversal, not foreign Unwrap/clone implementation
cost or the size of caller-owned graphs. Standard errors.Is/As retain their own
behavior on deliberately supplied cyclic foreign causes.

Classifiers must be pure fixed mappings; translated presentations must already be
safe and bounded. None of these helpers interprets cancellation as absence of
external effects. Public consumers cannot import this adapter-private package.

## Executable evidence

[Unit and fuzz tests](../../../../../adapters/internal/errorbridge/bridge_test.go)
cover native priority, public forwarding, privacy, cycles, wide graphs, detail
identity and aggregation. Provider contract tests and
[conformance checks](../../../../../internal/conformance/adapter_errors_test.go)
cover integration and metadata separation. The external-import rejection is
tested by the existing independent-module conformance fixture.

## Package organization

The [Adapter tree map](../../../../../adapters/README.md) defines this package's role
and file responsibilities; shared mechanisms, capability vocabulary, preparation
and concrete providers do not acquire identical APIs by convention.
