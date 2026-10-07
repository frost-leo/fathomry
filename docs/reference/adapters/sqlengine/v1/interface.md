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


# Shared SQL-engine contracts

[Documentation](../../../../README.md) / Public package reference

**Audience:** SQL-engine Adapter authors, independent applications and Framework composition.
**Status:** implemented provider-independent public contracts; provider qualification remains separate.
**Package:** `github.com/frost-leo/fathomry/adapters/sqlengine/v1`.

## Ownership and dependency direction

SQL engines own this public layer. Its types are concrete declarations, not aliases
or forwarding wrappers for `adapters/database/v1`. PostgreSQL/MySQL retain that
database layer unchanged. This package imports only standard library and
[adapters/v1](../../v1/interface.md), not Internal, SDKs, database contracts,
concrete providers or Framework. Importing it creates no engine, pool or worker.

The shared vocabulary serves the actual DuckDB, Trino and Doris integration
designs: source preparation identity, public attribution, observed attempts,
effective profiles and declared capacity. It defines no universal Query/Execute,
transaction, cursor, row, mutation-effect or load interface. Admission, actual-work
guards and evidence remain in adapters/v1; generations remain in
[resource/v1](../../../resource/v1/interface.md).

Provider distinctions are intentional:
- DuckDB owns an embedded engine and materialized native results; ordered
  transactions and Appender stages remain provider-specific.
- Trino construction executes coordinator readiness. Pages are provisional;
  canceling a page waiter does not cancel the owning query.
- Doris construction can be local-only. Its SQL cursor, HTTP Stream Load, label
  reconciliation, row quality and committed/visible states are distinct. A
  canceled admitted cursor read can terminate that cursor.

Redis belongs to its cache capability category, not this SQL-engine layer.
Logical error allocation still uses Database domain; no provider facilities or
operation identifiers are renumbered by introducing these public contracts.

## Budgets and source generations

`Budget` declares root work and per-record evidence bytes. Children share root
work but independently reserve evidence. Concrete Using facades reject larger
generations before native application dispatch. Declared bytes are not hard RSS
limits, measured service capacity or retry permission.

`Policy` describes **one source** and its operations. Concrete Recommend functions
derive it from authoritative validated native preparation; common contracts
duplicate no native defaults/formulas and own no runtime.

- Runtime and Evidence already include source charges; do not add them twice.
- SourceWorkBytes is the exact source-lifetime reservation, including idle
  ownership and retiring generations until actual release.
- SourceEvidenceBytes is the independent source terminal-record charge. It
  remains through handling/Ack, potentially after native release; failed
  readiness can retain native error data.
- Zero source bytes mean no declaration, not a certified zero-cost source.
  Runtime/Evidence options retain the public mechanism's zero/default semantics.

Compose multiple sources or overlapping Follow generations explicitly. If a
facade chooses the maximum Budget across heterogeneous profiles, reserve its
operations at that maximum rather than each smaller standalone recommendation.
Sum source work/evidence separately; charge active/queued roots and non-source
evidence slots at their selected budgets. Include every still-live generation.
Recommendations never resize caller-owned mechanisms.

Do not mechanically max/sum zero MaxTasks/MaxDepth/MaxHolds: zero selects public
defaults, whereas a provider can recommend explicit smaller limits. Choose
explicit adequate tree limits when composing such profiles. No scheduler,
global limiter or new execution/cleanup controller is introduced.

## Observations and copying

- Info identifies preparation, not readiness or generation. FormatVersion is
  the settings format and Revision is opaque identity, not a settings hash.
  Clone detaches provenance and every nested field-name list.
- Attribution captures runtime/operation/correlation and the borrowed generation.
  Zero Source covers direct access and failed acquisition, not absence of source
  configuration. Sequence is local and not a resume token.
- Attempts preserves observed counts and exactness; inexact zero leaves
  unobserved requests possible. Counts are not effect or retry decisions.
- Profile separates SDK mode, native library, protocol and service facts.
  Fact.Kind distinguishes unknown, declared, observed, redacted, development and
  not-applicable. Readiness/version text is not connector/table-format evidence.
  Profile.Clone detaches the options.
- LayerInfo contains provenance field names; Option contains intentionally
  non-secret effective settings. Neither contains an owning native handle.

Returned metadata belongs to its caller. Do not mutate concurrently with Clone;
independent clones may be mutated independently. Nil and empty slices stay
distinct. Source/profile/correlation fields can be sensitive: inspect deliberately
and do not use them as unbounded metric labels.

Info, Attribution, Fact and Profile redact fmt/slog presentation and reject JSON
persistence/reconstruction. Their methods return values; normalize optional
typed-nil pointers to untyped nil before logging absent observations. Budget/Policy
are configuration data, not ownership handles or execution evidence.

## Verification

The [Adapter tree map](../../../../../adapters/README.md) classifies this package
as capability vocabulary: `policy.go`, `metadata.go` and `diagnostics.go` separate
budgets, observations and presentation. There is no provider error catalog or
shared native execution mechanism to manufacture here.

[Metadata tests](../../../../../adapters/sqlengine/v1/metadata_test.go) cover owned
type identities, clone isolation, restricted presentation and serialization.
[Independent-consumer and dependency checks](../../../../../adapters/sqlengine/v1/integration_test.go)
exercise operation/evidence composition without any native provider. Concrete
provider tests separately exercise direct and Framework Fixed/Follow routes.
Common contracts cannot certify provider effects or deployment profiles.
