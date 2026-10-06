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

# Shared database contracts

[Documentation](../../../../README.md) / Public package reference

**Audience:** independent applications, concrete database Adapter authors and
Framework composition.
**Status:** implemented process-local contracts. Concrete Adapters have isolated
public-path qualification; [#102](https://github.com/frost-leo/fathomry/issues/102)
publication/merge status is separate from local verification.
**Package:** `github.com/frost-leo/fathomry/adapters/database/v1`.

## Responsibilities

This package holds the public contracts that PostgreSQL and MySQL actually share:
declared operation budgets, source attribution, observed attempt counts and
effective-profile metadata. It imports public `adapters/v1`, not Internal, a
database SDK, a concrete provider or Framework. Importing it creates no pool,
worker, registry, connection or service request.

The concrete packages are [PostgreSQL](../postgres/v1/interface.md) and
[MySQL](../mysql/v1/interface.md). Native
assemblies, receipts, evidence custody and retained-handle conversion are
non-exported implementation details in each concrete package. There is no added
`adapters/database/internal` package, exported native bridge or universal SQL
client. Shared admission and evidence mechanisms remain in
[adapters/v1](../../v1/interface.md); instance replacement remains in
[resource/v1](../../../resource/v1/interface.md).

## Budgets and observations

- `Budget` declares root-family work bytes and per-result evidence bytes. These
  are accounting envelopes, not measured RSS, native heap ceilings or a charge
  for every child. Concrete resource-backed facades reject a source generation
  that exceeds their declared envelope before native dispatch.
- `Policy` contains that Budget and explicit public Runtime/Evidence options.
  A provider's recommendation includes its source lifetime owner. Combining
  sources requires deliberate aggregate limits; recommendations never silently
  resize an existing caller-owned runtime.
- `Info` identifies one native preparation. `FormatVersion` is its
  configuration format version; Revision is an opaque preparation identity, not
  a secret-settings hash, public resource generation or service-readiness proof.
  `Clone` copies the outer provenance slice and every nested Fields slice.
- `Attribution` captures public operation identity and the actually borrowed
  resource generation. A zero Source also covers direct access and failed
  acquisition; it does not mean a configured source is absent.
- `Attempts` counts observed native dispatches. Inexact zero does not establish
  that no request was sent; a count is not a retry recommendation.
- `Profile` separates SDK mode, protocol, service and native-library facts.
  `Fact.Kind` distinguishes unknown, declared and observed knowledge. Settings
  declarations and server version text do not certify a supported deployment.

Provider result getters return detached metadata. Metadata containing slices is
caller-owned and not safe for concurrent mutation without synchronization.
Profiles, source identities and opaque correlation can be sensitive: inspect
their fields deliberately rather than treating them as metric labels. Ordinary
formatting is restricted, and JSON serialization/reconstruction of process-local
observations is refused. Budget and Policy are configuration/composition data,
not runtime handles.

Query rows, PostgreSQL OIDs/command tags, MySQL insert-ID presence, and transaction
or savepoint outcomes remain provider-specific. This package does not unify
those meanings or infer no effect, rollback, durability or retryability.

## Executable evidence

[Metadata tests](../../../../../adapters/database/v1/metadata_test.go) check
nested metadata isolation, restricted presentation and serialization refusal.
The common package's dependency boundary is checked in
[integration tests](../../../../../adapters/database/v1/integration_test.go).
The concrete provider tests exercise
the actual public/native translation separately. Protocol peers and successful
dependency checks are not real database acceptance.

## Package organization

The [Adapter tree map](../../../../../adapters/README.md) defines this package's role
and file responsibilities; shared mechanisms, capability vocabulary, preparation
and concrete providers do not acquire identical APIs by convention.
