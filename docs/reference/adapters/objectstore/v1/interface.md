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

# Shared object-storage contracts

[Documentation](../../../../README.md) / Public package reference

**Audience:** application composition and object-storage Adapter authors.
**Status:** implemented SDK-independent contracts, currently consumed by the MinIO Adapter.
**Package:** `github.com/frost-leo/fathomry/adapters/objectstore/v1`.

## Responsibility

This is a real shared contract boundary, not an empty grouping package or a
universal storage client. It imports public `adapters/v1`, never Internal,
a concrete Adapter, an SDK or Framework. Importing it creates no source, worker,
connection, registry or shutdown authority.

The owner requested this boundary during [#104](https://github.com/frost-leo/fathomry/issues/104)
implementation. It follows the existing database contract separation. Future
Adapters may share these already-defined observations without adopting MinIO's
addressing, grants, multipart protocol or client implementation.

## Budgets and evidence

- `Budget` declares root-family work and per-result evidence bytes. Providers
  derive effective envelopes from their actual implementation. These are not RSS
  measurements, distributed quotas or a promise about arbitrary native error graphs.
- `Policy` combines Budget with explicit Runtime/Evidence options, including
  source ownership, sessions, incremental children and queued operations.
  `ForGenerations(1..16)` scales equivalent source/root/queue/evidence envelopes
  for overlap without altering per-root budgets or an existing runtime. It checks
  arithmetic limits, not provider compatibility.
- `Info` identifies actual source preparation and copied provenance.
  `Clone` isolates both the outer provenance slice and nested field slices.
  Revision is opaque; configuration version, SDK version and public resource
  generation remain different axes.
- `Attribution` freezes public correlation and the generation actually borrowed.
  Zero Source covers direct use and failed acquisition, not proof of absence.
- `Attempts` counts observed dispatches. Inexact zero does not prove none occurred.
- `Effect` distinguishes `NotSubmitted`, `Unknown` and `Acknowledged` for a
  provider-defined action. Acknowledgement does not prove prior existence,
  persistence, rollback, idempotency or business success. A later cleanup ACK
  must not overwrite an earlier uncertain action.

Info and Attribution are potentially sensitive process-local observations.
Ordinary formatting is restricted and JSON serialization/reconstruction is
refused. Explicit fields and detached copies remain caller-owned data.
Budget/Policy are composition data, not runtime handles.

## Concrete boundary and verification

[MinIO](../minio/v1/interface.md) owns its Settings, requests, immutable results,
delegation, retained multipart/cursor handles and provider error definitions.
Shared contracts do not expose an Internal bridge or unify future SDK methods.

[Metadata tests](../../../../../adapters/objectstore/v1/metadata_test.go),
[option tests](../../../../../adapters/objectstore/v1/option_test.go) and
[integration tests](../../../../../adapters/objectstore/v1/integration_test.go)
check copying, diagnostics, overlap arithmetic and the actual dependency graph.
The [independent consumer](../../../../../adapters/objectstore/minio/v1/testdata/consumer/main.go)
uses both shared contracts and a selected concrete provider.
