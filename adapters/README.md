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

# Adapter packages and maintenance map

[Project](../README.md) / [Documentation](../docs/README.md)

**Audience:** contributors and consumers navigating the Adapter tree.
**Status:** implemented role-based organization for every current production Go
package below this directory: public contracts, concrete providers and one private helper.
Grouping directories are not Go packages; testdata consumer modules are executable
fixtures, not production providers. New packages must be classified and documented.

This README owns the package inventory and file-responsibility map. The
[maintenance guide](../docs/development/public-adapters.md) owns change/verification
rules, the [error guide](../docs/development/adapter-errors.md) owns error semantics,
and each linked `interface.md` owns its calling contract. Do not maintain competing
copies of those rules here.

## Every package has a role

Paths are relative to `adapters/`. A parent-looking import path does not
automatically own or construct the packages below it.

| Package | Role | Responsibility and calling contract |
| --- | --- | --- |
| `v1` | mechanism | [Operation Runtime, admission, actual-work references and independent evidence](../docs/reference/adapters/v1/interface.md) |
| `database/v1` | capability | [SQL budgets, attribution and profile vocabulary; no pool/client](../docs/reference/adapters/database/v1/interface.md) |
| `sqlengine/v1` | capability | [SQL-engine source/work/evidence budgets and observation vocabulary; no native authority](../docs/reference/adapters/sqlengine/v1/interface.md) |
| `broker/v1` | capability | [Broker budgets and attribution; no native client](../docs/reference/adapters/broker/v1/interface.md) |
| `cache/v1` | capability | [Cache/messaging composition budgets and attribution](../docs/reference/adapters/cache/v1/interface.md) |
| `objectstore/v1` | capability | [Object effects, budgets, generation scaling and attribution](../docs/reference/adapters/objectstore/v1/interface.md) |
| `httpclient/v1` | capability | [HTTP declared accounting and attribution; no request or runtime interface](../docs/reference/adapters/httpclient/v1/interface.md) |
| `configsource/v1` | preparation | [Strict configuration preparation and complete raw Source/Observer contracts](../docs/reference/adapters/configsource/v1/interface.md) |
| `configsource/viper/v1` | configuration-provider | [Explicit local acquisition, native decoding and owned observation](../docs/reference/adapters/configsource/viper/v1/interface.md) |
| `configsource/nacos/v1` | configuration-provider | [Nacos acquisition/mutation/search and retained observation](../docs/reference/adapters/configsource/nacos/v1/interface.md) |
| `database/postgres/v1` | data-provider | [PostgreSQL ownership, SQL, preparation, transactions and savepoints](../docs/reference/adapters/database/postgres/v1/interface.md) |
| `database/mysql/v1` | data-provider | [MySQL ownership, SQL, preparation and provider-specific transactions](../docs/reference/adapters/database/mysql/v1/interface.md) |
| `sqlengine/duckdb/v1` | data-provider | [Embedded SQL, parameter batches, Appender, transactions and bounded incremental results](../docs/reference/adapters/sqlengine/duckdb/v1/interface.md) |
| `sqlengine/trino/v1` | data-provider | [Coordinator Query/Execute/Insert, provisional page consumption and independent effect evidence](../docs/reference/adapters/sqlengine/trino/v1/interface.md) |
| `sqlengine/doris/v1` | data-provider | [Finite/incremental SQL, strict labeled Stream Load, label observations and independent cleanup evidence](../docs/reference/adapters/sqlengine/doris/v1/interface.md) |
| `objectstore/minio/v1` | data-provider | [Objects, multipart, versions/listing and restricted delegation](../docs/reference/adapters/objectstore/minio/v1/interface.md) |
| `broker/kafka/v1` | data-provider | [Production, direct/classic-group consumption and checkpoints](../docs/reference/adapters/broker/kafka/v1/interface.md) |
| `cache/redis/v1` | data-provider | [Commands, sessions, Streams/subscriptions and supported experimental modes](../docs/reference/adapters/cache/redis/v1/interface.md) |
| `httpclient/nethttp/v1` | data-provider | [Standard HTTP finite responses, retained streams and controlled direct connections](../docs/reference/adapters/httpclient/nethttp/v1/interface.md) |
| `httpclient/tlsclient/v1` | data-provider | [Explicit native profiles, protocol racing, finite/stream responses and measured TLS/TCP bandwidth](../docs/reference/adapters/httpclient/tlsclient/v1/interface.md) |
| `internal/errorbridge` | private | [Bounded error forwarding/inspection; no public registry or execution engine](../docs/reference/adapters/internal/errorbridge/interface.md) |

Public package version and native SDK version are separate: for example, public
Nacos `v1` uses the selected Internal Nacos `v2` integration. Shared capability
packages do not import concrete providers; consumers select providers explicitly.
The private helper is not importable by independent consumers.

## Common responsibilities, not identical file counts

Every production package has `doc.go`, a calling-contract page, focused tests
and a declared dependency role. Use the following files when the responsibility
exists; do not create empty files or new APIs to complete a template.

| File | Responsibility |
| --- | --- |
| `doc.go` | Package purpose, consumers, call sequence, ownership and limits |
| `options.go` | Configuration/dependency declarations, mapping and validation when exposed |
| `policy.go` | Budget/Policy vocabulary or public policy computation |
| `metadata.go` | Detached attribution, source identity, provenance and profile vocabulary |
| `source.go` | Native source Owner/Handle construction and repeatable shutdown, where such ownership exists |
| `client.go` | Capability facade construction/borrowing and common operation dispatch |
| `acquisition.go` | Complete original-document Capture/Observe profiles, not source ownership |
| `result.go` | Provider-specific outcomes/copying when there is a substantive result model |
| Operation files | Cohesive operations, requests and retained handles, such as transaction, watch or multipart |
| `definitions.go` | Stable codes, identifiers, owners and detached offline definitions, for packages owning a catalog |
| `error.go` | Error construction, native translation, error details and deliberate native inspection |
| `diagnostics.go` | fmt/slog presentation and JSON serialization/refusal hooks |
| `resources.go` | Embedded locale resources and offline filesystem access, for packages owning a catalog |

Error definitions and occurrences are different responsibilities. Viper and Nacos
follow the same separation as the data providers: `fail`, `translate` and
Nacos native-error inspection belong in `error.go`, not in declarations or
formatting code. Required evidence remains separate from optional diagnostics.

### Mechanism profile

`v1` has real `runtime.go`, `call.go`, `ownership.go`, `lifetime.go`,
`evidence.go` and `observer.go` mechanisms. It has its own error catalog but no
provider Settings, source Owner, SDK selection or provider Recommend API.
Runtime composition declarations may contain callbacks; they are not loadable DTOs.

### Capability profile

The capability packages share `policy.go`, `metadata.go` and
`diagnostics.go`. They expose data/vocabulary, not native pools, source owners,
provider factories or another runtime. Existing serialization-refusal guards do
not justify allocating a new provider error facility or fabricating
`definitions.go`/`resources.go`.

`objectstore.Policy.ForGenerations` is existing arithmetic composition over
caller-owned data; it does not construct sources or become a common provider engine.

### Configuration preparation profile

`configsource/v1` owns `options.go`, `schema.go`, `prepare.go`,
`decode.go`, `toml.go`, `variables.go` and `acquisition.go`.
Preparation/complete raw batches are not native source ownership or settings
publication. A Schema's explicit validator callback is composition authority,
not a callback field in loadable provider Settings. Deliberate Raw/Layer/Variable
data retain their JSON permissions; Prepared/Batch/Schema runtime contracts retain
their serialization guards.

### Configuration-provider profile

Both providers use `options.go`, `client.go`, `acquisition.go` and the four
error/presentation files. Their actual APIs remain different:

- **Viper:** `New(dependencies)` binds a non-owning facade. Settings are selected
  per Input/Load. `load.go`, `document.go` and `watch.go` retain acquisition,
  native weak decoding and owned observations. There is no fake pool Owner,
  Open, Using or Recommend API. Borrowed Readers stay caller-owned.
- **Nacos:** `source.go` owns Open/Owner/Handle/shutdown; `metadata.go` holds
  preparation metadata. `Using(ref, dependencies)` borrows generations.
  Read/management/search/watch files preserve native distinctions. Its fixed
  public route envelopes and native ceilings remain explicit; it does not acquire
  the data-provider Recommend/WithID signatures by convention.
- **Both:** `acquisition.go` adapts their selected complete raw Capture/Observe
  profile. Already-acquired Nacos observation batches are not refetched on Next.
  Source observations and strict application preparation remain separate.

### Data-provider profile

The data providers share options/policy/source/client/result responsibilities
and the same error/presentation separation. Capability files remain meaningful:
SQL statements/transactions/savepoints, MinIO multipart/list/delegation, Kafka
production/consumers/groups, Redis sessions/subscriptions.
DuckDB retains embedded-engine ownership, exact values, Appender/transaction
effects and an owned reader over native materialization; bounded Go delivery
does not become a hard RSS guarantee.
Trino retains coordinator readiness, exact direct JSON, single-statement Insert,
provisional page transfer and independent terminal/cleanup evidence; a page is
not a completed business range or a durable resume token.
Doris retains local-only construction, single-use SQL connections, bounded
cursor lifetime and strict Stream Load. Load visibility, row quality and label/
payload identity are distinct; an admitted canceled page ends its cursor.

Redis additionally has real `preparation.go`, `credentials.go`,
`logging.go` and `profile.go` responsibilities. Do not manufacture empty
equivalents elsewhere. Readiness, context lifetimes, effects and result shapes
remain provider-specific as documented in their contracts.

### Private-helper profile

`internal/errorbridge` uses `bridge.go`, `containment.go`, `details.go` and
`diagnostics.go`. Error/Unwrap/Is semantics remain with the error mechanism;
format/log/serialization hooks stay in diagnostics. It owns neither public
facilities/resources nor source construction, admission or background workers.

## Adding or changing a package

1. Classify its real role and dependency direction; update this inventory and the
   [whole-tree checks](../internal/conformance/adapter_packages_test.go).
   Do not create a package solely to satisfy a diagram or naming pattern.
2. Put contracts and implementation in their responsibility files. Preserve public
   signatures, settings keys/zero meanings, error identities and lifecycle
   semantics unless an explicit compatibility change is approved.
3. Update `doc.go`, its `interface.md`, this map and relevant guides together.
   Keep shared error allocation, CLI registration and documentation indexes
   incremental; retain other providers.
4. Follow the [verification workflow](../docs/development/public-adapters.md#pa-08--require-executable-change-evidence):
   focused tests, all-role conformance, independent consumers, actual offline
   catalogs, race/fuzz/build and only separately authorized service gates.
5. Keep permanent sanitized evidence with the issue. Record skips and failed
   controls honestly, retain uncertain cleanup, and finish independent review.

From the repository root, the structure and contract gates are:

```sh
go test -race -count=1 ./internal/conformance -run '^TestPublicAdapter'
go test -race -count=1 ./adapters/...
```

These checks do not qualify every real service or replace native capability,
protocol, lifetime and independent effect-readback tests. No SDK upgrade, new
provider, universal client API, deployment or publication is authorized by this README.
