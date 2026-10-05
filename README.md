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

# Fathomry

A Go framework for durable, observable data workflows powered by Temporal.

Development requires Go 1.27.0 or later. The selected Nuki SDK stack uses the
Go 1.27 standard library; the previous Go 1.26 floor is no longer sufficient.

## Direction

Fathomry focuses on explicit contracts, cohesive infrastructure clients,
predictable resource ownership, and durable workflow execution. Reliability,
observability, bounded resource use, and measured performance guide development.

The project is in early development. Configuration preparation, resource ownership,
controlled calls, technical errors, compatibility assessment and testing support
are implemented as private foundations. A bounded internal Viper v1 integration
supports explicit local acquisition and a separate preparation proof, not an
application-wide loading path. The public [failure contract](docs/reference/failure/v1/interface.md)
provides [capability-classified 32-bit codes](docs/reference/failure/v1/code-allocation.md),
extensible typed composition, native-cause inspection and an explicit definition atlas.
[Settings](docs/reference/settings/v1/interface.md) provides project-owned typed
snapshots, atomic publication, subsection reads and an explicit application default.
[Internationalization](docs/reference/i18n/v1/interface.md) gathers component-owned
resources, explains numeric errors in an explicit language and presents errors
using settings preferences without rewriting native causes.
The public [resource holder](docs/reference/resource/v1/interface.md) adds typed
instance scopes, fixed/following configuration adoption, generation borrowing and
cleanup continuation, independently of Internal mechanisms.
The public [Adapter operation mechanisms](docs/reference/adapters/v1/interface.md)
add bounded admission, callback/session ownership, typed results and independent
evidence custody, without reusing Internal engines or requiring localization.
Public [Viper](docs/reference/adapters/configsource/viper/v1/interface.md) and
[Nacos](docs/reference/adapters/configsource/nacos/v1/interface.md) configuration
Adapters now provide independently usable native capabilities and selected raw
sources, with [independent strict preparation](docs/reference/adapters/configsource/v1/interface.md).
Public [PostgreSQL](docs/reference/adapters/database/postgres/v1/interface.md) and
[MySQL](docs/reference/adapters/database/mysql/v1/interface.md) Adapters add bounded
SQL, reusable preparation, transactions and provider-specific evidence. They reuse
the public resource/operation owners and support direct or Framework composition;
shared [database contracts](docs/reference/adapters/database/v1/interface.md) expose
budget and attribution data without native engine dependencies. Isolated public
service checks include independent effect read-back and unique fixture cleanup,
not ORM, migration, HA or production qualification.
The public [MinIO Adapter](docs/reference/adapters/objectstore/minio/v1/interface.md)
adds bounded object operations, owned multipart sessions, incremental version
enumeration and restricted presigning. It consumes SDK-independent
[object-storage contracts](docs/reference/adapters/objectstore/v1/interface.md)
and supports direct or explicit Fixed/Follow composition with independent evidence.
[Framework common composition](docs/reference/framework/v1/interface.md) coordinates
public operation/resource shutdown, released-evidence reception and once-bound
safe localized logs. [Framework configuration](docs/reference/framework/configuration/v1/interface.md)
loads and watches project-owned application settings and independent business
values, preserving last-good data and separating publication from instance adoption.
The official [CLI](docs/reference/cmd/fathomry/interface.md) supplies offline help,
error explanations and translation-resource/coverage queries with shared failure
identity, explicit language selection and bounded invocation/cleanup behavior.
`fathomry new <project>` now generates an independent typed configuration project:
local or remote acquisition and YAML or TOML. Project Boot declares inputs,
settings and the selected Framework provider, without Adapter imports. Framework
owns acquisition, Load/Watch, released evidence and source cleanup; input lookup
and source selection remain explicit.
The generated executable validates configuration and exits; it is not a Worker or
a complete application runtime. There is no version command yet.

The earlier pre-release Adapter, failure, i18n, Framework, settings and CLI
surfaces were withdrawn for redesign; failure/v1, settings/v1 and i18n/v1 are rebuilt,
resource/v1 supplies public instance ownership, and adapters/v1 supplies shared
operation mechanisms. Configsource Adapters are rebuilt over those public foundations,
without a string-Condition compatibility facade. Internal configuration acquisition and
preparation improvements remain; removing the public layers does not revert
their native protocol, cancellation, authentication or type-admission corrections.

An internal Nacos v2 configuration integration supplies raw reads, bounded
invalidation subscriptions and owned protocol sessions. Its explicit compatibility
profile has local and isolated single-server verification, not production or
multi-node certification. See the [Nacos contract](docs/reference/internal/configsource/nacos/v2/interface.md).

The [internal PostgreSQL profile](docs/reference/internal/database/pgx/v5/interface.md)
and separate [MySQL profile](docs/reference/internal/database/mysql/v1/interface.md)
provide the native pools, SQL and transaction protocols reused by the public
Adapters. Real-service acceptance remains separate from protocol-peer tests;
no control schema or migration engine is included.

The [internal Zap integration](docs/reference/internal/logging/zap/v1/interface.md)
provides typed/contextual logging, multi-sink evidence and bounded Linux file
rotation, gzip and retention. It does not install an observability exporter/backend.

The [internal zerolog integration](docs/reference/internal/logging/zerolog/v1/interface.md)
provides bounded synchronous multi-sink JSON logging, structured context association
and local-file rotation/gzip. It is independent of Zap and does not implement a
public logger, production telemetry exporter or durable execution ledger.

The [internal OpenTelemetry integration](docs/reference/internal/telemetry/otel/v1/interface.md)
provides bounded logs, traces and metrics with explicit OTLP HTTP/protobuf export,
context propagation and independent evidence. Separate logging bridges preserve
existing local sinks. No Collector, Logstash or observability backend is deployed
or certified by the local protocol and TLS tests.

The [internal Kafka integration](docs/reference/internal/broker/franz/v1/interface.md)
provides bounded franz-go production, Kafka-only transactions, exact record reads,
direct consumer cursors and explicit checkpoints, with independent evidence.
Its [tests and service profile](docs/reference/internal/broker/franz/v1/verification.md)
do not certify consumer-group rebalances or the complete data/reference protocol.

## Documentation

Start with the [documentation map](docs/README.md) for current status and reading paths:

- [Architecture by topic](docs/README.md#architecture-by-topic): responsibilities,
  design rationale and cross-package guarantees.
- [Internal package reference](docs/README.md#internal-package-reference): each
  package's `interface.md` contract and detailed topics.
- [Development guides](docs/README.md#development-guides): SDK integration, testing
  and the canonical documentation-writing policy.

## Participate

- Read [AGENTS.md](AGENTS.md) and the
  [maintainer workflow](.github/CONTRIBUTING.md) before changing the repository.
- Use [Issues](https://github.com/frost-leo/fathomry/issues/new/choose) for
  actionable bugs, features, research proposals, and implementation tasks.
- Use [Discussions](https://github.com/frost-leo/fathomry/discussions) for questions
  and exploratory conversations.
- Follow [SECURITY.md](.github/SECURITY.md) for sensitive reports.

Maintenance is owner-led with Codex assistance. Only write-capable collaborators
can create PRs; public readers can inspect the project and share non-sensitive
feedback through Issues or Discussions. See [AGENTS.md](AGENTS.md) for session
entry points and [.agents/README.md](.agents/README.md) for pinned skill provenance.

`develop` is the integration and default branch before the first release. Every
file change, including initialization, enters it through a topic PR. The only
direct bootstrap push is a signed empty root commit. `main` is created from that
empty root for the first approved release PR and receives releases only; it then
becomes the default branch.

## Development workspace

The working layout is `lab/fathomry` for the product repository and `lab/reference`
for requirements, per-issue preparation, discussions, draft designs, and pinned
SDK source. Reference materials are workspace-local and do not arrive with a
Git clone; the product build must not depend on them. Public Issues retain enough
approved scope and sanitized evidence to identify missing handoff material.

Use the repository's `fathomry-development` skill to continue one confirmed issue,
then prepare one next issue with the owner. Product `docs/` holds established
contracts, accepted architecture and maintainer guides; its `reference/` section
is not the sibling working-literature workspace. Follow
[Writing documentation](docs/development/documentation.md) for placement, interface
contracts, truthful status and validation. The
[integration-standard index](docs/architecture/internal-sdk-integration.md) retains
accepted section identifiers and links to their topic pages.

## License

GNU General Public License v3.0 or later. See [LICENSE](LICENSE).
