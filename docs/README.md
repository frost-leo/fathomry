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

# Fathomry documentation

[Project overview](../README.md)

**Audience:** readers evaluating Fathomry and maintainers of its technical foundation.
**Status:** early development; public failure/i18n/version foundations, private mechanisms and accepted architecture.

Architecture explains cross-package decisions; package references specify calling
contracts; development guides explain tasks. Choose a reading path below.

## What is implemented

Configuration preparation, resource ownership/admission, controlled calls and
evidence handoff, technical errors, build/compatibility assessment and maintainer
testing support are implemented as internal foundations. A bounded Viper v1
integration is implemented under `internal/configsource/viper/v1`, with separately
versioned `OptionsV1` bootstrap settings and a raw-input preparation proof.
The Nacos v2 integration adds explicit native gRPC sessions, raw configuration
reads and bounded invalidations with a separate preparation proof. Its
[contract](reference/internal/configsource/nacos/v2/interface.md) distinguishes
the implemented compatibility profile and single-server checks from production
TLS or multi-node support.

The internal PostgreSQL profile under `internal/database/pgx/v5` adds explicit
pool ownership/statistics/expiration, bounded ordinary SQL and parameterized
results, reusable preparation, single-connection transactions and savepoints.
Its [contract](reference/internal/database/pgx/v5/interface.md) separates local
protocol verification and isolated PostgreSQL 18.6 acceptance from production support.

MySQL under `internal/database/mysql/v1` uses native `database/sql` pooling,
controlled queries/prepared statements and transactions. Its incoming framing and
LOCAL INFILE boundaries apply before SDK dispatch, including above verified TLS.
The [contract](reference/internal/database/mysql/v1/interface.md) distinguishes
isolated MySQL 8.4.11/InnoDB TLS writes over Unix sockets from separate TCP TLS reads.

The [Redis integration](reference/internal/cache/redis/v9/interface.md) adds bounded
native commands, batches, pinned transactions and subscriptions using official
go-redis v9.22.0 without a local SDK replacement. Its
[capability census](reference/internal/cache/redis/v9/capabilities.md) distinguishes
topologies, experimental caching/batching, service-version requirements and native
limits; [verification](reference/internal/cache/redis/v9/verification.md) separates
protocol checks from authorized Standalone, Sentinel, Cluster and Ring profiles.

The [Zap integration](reference/internal/logging/zap/v1/interface.md) adds typed,
context-aware logging with independent evidence, multiple sinks and bounded Linux
file rotation/gzip/retention. Its structured extension is composition-owned;
this integration neither imports zerolog nor installs an exporter/backend.

The [mail integration](reference/internal/notification/mail/v0/interface.md) adds
bounded outbound SMTP/MIME with verified TLS/authentication, plain/HTML bodies,
generic CID resources/attachments, reusable connections and independent per-message
and recipient-stage evidence. Chart rendering and recipient interaction remain
upper-layer responsibilities; relay acceptance is not inbox placement or reading.

The [Lark / Feishu integration](reference/internal/notification/lark/v3/interface.md)
adds bounded application notifications, native chart/table cards and CardKit
updates, media, signed custom bots and authenticated HTTP event reception.
Its opt-in [WebSocket receiver](reference/internal/notification/lark/v3/websocket.md)
adds bounded heartbeat/reconnect and explicit acknowledgements. CardKit writes were
verified, and the recipient confirmed the rendered Welcome/report. Cross-client
rendering and the live persisted positive-ACK probe remain unqualified. No service
or subscription settings are deployed implicitly.

Zerolog under `internal/logging/zerolog/v1` supplies bounded synchronous multi-sink
JSON logging, immutable structured records/context association and owned local
file rotation/gzip. Its [contract](reference/internal/logging/zerolog/v1/interface.md)
separates per-sink acceptance, cleanup and explicit file recovery from durable
execution evidence or production telemetry. It neither implements nor imports Zap.

The [OpenTelemetry integration](reference/internal/telemetry/otel/v1/interface.md)
adds bounded logs, spans and synchronous metrics, explicit context propagation,
native HTTP/protobuf export and independent evidence. Separate Zap/zerolog bridges
preserve existing local sinks and ownership. Export is explicitly scheduled;
local protocol/TLS/mTLS tests do not certify a Collector or production backend.

The public [failure contract](reference/failure/interface.md) provides extensible
semantic identity, immutable occurrences, typed-detail extension and bounded
optional diagnostics. It is a new limited contract, not the withdrawn API or a
public execution capability. No project generator, application configuration loader,
production-qualified application runtime or Run/Item model is supplied. The intended
`fathomry new <project>` entry is not runnable. Business authors are not expected to
recreate private assembly machinery as startup boilerplate.

The public [i18n foundation](reference/i18n/interface.md) prepares bounded external
English/translation resources and renders plain text with explicit locale/fallback,
named scalar/cardinal contracts, source freshness and immutable snapshot metadata.
Independent business consumers own their resources. The failure package remains
localization-independent; no CLI/report/runtime integration or full language catalog
is supplied.

The public [version foundation](reference/version/interface.md) separates exact
software identity from precedence, native application/framework/dependency build
facts and validated application declarations. Its [injection contract](reference/version/injection.md)
requires actual artifact read-back; optional [presentation](reference/version/presentation/interface.md)
uses existing i18n resources. No business versioning, CLI or release pipeline is supplied.

The [Temporal integration](reference/internal/orchestration/temporal/v1/interface.md)
adds named Namespace Clients, native execution handles, managed Worker registration
and shutdown, Activity/Local Activity/Nexus callback evidence and controlled raw
service access. Native Schedules, Worker Deployments, Sessions, serialization and
backed/standalone Nexus operations have selected isolated service/replay evidence.
The compatible browser profile also verifies history, Query and codec rendering.
Logging, metrics and tracing use native injected interfaces, not concrete upper-
layer Provider dependencies. Optional transport/backend profiles and broader
deployment qualification remain separate from the implemented SDK contracts.

The [Kafka integration](reference/internal/broker/franz/v1/interface.md) adds bounded
franz-go production, Kafka-only atomic batches, exact historical reads, direct
consumer cursors and explicit standalone checkpoints with independent evidence.
Its [verification profile](reference/internal/broker/franz/v1/verification.md)
distinguishes local SDK/fault tests from the owner-authorized Kafka service.
It is not the framework's complete data/reference or workflow recovery protocol.

The [MinIO integration](reference/internal/objectstore/minio/v7/interface.md) adds
bounded object reads/downloads, conditional serial multipart uploads, native copy,
version/listing and per-target removal evidence, optional tags and explicit cleanup.
Its [verification boundary](reference/internal/objectstore/minio/v7/verification.md)
separates local protocol/race qualification from the owner-authorized, isolated
HTTP/unversioned service profile. It does not implement Run freezing, archives
or publication protocols.

The [Iceberg integration](reference/internal/tableformat/iceberg/v0/interface.md)
adds bounded format-2 batch reads/writes, schema/partition evolution, snapshot
operations and restricted rewrites. It owns an independent S3 FileIO and does not
import the MinIO integration. Its [verification profile](reference/internal/tableformat/iceberg/v0/verification.md)
separates native/local checks from isolated Catalog/S3 acceptance and unverified
cross-engine or large-data behavior.

The [Trino integration](reference/internal/sqlengine/trino/v0/interface.md) adds
bounded native SQL batches, finite direct JSON queries, DDL/CTAS and restricted
UPDATE/DELETE/MERGE/TRUNCATE, with independent effect and cleanup evidence.
Its Trino 482/Iceberg v2 acceptance is separate from the direct Iceberg provider;
spooling, managed multi-statement transactions and production auth qualification
are explicitly excluded.

The [DuckDB integration](reference/internal/sqlengine/duckdb/v2/interface.md) adds
bounded local native SQL, many-row results, prepared parameter batches, Appender
writes and explicit transactions with independent evidence. CGO is required;
native cancellation and memory limits remain qualified rather than hard bounds.
Its [verification profile](reference/internal/sqlengine/duckdb/v2/verification.md)
separates the product API from independent native Iceberg REST probes; external
catalogs and Arrow are not exposed by this provider.

The [Doris integration](reference/internal/sqlengine/doris/v1/interface.md) adds
strict labeled JSON Stream Load, retained-label inspection and bounded single-use
SQL with independent effect and cleanup evidence. Native-table ingestion and SQL
DML are qualified on an isolated Doris 4.1.4 profile; the
[capability classification](reference/internal/sqlengine/doris/v1/capabilities.md)
preserves external-catalog restrictions without adding backend SDKs or storage
management. SQL acknowledgement is not a visibility or external-commit certificate.

The [net/http v1 integration](reference/internal/httpclient/nethttp/v1/interface.md)
adds independently configured standard HTTP clients, bounded response streams and
owned direct connections, with native runtime inputs and independent evidence.
Explicit runtime proxy choices preserve source identity and isolate native H2
route/authentication pools; configured routing can be locked against overrides.
H1/H2, explicit cleartext H2, local CONNECT and TLS/mTLS tests remain separate from
production-site qualification; business credential and Provider-selection policies
are not supplied. The integration/configuration versions are distinct from Go's
standard-library version.

The [tls-client v1 integration](reference/internal/httpclient/tlsclient/v1/interface.md)
adds externally configured native/custom profiles, controlled H1/H2/H3-racing
requests and streams, runtime proxy isolation and shared native resource bounds.
Its local SDK compatibility patch and Provider have separate version axes.
Upstream license compatibility remains unresolved; implementation delivery does
not establish distribution permission or production/arbitrary-profile qualification.

The [Nuki v1 integration](reference/internal/httpclient/nuki/v1/interface.md) uses
the distinct Nukilabs SDK with external native profiles, controlled H1/H2/H3,
per-call routing and source-wide ownership/evidence bounds. Go 1.27.0 is required.
Local Nuki, SOCKS, QUIC and QPACK corrections retain separate provenance and
regressions. The SOCKS dependency's license/publication status remains unresolved;
implementation is not distribution clearance or production-site qualification.

The [Surf v1 integration](reference/internal/httpclient/surf/v1/interface.md)
adds externally configured native/custom JA profiles, controlled H1/H2/H3 requests
and streams, runtime CONNECT/SOCKS routing, bounded native work and independent
evidence. Go 1.27, Surf and the local Surf/H2/H3 corrections have separate version
axes. H3 does not imply JA fidelity. Native fork licensing and production-site
qualification remain explicit prerequisites, not consequences of fixture passes.

The [HTTPcloak v1 integration](reference/internal/httpclient/httpcloak/v1/interface.md)
adds explicit H1/H2/direct-H3 requests and streams, external native/JSON presets,
dynamic TCP proxies, bounded native ownership and independent framing/callback
evidence. SDK corrections are separate from the Provider contract. UDP/MASQUE
proxies, automatic protocol racing and native Session policy are not qualified.

The [chromedp v0 integration](reference/internal/browser/chromedp/v0/interface.md)
adds named browser Providers with explicit process/connection ownership, isolated
BrowserContext sessions, controlled native CDP/Actions, runtime proxy inputs and
independent evidence. Browser load, output, cancellation and disposal are separate
facts. Session concurrency is not a physical HTTP quota or a native-memory limit.

Fixture passes and pinned YAML behavior do not establish service support, durable
recovery or native-memory guarantees. See [package boundaries](architecture/package-boundaries.md).
The [configuration contract](reference/internal/resource/configuration.md),
[acceptance probes](reference/internal/conformance/diagnostics.md) and
[cleanup-history budgets](reference/internal/resource/shutdown.md) describe the
current internal safeguards and their limits.

## Choose a reading path

| Your task | Start here |
| --- | --- |
| Define, inspect or extend a public failure | [Public failure contract](reference/failure/interface.md) |
| Prepare and render business-owned localized resources | [Public i18n contract](reference/i18n/interface.md) |
| Inspect software/build versions or inject application declarations | [Public version contract](reference/version/interface.md) |
| Understand responsibilities and design | [Package boundaries](architecture/package-boundaries.md), then the relevant architecture topic |
| Work on one internal package | Its [interface.md](#internal-package-reference), then the package's topics and source/examples |
| Prepare an approved integration or upgrade | [Integration workflow](development/sdk-integration.md) and [integration architecture](architecture/sdk-integration.md) |
| Reproduce a test or verify a change | [Testing](development/testing.md) and the affected package contract |
| Add or reorganize documentation | [Writing documentation](development/documentation.md) and [contribution policy](../.github/CONTRIBUTING.md) |

## Architecture by topic

These pages own cross-package reasoning and obligations, not package API listings.

| Topic | Questions answered |
| --- | --- |
| [Package boundaries](architecture/package-boundaries.md) | Who owns capabilities, assembly and technical mechanisms? What is private? |
| [Configuration and named sources](architecture/configuration.md) | Who selects sources, authorizes settings and owns configuration meaning? |
| [Resource lifetime](architecture/resource-lifecycle.md) | Who owns live work and dependencies after cancellation or partial failure? |
| [Errors and evidence](architecture/errors-and-evidence.md) | How do technical facts, future framework meaning and evidence differ? |
| [Data guarantees and resource limits](architecture/resource-limits.md) | What do bounds and acknowledgements actually prove, and at what scope? |
| [Versions and compatibility](architecture/versioning.md) | Which version and evidence axes must remain separate? |
| [SDK integration architecture](architecture/sdk-integration.md) | Which shared mechanisms and native-specific proofs are required? |
| [Design tradeoffs](architecture/decisions.md) | Which alternatives were rejected, and which boundaries remain open? |

The [integration-standard index](architecture/internal-sdk-integration.md) preserves
the existing S01-S12 identifiers and links each to its canonical topic.

## Internal package reference

The path follows the actual package, then a substantive topic. `interface.md` is
the contract entry, including functions and concrete types; it does not require
a Go interface declaration. These are in-module contracts, not an external SDK.

| Package contract | Detailed topics |
| --- | --- |
| [`internal/resource`](reference/internal/resource/interface.md) | [Configuration](reference/internal/resource/configuration.md), [ownership](reference/internal/resource/ownership.md), [admission](reference/internal/resource/admission.md), [shutdown](reference/internal/resource/shutdown.md) |
| [`internal/invocation`](reference/internal/invocation/interface.md) | [Budgets](reference/internal/invocation/budgets.md), [completion](reference/internal/invocation/completion.md), [evidence](reference/internal/invocation/evidence.md), [observation](reference/internal/invocation/observation.md) |
| [`internal/fault`](reference/internal/fault/interface.md) | [Diagnostics and inspection boundaries](reference/internal/fault/diagnostics.md) |
| [`internal/compatibility`](reference/internal/compatibility/interface.md) | [Build facts](reference/internal/compatibility/build-info.md), [assessment and policy](reference/internal/compatibility/assessment.md) |
| [`internal/conformance`](reference/internal/conformance/interface.md) | [Diagnostic probes](reference/internal/conformance/diagnostics.md), [fixtures and evidence classes](reference/internal/conformance/fixtures.md) |
| [`internal/configsource/viper/v1`](reference/internal/configsource/viper/v1/interface.md) | Native local profile, OptionsV1, raw handoff, ownership, bounds and version evidence |
| [`internal/configsource/nacos/v2`](reference/internal/configsource/nacos/v2/interface.md) | Native protocol-component profile, OptionsV1, authentication, raw handoff, observation and owned sessions |
| [`internal/database/pgx/v5`](reference/internal/database/pgx/v5/interface.md) | Native pool lifecycle/statistics, ordinary SQL, reusable preparation, bounded results, transactions/savepoints and independent evidence |
| [`internal/database/mysql/v1`](reference/internal/database/mysql/v1/interface.md) | Native sql.DB pooling, framed/TLS transport, controlled SQL/preparation, transactions and independent evidence |
| [`internal/notification/mail/v0`](reference/internal/notification/mail/v0/interface.md) | SMTP/TLS/authentication, MIME bodies and assets, bounded connection reuse, partial/unknown effects and independent evidence |
| [`internal/notification/lark/v3`](reference/internal/notification/lark/v3/interface.md) | Native charts/tables, application and signed-bot messages, lifecycle/inspection, media, CardKit updates and authenticated HTTP callbacks |
| [`internal/broker/franz/v1`](reference/internal/broker/franz/v1/interface.md) | [Options/bounds](reference/internal/broker/franz/v1/options.md), producer/direct consumer/checkpoints, [verification](reference/internal/broker/franz/v1/verification.md) |
| [`internal/broker/franz/v1/otelbridge`](reference/internal/broker/franz/v1/otelbridge/interface.md) | Explicit bounded W3C header translation, without native SDK instrumentation hooks |
| [`internal/objectstore/minio/v7`](reference/internal/objectstore/minio/v7/interface.md) | Bounded reads/transfers, conditional multipart, copy/versions/listing/removal, independent evidence and [verification](reference/internal/objectstore/minio/v7/verification.md) |
| [`internal/tableformat/iceberg/v0`](reference/internal/tableformat/iceberg/v0/interface.md) | Batch table access, independent S3 FileIO, evolution/snapshots, bounded rewrites and [verification](reference/internal/tableformat/iceberg/v0/verification.md) |
| [`internal/sqlengine/trino/v0`](reference/internal/sqlengine/trino/v0/interface.md) | Bounded native SQL batches, direct JSON results, query-specific ownership, native DML and explicit effect/cleanup evidence |
| [`internal/sqlengine/duckdb/v2`](reference/internal/sqlengine/duckdb/v2/interface.md) | Native SQL, exact bounded scalar results, prepared/Appender batches, local transactions and [qualification limits](reference/internal/sqlengine/duckdb/v2/verification.md) |
| [`internal/sqlengine/doris/v1`](reference/internal/sqlengine/doris/v1/interface.md) | Bounded native Stream Load and SQL, label/row/visibility evidence, explicit connection ownership and [capability limits](reference/internal/sqlengine/doris/v1/capabilities.md) |
| [`internal/logging/zap/v1`](reference/internal/logging/zap/v1/interface.md) | Typed logging, native multi-sink results, context extensions and [local file ownership/rotation](reference/internal/logging/zap/v1/file-output.md) |
| [`internal/logging/zerolog/v1`](reference/internal/logging/zerolog/v1/interface.md) | Structured multi-sink logging/context, bounded evidence and [local file output](reference/internal/logging/zerolog/v1/file-output.md) |
| [`internal/browser/chromedp/v0`](reference/internal/browser/chromedp/v0/interface.md) | Isolated browser sessions, scoped native CDP/Actions, explicit lifecycle/cleanup and browser-version evidence |
| [`internal/telemetry/otel/v1`](reference/internal/telemetry/otel/v1/interface.md) | Logs, traces, metrics, propagation, explicit bounded export, ownership, options/errors and compatibility |
| [`internal/telemetry/otel/v1/zapbridge`](reference/internal/telemetry/otel/v1/zapbridge/interface.md) | Borrowed typed Zap structured sink; Sync does not flush telemetry |
| [`internal/telemetry/otel/v1/zerologbridge`](reference/internal/telemetry/otel/v1/zerologbridge/interface.md) | Borrowed immutable zerolog RecordWriter; independent telemetry lifecycle |

Exact declarations and symbol comments live with the Go source. Each entry links
implementation/tests; use `go doc -all ./internal/<package>` from the repository root.

## Development guides

- [Develop and verify an SDK integration](development/sdk-integration.md).
- [Run tests and verification](development/testing.md).
- [Write documentation](development/documentation.md), the canonical writing policy.
- [Contribution policy](../.github/CONTRIBUTING.md), the canonical branch/signing/review rules.

## Product documentation and working literature

`docs/reference/` is versioned product documentation. Sibling `../reference/`
contains working requirements, research, issue discussions, raw experiments and
pinned SDK sources. A clone does not fetch that workspace and the product does not
depend on it. A contract must be understandable without a private issue transcript.

Tutorials, operator guides and public API references will be added when the actual
capabilities exist. Empty categories and speculative commands do not replace implementation.
