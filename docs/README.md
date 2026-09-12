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
**Status:** early development; private mechanisms and accepted architecture.

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

There is no public Go package, project generator, application configuration loader,
production service Provider or complete Temporal execution runtime yet. The intended
`fathomry new <project>` entry is not runnable. The removed public `failure` contract
is not replaced by a speculative error API. Business authors are not expected to
recreate private assembly machinery as startup boilerplate.

Fixture passes and pinned YAML behavior do not establish service support, durable
recovery or native-memory guarantees. See [package boundaries](architecture/package-boundaries.md).
The [configuration contract](reference/internal/resource/configuration.md),
[acceptance probes](reference/internal/conformance/diagnostics.md) and
[cleanup-history budgets](reference/internal/resource/shutdown.md) describe the
current internal safeguards and their limits.

## Choose a reading path

| Your task | Start here |
| --- | --- |
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
