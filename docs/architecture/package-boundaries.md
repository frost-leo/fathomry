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

# Package boundaries and responsibilities

[Documentation](../README.md) / Architecture

**Audience:** framework and integration maintainers.
**Status:** accepted architecture; implementation coverage is limited.

This topic states accepted cross-package obligations, not a claim that every
SDK or framework feature is implemented. See the [documentation map](../README.md)
for current availability and package contracts.

**Standard sections:** S01, S09.

## Contents

- [Roles and collaboration](#roles-and-collaboration)
- [Cohesive package organization](#cohesive-package-organization)

## Roles and collaboration

The role names below describe responsibilities, not mandatory Go packages.

| Role | Owns | Does not own |
| --- | --- | --- |
| Framework | Public capability contracts; execution identity and attribution rules; orchestration and control; interpretation of business results; reliable evidence recording and Item/Run disposition | Invented service guarantees or the mechanics of every SDK |
| Adapter | Implements a public capability using internal technical capabilities; enforces its public-operation constraints; freezes execution attribution, adds public error meaning and hands required evidence to the framework | An independent terminal-state policy, resource ownership obtained merely by borrowing, or a competing public error catalog |
| Internal | Process-local technical capabilities and reusable mechanisms for configuration preparation, ownership, controlled operations, technical evidence, safe diagnostics, and version accountability | Workflow-specific business meaning, Item/Run terminal decisions, or application-wide orchestration |
| Framework-supplied composition boundary | Resolves authorized configuration and dependencies; assembles selected implementations and named instances; grants bounded capabilities; owns application startup and shutdown coordination | Per-Item mutable state inside shared clients or an unrestricted runtime service locator |

The accepted design calls for two public paths (shared public operation ownership
is implemented, but the concrete capability/scenario paths remain incomplete):

- **Framework orchestration/control:** the framework defines the operation and
  execution rules, delegates technical work through the appropriate Adapter,
  and interprets the returned business and technical evidence.
- **Business I/O:** business code chooses permitted queries, requests, transfers,
  or writes through public capabilities. Its Adapter still applies the relevant
  constraints, attribution, error contract, and evidence handoff. Flexibility
  does not mean taking an internal client or bypassing these obligations.

In both paths, technical results travel from Provider through Adapter to the
public caller and, where required, the framework's evidence boundary. This is
a collaboration path, not a requirement for a wrapper at every diagram arrow.

Public semantic errors belong to the framework contract. Internal technical errors,
correlation and required-evidence mechanics do not become public merely because an
Adapter consumes them. Public shared contracts must not pull in concrete Providers,
SDK clients, framework orchestration or business workflows. Adapters depend on
their public contracts and private technical capabilities; Providers depend on
technical contracts/shared foundations, not public framework attribution, concrete
Adapters or business implementations. Composition
may know selected concrete implementations. All dependencies must remain acyclic.

The HTTP capability vocabulary in `adapters/httpclient/v1` shares declared
accounting and attribution only. The nethttp provider retains native request,
context, stream and direct-connection semantics; later HTTP providers must not
depend on its implementation or infer a universal Complete meaning.
Fathomry, not each independent business project, supplies composition. Business
authors declare configuration, choose permitted capabilities/named sources and
write workflow/node logic. The generated project Boot declares those choices through Framework scenario
APIs, without Adapter imports. Framework assembles public Adapters and owns its
scenario lifetimes; projects do not reimplement lease/admission accounting,
source protocols or evidence-reception loops. Public
contracts expose behavior, errors, results and diagnostics, not these authorities.
The entry is obtain CLI → `fathomry new <project>` → configuration and business
code. Project generation and finite configuration startup are implemented; unified
execution/query/control remains future work. No generic third-party Provider SDK
is implied.

Go's `internal` boundary protects implementation visibility; it is not a security
sandbox or a reason to hide contracts external consumers require.
[Go module organization](https://go.dev/doc/modules/layout)

Public shared mechanisms are independent implementations over public foundations,
not facades over Internal engines. [Adapters/v1](../reference/adapters/v1/interface.md)
owns bounded operations and independent evidence; [resource/v1](../reference/resource/v1/interface.md)
owns instance generations. Concrete provider integration may translate native
Internal capabilities at its explicit boundary. Error construction does not depend
on an i18n instance; presentation/log composition supplies that optional dependency.

The I/O path operates in Activities or other appropriate process-local callers,
not directly in Temporal Workflow logic. A public wrapper does not make network
I/O, ordinary goroutines, or wall-clock control deterministic. Durable execution
receives serializable results or references, not live clients, streams, or native
handles. Workflow command and serialization changes require replay evidence.
[Temporal deterministic constraints](https://docs.temporal.io/workflow-definition#deterministic-constraints)

Two end-to-end examples make the division concrete:

- **Framework input preparation:** the framework requires the complete frozen
  input before starting business execution. Adapters translate authorized source
  reads and object writes into technical operations and retain their attribution.
  Internal bounds the resource lifetimes and returns read, transfer, and cleanup
  evidence. If only some objects were written, the framework must not treat that
  partial preparation as complete. Internal does not decide when a business Run
  may start or invent a cross-service transaction; it executes the authorized
  technical operations. Snapshot and publication mechanisms remain separate
  implementation decisions.
- **Flexible business output:** business code chooses a permitted operation and
  declares identity, conflicts, and success conditions. The Adapter preserves
  Item ownership through physical batching; the Provider reports delivery or
  write evidence at the granularity the SDK supports. Shared mechanisms protect
  the applicable bounds and evidence handoff. Handling a public error does not
  erase required facts, while a partial technical failure does not itself decide
  whether the Item or Run must fail. The framework makes that disposition under
  its public contract.

## Cohesive package organization

Package names must be concise, idiomatic English and describe a clear capability
or bounded responsibility. A package's documentation must explain its purpose,
ownership, consumers, and dependency direction. Names such as `common`, `utils`,
or `helpers` do not justify collecting unrelated functionality; nor does a package
called `types` justify moving every contract away from its owner. Naming is a
design review, not merely a prohibited-word scan.
[Go package naming guidance](https://go.dev/blog/package-names)

Create a package for a real boundary such as independent Provider selection,
dependency direction, a reusable capability, or access/ownership restrictions.
Do not create a package solely for a diagram box, a few moved types, or a speculative
future implementation. Prefer concrete types and native facilities; justify small
consumer-owned interfaces at actual seams rather than mirroring entire SDKs.

Files should contain meaningful functional units that can be understood together.
Do not default to a file per type, method, interface, or lifecycle phase. Split
when responsibilities or platform/build constraints genuinely differ; combine
when fragments jointly describe one straightforward behavior and obscure its
invariants. Adjacent tests should follow the same functional organization.
Compactness does not justify giant mixed-purpose files, duplicated mechanisms,
or merging unrelated Providers. No fixed tree or numerical file/line limit is set.

The current configuration integration uses `internal/configsource/viper/v1`: capability,
SDK, then SDK major. Grouping directories do not require forwarding Go packages or
a universal Provider interface. Its `OptionsV1` option type in `options.go`
versions a different contract from the SDK major. Exact SDK versions remain build
facts, not per-patch directory names. See the [package contract](../reference/internal/configsource/viper/v1/interface.md).

PostgreSQL follows the same capability/SDK/major organization at
`internal/database/pgx/v5`, without a grouping-level Go package. Its
[contract](../reference/internal/database/pgx/v5/interface.md) specifies the selected
native pgx/puddle ownership profile; it is not a common PostgreSQL/MySQL interface
or a workflow-control schema.

MySQL at `internal/database/mysql/v1` independently owns `database/sql.DB` pooling
and controlled native connections. Its [contract](../reference/internal/database/mysql/v1/interface.md)
uses MySQL framing, preparation, authentication and transaction-status evidence;
it does not inherit pgxpool lifecycle or PostgreSQL transaction-abortion semantics.
Neither integration imports the other or introduces a grouping-level interface.

Provider-specific settings stay in `options.go`, not a central data-structure
directory. Operation-specific input/output types stay with their operation.
`errors.go` owns this implementation's error identity and shared-fault adaptation;
core function groups pair with focused tests, and `integration_test.go` proves
their actual composition. These are responsibility conventions, not a requirement
to generate empty files or implement identical resource lifecycles for every SDK.

OpenTelemetry lives at `internal/telemetry/otel/v1`, with native capture/aggregation
and explicit bounded export. Its optional `zapbridge` and `zerologbridge` packages
own different structured translations and preserve dependency selection:
core telemetry imports neither logging provider. The main SDK-major path does not
stabilize the beta Logs modules or version configuration and OTLP together. See
the [implemented contract](../reference/internal/telemetry/otel/v1/interface.md).

Kafka follows this organization at `internal/broker/franz/v1`. Its
[contract](../reference/internal/broker/franz/v1/interface.md) combines controlled
production, exact reads, direct consumers and explicit checkpoints. Native
producer shutdown and callback completion remain separate owned obligations;
metadata and required evidence do not inherit business publication meaning.
The optional `otelbridge` owns only explicit header/context translation.

SQL-engine Adapters own their common public contracts at
`adapters/sqlengine/v1`, separately from the database contracts used by
PostgreSQL/MySQL. This boundary contains source/operation observations and
source/work/evidence budgets, not aliases of the database layer or a universal
SQL execution interface. Embedded-engine, coordinator-query and Doris load/cursor
semantics remain provider-specific. Existing public operation/resource mechanisms
still own admission, evidence and generation lifetimes; the category package
introduces no competing runtime or new error domain.

A package/file review must answer:

- Can a reader infer its actual purpose and find one behavior without hopping
  through many tiny files?
- Do its contents change for the same reason and share invariants and ownership?
- Does extracting a package remove a real dependency or enforce a real boundary,
  rather than add forwarding and navigation cost?
- Does combining files retain Provider separation, testability, and a clear owner,
  rather than hide several unrelated responsibilities?
- Do provider-only contracts avoid unrelated SDK imports, and does composition
  document its built-in dependency closure without initializing unused clients?

These questions make both fragmentation and overloaded packages reviewable.
Earlier candidate names and layouts are not approved by satisfying this section.

## Current package ownership


| Package | Complete responsibility |
| --- | --- |
| `failure/v1` | Public numeric/symbolic identity, component-owned error composition, native-cause access and definition atlas |
| `settings/v1` | Typed configuration snapshots, atomic publication/read-only views and explicit application-default data access; no sources or runtime lifecycle |
| `i18n/v1` | Explicit resource/translation catalogs, code explanations and settings-backed presentation; no rewrite of operation/native error identity |
| `resource/v1` | Explicit runtime scopes, typed instance bindings, fixed/follow adoption, generation borrowing and retained cleanup; no parsing, SDK or Internal mechanism dependency |
| `adapters/v1` | Independent public operation admission, actual-work ownership and evidence custody; no Internal engine dependency |
| `adapters/configsource/v1` | Raw source/batch contracts and independent strict preparation; no native provider or Internal dependency |
| `adapters/configsource/{viper,nacos}/v1` | Full supported native capability translation, loadable settings, public evidence/lifecycle composition and selected raw profiles |
| `adapters/database/v1` | Shared public budget, attribution and effective-profile data; no private/native engine or universal SQL client |
| `adapters/database/{postgres,mysql}/v1` | Complete supported native SQL/preparation/transaction translation, source and retained-generation ownership, independent evidence and provider-specific results |
| `adapters/broker/v1` | Shared public broker budget and attribution data; no native engine or universal client |
| `adapters/broker/kafka/v1` | Kafka-specific production, exact/direct/group consumption, checkpoints and retained source/evidence ownership |
| `framework/v1` | Composition of public runtime owners, released-evidence reception and bound presentation/logging; no SDK or Internal engine |
| `framework/configuration/v1` | Explicit input binding and provider-owned typed Load/Watch; source/policy association, fenced publication, released facts and opt-in borrowed resource adoption |
| `cmd/fathomry` | Official offline catalogs and exclusive project generation; private command implementation, not a public extension SDK |
| `internal/fault` | Technical kinds, frozen context, multi-cause inspection and safe presentation |
| `internal/resource` | Configuration preparation, source identity/provenance, assembly, authoritative ownership, limits, admission and leases |
| `internal/invocation` | Requests/budgets, producers, outcomes/results, concrete read-only receipts, scopes/guards, required evidence delivery and optional observation |
| `internal/compatibility` | Consuming-build inspection, profiles, records, assessment and use policy |
| `internal/conformance` | Maintainer-only testing helpers; never a production dependency |

```text
failure/v1               -> standard library
settings/v1              -> failure/v1, standard library
i18n/v1                  -> failure/v1, settings/v1, x/text, standard library
resource/v1              -> failure/v1, settings/v1, standard library
adapters/v1              -> failure/v1, resource/v1, standard library
adapters/configsource/v1 -> failure/v1, settings/v1, YAML/TOML, standard library
concrete configsource    -> public foundations, selected Internal configsource
adapters/database/v1     -> public adapters/v1, standard library
concrete database        -> public database/resource/operation/failure contracts, selected Internal database
framework/v1             -> public failure/settings/i18n/resource/adapters
framework/configuration  -> public configsource preparation, selected Adapter implementations and shared foundations
internal/fault            -> standard library
internal/resource         -> internal/fault, existing YAML
internal/invocation       -> internal/resource, internal/fault
internal/compatibility    -> internal/resource, internal/fault
internal/conformance      -> internal mechanisms, testing (test support only)
```

The Internal production mechanisms retain no public error dependency or all-SDK
aggregator. Failure imports only the standard library. Compatibility assessment accepts the authoritative
`*internal/resource.Access`; it does not introduce a public snapshot-assessment API.
The generated project owns project choices and data; it imports Framework
configuration and ordinary data foundations, not Adapters or Internal.
Framework Viper/Nacos declarations explicitly select the source implementation.
Load/Watch constructs and owns that scenario's operation/evidence/source machinery.
There is no provider-name registry, ambient inference, global service locator or
compatibility facade. Direct public Adapter use remains a separate supported path,
not an extra assembly obligation on the generated Boot. Shared data contracts and
scenario-specific policies must not expose native/runtime handles as settings.

The [Adapter tree map](../../adapters/README.md) owns the role-specific file
conventions for every current Adapter package. Source follows cohesive
responsibilities with matching focused tests; shared contracts and configuration
providers do not acquire the data-provider API merely to match a file template.
Cross-package, Framework and external-consumer checks live in
`integration_test.go`. Separately authorized service tests retain explicit
opt-in/build-tag boundaries. Review-round, stress and fuzz categories do not
create parallel file taxonomies: those tests stay with the capability they test.

Database Adapters reuse the original native pools. Their non-exported provider
code translates private assemblies/receipts; no additional `adapters/database/internal`
package or exported native bridge is introduced. A transaction/preparation borrows
one public generation through final cleanup. Setup contexts, retained lifetimes,
initial preparation readiness and final evidence remain distinct. Finalization
uses existing reservations, and evidence redelivery never reruns SQL. Framework
composition uses these same public capabilities rather than another database facade.
The [PostgreSQL](../reference/adapters/database/postgres/v1/interface.md) and
[MySQL](../reference/adapters/database/mysql/v1/interface.md) contracts preserve
their different context, result, transaction and session-reset semantics.

## Pre-release API migration


The unversioned `source`, `operation`, `compatibility` and `failure` packages remain
withdrawn. The new public `failure/v1` uses uint32 Code with an
[explicit capability-domain allocation protocol](../reference/failure/v1/code-allocation.md), a distinct Identifier,
explicit definitions and extensible owner data. The former string Condition API
and presentation-fact atlas are not retained as aliases or forwarding facades.
[Settings](../reference/settings/v1/interface.md) now supplies data storage/access,
not an application host or source loader. [I18n](../reference/i18n/v1/interface.md)
supplies resource catalogs and error presentation separately.
[Resource](../reference/resource/v1/interface.md) now supplies independent public
instance ownership without restoring private assembly facades. Public operation
mechanisms and concrete configsource Adapters now use those public foundations;
strict public preparation is independent of Internal's engine. Framework common
composition and configuration scenarios now reuse those public owners. The official
[CLI](../reference/cmd/fathomry/interface.md) provides offline catalogs and project generation through
private command packages; the former public CLI Go API remains withdrawn. No complete
application/Worker runtime is implied. No compatibility
with the unreleased outer interfaces or earlier uint64 code draft is implied.

## Technical facts and future public meaning

The [error and evidence architecture](errors-and-evidence.md) owns the cross-package
rules. The [fault interface](../reference/internal/fault/interface.md) specifies
the implemented technical boundary. The separate
[failure contract](../reference/failure/v1/interface.md) supplies the public core,
without making private providers depend on it or defining every component's data.

## Implementation references

[Internal package contracts](../README.md#internal-package-reference).

This topic carries forward the [accepted integration standard](internal-sdk-integration.md)
from [Issue #3](https://github.com/frost-leo/fathomry/issues/3), including the
[Issue #15](https://github.com/frost-leo/fathomry/issues/15) boundary refinements.
