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

The accepted design calls for two public paths (the runtime/capabilities are not
implemented by the current foundations):

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
Adapter consumes them. Public dependencies must not pull in concrete Providers,
SDK clients, framework orchestration or business workflows. Adapters depend on
their public contracts and private technical capabilities; Providers depend on
technical contracts/shared foundations, not public framework attribution, concrete
Adapters or business implementations. Composition
may know selected concrete implementations. All dependencies must remain acyclic.
Fathomry, not each independent business project, supplies composition. Business
authors declare configuration, choose permitted capabilities/named sources and
write workflow/node logic. They do not write resource factories, Viper wiring,
lease/admission accounting or independent evidence-reception loops. Public
contracts expose behavior, errors, results and diagnostics, not these authorities.
The intended entry is obtain CLI → `fathomry new <project>` → configuration and
business code → unified execution/query/control. That entry is not implemented
by this boundary refactor, and no generic third-party Provider SDK is implied.

Go's `internal` boundary protects implementation visibility; it is not a security
sandbox or a reason to hide contracts external consumers require.
[Go module organization](https://go.dev/doc/modules/layout)

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

A package/file review must answer:

- Can a reader infer its actual purpose and find one behavior without hopping
  through many tiny files?
- Do its contents change for the same reason and share invariants and ownership?
- Does extracting a package remove a real dependency or enforce a real boundary,
  rather than add forwarding and navigation cost?
- Does combining files retain Provider separation, testability, and a clear owner,
  rather than hide several unrelated responsibilities?
- Does selecting one Provider avoid importing other concrete SDK implementations?

These questions make both fragmentation and overloaded packages reviewable.
Earlier candidate names and layouts are not approved by satisfying this section.

## Current package ownership


| Package | Complete responsibility |
| --- | --- |
| `internal/fault` | Technical kinds, frozen context, multi-cause inspection and safe presentation |
| `internal/resource` | Configuration preparation, source identity/provenance, assembly, authoritative ownership, limits, admission and leases |
| `internal/invocation` | Requests/budgets, producers, outcomes/results, concrete read-only receipts, scopes/guards, required evidence delivery and optional observation |
| `internal/compatibility` | Consuming-build inspection, profiles, records, assessment and use policy |
| `internal/conformance` | Maintainer-only testing helpers; never a production dependency |

```text
internal/fault            -> standard library
internal/resource         -> internal/fault, existing YAML
internal/invocation       -> internal/resource, internal/fault
internal/compatibility    -> internal/resource, internal/fault
internal/conformance      -> internal mechanisms, testing (test support only)
```

No public error dependency, all-SDK aggregator or new module dependency is
introduced. Compatibility assessment accepts the authoritative
`*internal/resource.Access`; it does not introduce a public snapshot-assessment API.

## Pre-release API migration


The complete top-level `source`, `operation`, `compatibility` and `failure`
packages are withdrawn. There are no public aliases, forwarding constructors,
receipt interfaces, diagnostic shells or fixture-only bridges. The former public
error package had no production consumer after internalization; its tests and build
probe did not justify retaining a framework contract before the framework layer.
It is removed, not copied into another package or rebuilt in the test fixtures.

Future public capabilities and their error semantics will be designed against
actual framework needs. This is a deferred contract, not an implicit promise of
compatibility with the withdrawn Definition/Identity/Attribution API.

## Technical facts and future public meaning

The [error and evidence architecture](errors-and-evidence.md) owns the cross-package
rules. The [fault interface](../reference/internal/fault/interface.md) specifies
the implemented technical boundary; no public framework error API is prebuilt.

## Implementation references

[Internal package contracts](../README.md#internal-package-reference).

This topic carries forward the [accepted integration standard](internal-sdk-integration.md)
from [Issue #3](https://github.com/frost-leo/fathomry/issues/3), including the
[Issue #15](https://github.com/frost-leo/fathomry/issues/15) boundary refinements.
