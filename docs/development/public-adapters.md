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

# Maintain public Adapter contracts

[Documentation](../README.md) / Development

**Audience:** maintainers adding or changing any package in the Adapter tree.
**Status:** implemented role-based conventions and checks for all current
production packages. The [Adapter tree map](../../adapters/README.md) owns the
complete inventory and file responsibilities; future providers are not thereby
implemented or service-qualified.

A common standard means common obligations and change controls, not a universal
client interface. The [package boundaries](../architecture/package-boundaries.md),
[operation runtime](../reference/adapters/v1/interface.md) and
[resource lifecycle](../reference/resource/v1/interface.md) own the architecture.
This guide tells maintainers how to preserve it.

## PA-01 — Preserve dependencies and authority

Shared capability packages own provider-independent data/meaning, not SDK clients
or registries. A concrete provider depends on its public contracts and selected
Internal capability, never another concrete provider. Use `adapters/v1` for
operations/evidence and `resource/v1` for source generations. Do not add another
runtime, admission loop, universal provider interface or native-client escape
hatch to reduce repetition.

Share a mechanism only when its contract is genuinely shared. Native translation
and lifetime mappings remain provider-owned. Constructors, validation and offline
catalogs must not install global loggers, exporters or registries.

## PA-02 — Separate configuration from live authority

Where a provider exposes `Settings` and `Dependencies`, keep loadable data
separate from caller-owned authority. Shared Runtime Options, operation
Declarations and configuration Schemas are different contracts: explicit
composition callbacks do not belong in loadable provider Settings.
Use explicit, unique JSON names without omission that erases nil/zero meaning;
any mapstructure names must match. Settings must pass the existing strict
`configsource` schema, not acquire custom JSON/Text codecs, callbacks, contexts,
SDK objects or live handles. Document units, bounds, defaults, version axes and
absent/zero meanings next to declarations.

When exposed, `Validate` checks configuration, not service readiness, and
`Recommend` rejects invalid settings and leaves input unchanged. Both are
offline: no service construction/network or ambient global mutation. Document
narrow local preparation dependencies such as PostgreSQL's authorized parser-home
metadata/environment checks; offline does not promise no local metadata access.

Viper uses per-Load Settings and has no separate Validate/Recommend API.
Nacos exposes Validate but no Recommend. Do not invent those APIs merely to
satisfy a uniform spelling. Existing Viper native weak Decode/live environment
behavior is separate from strict configsource application preparation.

Freeze mutable inputs at the documented preparation/Open boundary. Do not permit
concurrent mutation of borrowed inputs. Credential replacement is explicit
composition authority, not an ambient refresh callback in Settings.

## PA-03 — Use authoritative effective budgets

Native defaults, work/evidence formulas and resident SDK reservations have one
owner: Internal. Consume metadata for the exact configuration used to construct
the source; do not copy private arithmetic/defaults into public policy. Defaulted
options helpers apply only where the public route exposes no later Internal
overlays. For overlaid settings, consume resolved preparation metadata.

| Provider | Native input to public policy |
| --- | --- |
| PostgreSQL/MySQL | `LimitsV1` and `EvidenceBytesV1`, validated and without later layers |
| MinIO | `BudgetV1`, validated and without later layers |
| Kafka | `BudgetV1`, including persistent clients and the defaulted record bound |
| Redis | Frozen `Prepared.Metadata`, after explicit-zero overlays resolve |
| DuckDB | Frozen `PrepareV1` / `PrepareResolvedV1` metadata, including finite/reader and source reservations |
| Trino | Frozen `PrepareV1` / `PrepareResolvedV1` metadata, including readiness, finite/reader and source reservations |
| Doris | Frozen `PrepareV1.Reservation`, including finite/page and source work/evidence charges |
| nethttp | Frozen `PrepareV1.Metadata` for final data/native selections, H2 buffers and root/evidence/source reservations |
| OpenTelemetry | Frozen `PrepareV1.Metadata` for final three-signal configuration, queue/span limits, cumulative metric residence, source/work and evidence charges |

This table covers data providers, not every Adapter role. Configuration
providers retain documented fixed public route envelopes and selected native
ceilings; they have no invented generic recommendation API. Core/capability/
preparation packages do not wrap a native service and must not copy provider
policies into themselves.

Public policy adds its own documented bridge, attribution, child-evidence and
source-owner costs. Distinguish root work, independent evidence, queue and resident
source reservations. These are declared envelopes, not measured RSS, remote
capacity, exact attempt counts or distributed quotas.

`Recommend` covers one source. Before constructing shared owners, reserve
overlapping generations explicitly: Kafka/Redis have `Compose`, MinIO has
`Policy.ForGenerations`, and SQL composition combines its needed one-source
envelopes explicitly. These APIs are not interchangeable. Sum concurrent source/
root capacities, cover the largest adopted root/evidence profile, and retain
retired-generation costs until actual shutdown. Never resize a caller's runtime
implicitly or let a larger adopted source bypass its frozen `Using` budget.

## PA-04 — Preserve owning and borrowing routes

The data providers share these roles:

- `Open(ctx, settings, dependencies)` returns an `Owner`. The context owns the
  source lifetime, not just setup waiting. A non-nil owner returned with an error
  still requires cleanup; nil ownership means no retained source authority.
- `Owner.Client()` and `Owner.Handle()` grant non-owning capabilities. `Using`
  consumes a typed public resource reference and borrows the actual Fixed/Follow
  generation for each root and its retained descendants.
- `Client.WithID` makes an immutable correlation view, not a new pool, admission
  allowance or source owner.
- `Owner.Close`, `Release` and `ShutdownComplete` retain reachable cleanup.
  Timed-out waiting is not release. Cleanup must not need new work/evidence
  admission or discard independent cleanup errors. Borrowers cannot close shared
  Runtime/Inbox/source owners.

Keep actual source/generation attribution through callbacks, deferred work,
sessions and credential replacement. Required evidence custody continues until
safe transfer and actual local termination; optional diagnostics never own it.

Nacos also has Open/Owner/Handle, but retains its existing
`Using(ref, dependencies)` contract without WithID or a data-provider budget
argument. Viper's `New(dependencies)` constructs a non-owning facade with no
native source Owner; Watch/Observe own their actual work. Configuration-core
Source/Observer describe complete raw acquisition, not native construction.
Shared Runtime and configuration Schema/Prepared ownership follow their own
contracts rather than the provider Owner pattern.

## PA-05 — Preserve provider-specific facts

Return typed, immutable facts with explicit copying/borrowing semantics. Keep
unavailable, native null, empty, partial and complete outputs distinct. Error,
effect and cleanup are independent; cancellation or lost acknowledgement never
proves absence of a write. Do not normalize unlike results/context lifetimes to
give all methods one signature.

| Provider | Differences that remain explicit and tested |
| --- | --- |
| PostgreSQL | Lazy pool ownership; explicit Ping. Begin context is setup-only; client lifetime retains the transaction. Savepoints and aborted commits have provider-specific evidence. |
| MySQL | Lazy pool ownership; explicit Ping. Begin context owns the transaction including native rollback. Statement errors and DDL implicit commit differ from PostgreSQL. |
| MinIO | Open performs bucket HEAD, not proof of write/version/delegation authority. Multipart/list/delegation handles preserve object/version/stream obligations. |
| Kafka | Open observes metadata, not producer/group readiness. Async production, transactions, direct reads and classic groups retain distinct outcomes; checkpoints are not business acknowledgements. |
| Redis | Construction may start topology/cache background work; Open is not readiness. Cache/messaging views share ownership but retain separate error domains, including mixed batches and lifecycle roots. |
| DuckDB | Open initializes a real embedded engine and may create the authorized database/WAL. Incremental Go consumption retains native materialization; Appender flush, transaction commit and complete result decoding remain distinct facts. |
| Trino | Open executes coordinator readiness, not a connector-permission proof. Query/Execute/Insert retain distinct result/effect facts; streamed pages remain provisional until terminal complete-query evidence. Canceling a page waiter does not cancel its owning query. |
| Doris | Open is local-only. Finite native SQL/load work is synchronous; the existing public receipt surface separates admission from outcome/evidence projection. QueryCursor retains a full owning lifetime; an admitted canceled page ends it. Commit, visibility, row quality and retained-label/payload identity remain separate. |
| nethttp | Open is local-only. Method context replaces Request.Context. Finite Do, retained streams and controlled direct Connection children retain different completion meanings; EOF and cleanup/evidence acknowledgement remain separate. |
| Viper | New only binds public dependencies; per-call Load acquires explicit files/readers. Native weak decoding/live environment differ from strict preparation. Borrowed readers remain caller-owned. |
| Nacos | Open acquires local ownership, not readiness. A complete capture/subscription retains one generation; already-acquired observation batches are not refetched on Next. |

Pre-admission refusal creates no accepted operation/evidence. Refusal after
public admission still retains evidence even when no native dispatch occurs;
accepted native/business failure likewise retains its independent facts. Do not
force synchronous SQL into a new generic receipt API or fabricate synchronous
success for asynchronous production.

## PA-06 — Use one error/presentation contract

Follow [Maintain public adapter error boundaries](adapter-errors.md), the
canonical declaration/mapping/redaction guide. Preserve shared public occurrences,
intentional native `errors.Is/As`, bounded cause traversal, partial effects and
cleanup attribution. Capability meaning, not SDK branding, chooses the owner.

Runtime handles refuse serialization and expose no mutable/native authority.
Settings deliberately permit sensitive explicit JSON; fmt/slog remain redacted.
Normalize optional typed-nil Settings pointers before slog as documented by the
packages. Explicit native/payload inspection may be sensitive. Offline definitions,
resources and CLI registration must preserve all other providers and never
construct sources.

## PA-07 — Organize by the responsibility that changes

Follow the canonical [file map and role profiles](../../adapters/README.md#common-responsibilities-not-identical-file-counts).
Apply one responsibility convention wherever that responsibility exists:
error construction/mapping is not offline definition or formatting; shared policy
vocabulary is not provider construction; raw acquisition is not native ownership.
Do not fabricate missing responsibilities, empty files or a universal interface.
Focused tests may cover related behavior together; no fixed file count or line
limit is a quality rule.

## PA-08 — Require executable change evidence

From the repository root, begin with:

```sh
go test -race -count=1 ./internal/conformance -run '^TestPublicAdapter'
go test -race -count=1 ./adapters/...
```

The shared [contract checks](../../internal/conformance/public_adapters_test.go)
exercise strict loading, zero/nil/copy semantics, redaction, opaque borrowing
projections, usable recommendations and rejected construction without retained
evidence. [Responsibility checks](../../internal/conformance/adapter_errors_test.go)
keep configuration, policy and offline metadata separately reviewable. These
guards do not replace provider-specific protocol/effect/lifetime tests.

The [whole-tree inventory and role checks](../../internal/conformance/adapter_packages_test.go)
cover every production package, its README entry/calling contract, foundational
dependency boundaries and declaration/presentation locations. An unclassified
new package or a stale entry fails the gate. The data-provider behavior
matrix is intentionally narrower than this all-role structural gate.

Retain a requirement → native authority → public path → test matrix. Verify
affected field/default parity, budgets, aliasing, cancellation, evidence saturation,
partial/lost effects, old generations and cleanup. Keep provider differences as
explicit cases, not skips. Use the existing independent-consumer modules for direct
and Framework Fixed/Follow paths; consumers must not import Internal.

Compare public declarations, configuration keys, error identities, locale data
and actual built offline CLI catalogs. A deliberate contract change requires an
explicit compatibility decision and corresponding comments/reference/tests, not
a silent change disguised as file reorganization. Then run normal tidy/format/
vet/race/build and relevant bounded fuzz checks.

Real-service mutation requires isolated authority, unique fixtures and independent
exact cleanup. Record the actual revision, toolchain, commands, outcomes, skips
and limits. Later passes do not erase failures; never change retry semantics just
to make acceptance green. Keep disposable builds in the prescribed temporary
area and permanent sanitized evidence with the issue.

Finish with independent review of contracts/compatibility, configuration/budgets
and lifetime/evidence ownership. Resolve findings and rerun affected checks.
Agent review is not independent human approval.
