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

# Application bootstrap and main configuration

[Documentation](../../../../README.md) / Public package reference

**Audience:** application-entry and framework-command authors.
**Status:** implemented source candidate; local and isolated Nacos protocol-peer
verification, not published-module or production-service qualification.
**Package:** `github.com/frost-leo/fathomry/framework/configuration/v1`.

## Responsibility

This package supplies two application-configuration operations:

1. `LoadBootstrap` reads the selected literal dotenv input and resolves where and
   how the application's main configuration will be loaded.
2. `LoadConfiguration` applies the project's schema and returns after accepting
   the first complete main configuration, or with a truthful failure.

Neither operation starts application services. An independent `config check`
and a future application runner can share these operations without calling each
other. Business-specific documents, such as SP-API credential observations, use
public configuration-source Adapters and existing preparation/settings mechanisms
directly; they do not need this application's bootstrap policy.

Adapters own original reads/observations and native recovery.
[Strict preparation](../../../adapters/configsource/v1/interface.md) owns decoding,
original-layer admission and merging.
[Settings](../../../settings/v1/interface.md) owns immutable accepted data.
This package orders application acceptance and owns its source Session.

Application declarations use this package's concrete Schema[T], Variable and
Encoding contracts. They are not aliases of Adapter preparation types. The loader
translates them at its private preparation boundary and reuses the existing strict
engine; it does not introduce another parser or merger. Generated bootstrap and
setting code therefore do not import adapters/configsource.

Provider-specific settings and borrowed Dependencies remain explicit Adapter
contracts for advanced integration. They are not copied into a second set of
Framework SDK settings, nor are they required in the generated project declaration.

## Call sequence

### Load bootstrap

Pass `BootstrapInput` with an absolute selected dotenv path, already captured
allowlisted variables and an optional explicit environment override. The caller's
context bounds acquisition. Viper dependencies are borrowed; released acquisition
evidence stays in their Inbox until the owner handles and acknowledges it.

The result is `Bootstrap`: a detached `SourcePlan` under Configuration and the
initial/candidate/shutdown budgets. It contains no clients, goroutines or other
resource handles. Ordinary formatting is restricted; deliberate field/JSON access
may reveal sensitive remote settings.

See [bootstrap selection](bootstrap.md) for the exact literal grammar, precedence,
key inventory and source constraints. Bootstrap selection is fixed for a load;
main-configuration Watch does not reload it.

### Load and consume main configuration

Pass the Bootstrap, the project's `configuration.Schema[T]`, explicit captured
application variables and borrowed `Dependencies` to `LoadConfiguration`.
The schema must have a nonzero version and a pure, bounded, non-panicking validator.

Defaults may be incomplete. The loader first detaches them structurally, acquires
the selected original documents, admits every original layer, applies the allowed
overlays, and only then invokes semantic validation on the complete candidate.
Startup-owned build/environment/instance facts are not editable configuration.

A nil error guarantees the returned `Session[T]` already has an accepted
`InitialView()`; callers do not need a separate start/wait protocol.
`Reader()` exposes later accepted data, and neither method installs a process
settings default. A caller can use `settings.As[T]` and `ValueCopy` for explicit
typed access.

A failed load can return a non-nil Session. Always retain and close it, even on
error: a timeout or cancellation can precede actual producer termination.
Pre-acquisition declaration errors return no Session and perform no source I/O.
Borrowed Runtime/Inboxes remain caller-owned.

### Observe updates and close

Watch=false captures once; Watch=true consumes the provider's initial complete
observation and then its updates. Completing LoadConfiguration does not cancel a
valid watcher. There is no duplicate native polling/refetch/recovery engine.

`Next(ctx)` consumes one bounded latest report; only one simultaneous waiter is
allowed. Publication reports carry their exact View. Errors inside Update describe
source/preparation/cleanup outcomes; the outer error describes the wait.
Coalesced reports retain Gap/Skipped, not an invented lossless event journal.
After final reporting Next preserves io.EOF.

`Status()` distinguishes source lifetime, accepted data and confirmed completion.
An accepted Reader does not prove that the configuration source is healthy.

`Close(ctx)` fences publication, requests stop and joins owned source work.
Bootstrap's shutdown budget caps each close call in addition to the supplied
context. A timed-out owner remains reachable; continue Close with a fresh context.
`ShutdownComplete()` distinguishes completed failure from unfinished work.
Old Views and accepted Reader data remain usable after source shutdown.

## One initial acceptance cutoff

Bootstrap's InitialTimeout includes main-configuration admission/default
detachment, source setup/acquisition, validation and first publication.
The Resource integration also includes the project schema-factory work in that
same absolute cutoff. It is not restarted after each phase.

CandidateTimeout bounds each complete candidate and is capped by the initial
deadline before first acceptance. The acceptance fence checks the absolute time,
not just eventual timer delivery. Producer lifetime is separate: an initialized
Watch session must survive expiry/cancellation of the temporary initial wait.

Contexts and callbacks are cooperative. They do not forcibly stop a blocked syscall
or foreign callback, and that limitation does not authorize a late publication.

## Existing Resource integration

`PrepareInput` admits and detaches the selected bootstrap input and captured
application variables into a settings snapshot, without source I/O.
`BindConfiguration` registers a Fixed configuration binding on an existing
[Resource scope](../../../resource/v1/interface.md). Binding has no unused request
context: the Scope owns its lifetime, and Apply starts the work.

The binding calls LoadBootstrap, constructs the project's schema through
`SchemaFactory`, then loads the main configuration using the same acceptance
path as LoadConfiguration. A schema factory may capture explicitly declared
process inputs once; it cannot create business resource owners.

The binding returns a Resource Ref and a separate read-only `settings.Reader`.
The latter captures a `settings.Snapshot[Loaded]` after main acceptance, before
Resource adoption or any cancelable lease acquisition. Until acceptance Capture
returns `settings.ErrUnconfigured`; failed validation creates no accepted result.
The Reader retains data through cleanup, not resource authority. An explicit
Ref.Retry may publish a later accepted construction; old Views stay unchanged.

The captured `Loaded` value is non-owning. InitialView retains the exact first
main snapshot; its Reader may see later accepted Watch updates; Info copies safe
selection metadata. The Scope keeps cleanup authority, including failed construction.
There is no second application container, cleanup registry or service locator.

The configuration Scope seals its declarations on Apply. Its Input is bootstrap
selection, not the application's Config. Later business resources therefore use
a separate scope supplied with the accepted main-configuration View; they are
not registered into an already-applied configuration scope. The public load
operations also remain usable without this optional binding composition.

`Policy` supplies finite bootstrap/early-cleanup budgets and an optional required
evidence receiver. [Bootstrap and custody details](bootstrap.md) define its limits.

## Sources, admission and compatibility

SourcePlan supports local/viper or remote/nacos with exactly one matching branch.
Documents correspond to selected local paths or fixed remote keys in order:
one to three distinct base/environment/local layers, encoded as YAML/JSON/TOML.
Paths are absolute, unique, UTF-8, NUL-free and at most 4096 bytes.
Environment is 1-64 ASCII letters/digits/dot/underscore/hyphen.

Native provider settings retain their contracts and nanosecond units. Nacos is
read-only/fixed-key; no inferred gRPC endpoint, fallback provider, native cache
substitution or implicit write probe is added. Publication is not a same-time
transaction across independently acquired files/remote keys.

Preflight checks declared Runtime/Inbox capacity, not currently free capacity:

| Profile | Active roots | Work | Evidence |
| --- | --- | --- | --- |
| Bootstrap/local finite | 1 | 8 MiB | 1 x 64 KiB |
| Local Watch | 1 | 24 MiB | 1 x 64 KiB |
| Nacos Open + finite | 2 | 17 MiB | 2 x 64 KiB |
| Nacos Open + Watch | 2 | (17 + 4q) MiB | 2 x 64 KiB |

q is the Nacos observation capacity (default 2). Declared work limits are not
process-RSS or arbitrary-callback memory guarantees.

This is an intentional pre-release source API replacement under #167:

- The former layered YAML/JSON/TOML bootstrap-plan loader and File/BootstrapInput
  shape are withdrawn. LoadBootstrap now names the literal dotenv bootstrap profile.
- The former plan-only Bootstrap data is SourcePlan. Bootstrap now carries that
  selected plan and its phase budgets together.
- The former Start/Options contract is replaced by LoadConfiguration, which
  returns after first acceptance; no compatibility alias retains the old protocol.
- Application schema/override declarations now use configuration.Schema and
  configuration.Variable, with configuration.JSON/YAML/TOML document encodings.
  Technical configsource contracts remain independently available to Adapter
  consumers; project declarations are converted internally rather than aliased.
- WaitInitial/WaitInitialView are not public workflow steps. Use InitialView on
  the returned Session and retain failed Sessions for cleanup.
- Resource integration uses PrepareInput, BindConfiguration and Policy, not
  application-start naming. BindConfiguration now returns both the Resource Ref
  and a read-only accepted-result Reader before the error result. Existing generated
  projects are not rewritten.

The package path and existing configuration failure facility/codes 1-11,
identifiers and details revision remain unchanged. Original errors.Is/As causes
remain inspectable. Retired assembly codes are not reused.

## Evidence

[Public loading tests](../../../../../framework/configuration/v1/load_configuration_test.go)
exercise accepted results, failed-session ownership, bootstrap budgets and Watch
after load completion.
[Bootstrap tests](../../../../../framework/configuration/v1/load_bootstrap_test.go)
cover selection, rejecting layers and projected-file recovery.
[Binding tests](../../../../../framework/configuration/v1/binding_test.go) cover
shared absolute deadlines, custody continuation and Resource integration.
The [independent consumer](../../../../../framework/configuration/v1/testdata/consumer/README.md)
uses both primary operations through public APIs.

These are configuration checks, not application/Worker readiness, real Kubernetes,
production Nacos TLS/auth/failover, or workflow replay qualification.
