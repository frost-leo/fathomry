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

# Framework configuration acceptance

[Documentation](../../../../README.md) / Public package reference

**Audience:** project authors loading application settings or independent business
variables through public Sources.
**Status:** implemented finite loading, owned Watch, atomic typed publication and
explicit resource-adoption handoff. CLI generation and full application runtime
remain outside this contract.
**Package:** `github.com/frost-leo/fathomry/framework/configuration/v1`.

## Declare data and already-resolved bootstrap

Declaration[T] contains the project-owned Schema[T], a selected public
[configsource.Source](../../../adapters/configsource/v1/interface.md), external
layer slots and explicit environment bindings. It imposes no application-name,
locale or framework envelope on T. Empty structs and unrelated complete models
are supported by the independent preparation profile.

A source's endpoints, namespace and credentials must be resolved before reading
the document. That document cannot supply credentials needed to fetch itself.
The same provider Settings can separately configure a business source. Source
selection does not aggregate SDK dependencies: Framework imports public contracts,
never Viper/Nacos or root Internal.

Dependencies borrows an explicit public operation Runtime, required Inbox[Evidence]
and optional resource Scope for adoption. [Framework common composition](../../v1/interface.md)
can supply the runtime/scope and evidence receivers; it is not a service registry.

## Layer and variable policy

External slots are at most three distinct Base, Environment and Local declarations
in source-document order, each with JSON/YAML encoding and optionality. Acquisition
must produce exactly that many documents. Optional means positive missing only:
present-empty, malformed, unknown fields, wrong types and failed reads still reject.
Partial acquisition never publishes a usable prefix.

[Strict preparation](../../../adapters/configsource/v1/interface.md) owns the single
merge/type/null/list policy. Framework does not reconstruct it from native maps.
Precedence is defaults < base < environment < local < captured variables. A malformed
lower layer cannot be hidden by a later override. The final project validator gets
an isolated candidate and cannot modify the accepted value by mutating its argument.

Up to 64 explicit Environment bindings name a process variable and a struct-field
path. Names are bounded UTF-8 without '=' or NUL. Each name is captured once at
admission; Watch never rereads the process environment. Keep the environment stable
during admission when a common-time epoch is required.

- JSON=false preserves literal text for string/pointer-to-string fields, including
  empty strings and literal "null".
- JSON=true parses strict JSON numeric/bool/null/object/list values with target-type
  validation and exact integer precision.
- Unset contributes no override; empty is a present value.
- Duplicate/ancestor-overlapping paths reject, even when unset.
- Whole-map bindings overlay recursively; individual map keys/list indices are
  not addressable bindings. Values and encoded variables are bounded to one MiB.

Defaults/declaration slices and variable values freeze at admission. Source and
validator callbacks are retained under their bounded/concurrent-use contracts.
No dotenv discovery, process mutation, implicit settings default or secret-store
service is introduced.

## Finite loading and live publication

Load returns State[T] only after complete acquisition/preparation/publication.
Bootstrap/read/schema failure returns no usable State. Defaults/environment-only
Load is valid without a Source; unused source/slot combinations reject.

Watch requires a Source and returns an asynchronous owned Watcher[T]. Construction
is not readiness: Reader.Capture initially returns settings.ErrUnconfigured.
Wait for an accepted Event or inspect Capture before explicitly calling
settings.Configure. Watch does not replace the Reader from a separate earlier Load.

Both paths preserve T as the settings root. Reader provides ordinary public data
consumption. Capture returns one Accepted[T] whose ValueCopy, Description, View and
Sequence belong to the same acceptance. Separate Reader and Status calls do not
form a transaction. Revisions are opaque preparation identities, not sortable
source cursors. Every successful acceptance, including equal data, gets a fresh
revision; a resource binding's Equal controls whether it reconstructs.

Watch owns one ingress and one serial validation path. Ingress keeps the latest
pending observation, advances a local epoch and cooperatively cancels obsolete
validation. Final publication checks that epoch and the closing fence under the
publication lock. A slow obsolete validator cannot replace newer observations;
there is no validator goroutine per update or additional native poller.

Invalid or unavailable observations keep last-good data. Successful later input
can recover. Related fields publish together, never separately. Status separates
observations, acceptance/rejection/supersession, latest source/preparation errors
and adoption. Events are bounded (zero capacity selects 16; valid 1..64), with
sticky Gap on coalescing or dropped notifications. They are not durable history.

## Adoption is not publication

Optional Dependencies.Resources explicitly hands the accepted View to the existing
public resource Scope. State.Status.Adoption is its receipt; use Wait and inspect
the corresponding resource Ref when actual adoption matters.
AdoptionError/AdoptionFailed report immediate Apply refusal,
not all asynchronous constructor results. Accepting data never means every instance
is ready. A failed constructor can preserve an older usable instance while readers
already see the newly accepted settings.

Fixed and Follow remain per-resource policies. Existing leases and subscriptions
retain their generation. This does not migrate arbitrary SDK sessions, databases,
accounts or frozen Workflow inputs.

## Two independent data domains

Application settings and a related business credential bundle use separate
Declaration/State/Watcher instances and Readers. Business updates do not republish
application settings or reconfigure unrelated resources. No cross-domain atomicity
or remote credential validity is inferred. Only the application Reader should be
explicitly installed as the process default, if that convenience is selected.

Configure sufficient common-runtime budgets for the selected lifetime owners.
For example, the tested simultaneous file-backed application/business Watch
composition uses 128 MiB of declared work allowance. Declared envelopes are not
measured process RSS or a cross-process quota.

## Shutdown, evidence and privacy

Close fences publication before cancellation and joins ingress, validation and the
source owner. A blocked validator/source/cleanup remains owned after timeout;
retry Close with a fresh budget. Last-good captures remain readable after shutdown.
Next cancellation only stops that wait. No late validation may publish after Close.

Finite and Watch operations reserve required public evidence before native
dispatch. Lifecycle facts resolve only after actual cleanup. Receive them with
the common released-record path so live owners cannot block finite evidence.
Source, preparation and adoption failures remain distinct from presentation.
Definitions/Resources compose through Components without importing providers.

Runtime handles reject serialization/reconstruction and default formatting hides
payloads. Explicit ValueCopy/Reader data, environment values and original causes
are sensitive. No process-local watcher/goroutine code belongs in Workflow execution.

## Executable contracts

- [Real layered local Load, strict variables, extensions and initial refusal](../../../../../framework/configuration/v1/load_test.go).
- [Obsolete validation, last-good recovery, cleanup, overflow and concurrent readers](../../../../../framework/configuration/v1/watch_test.go).
- [Independent application/business domains and Fixed/Follow adoption](../../../../../framework/configuration/v1/integration_test.go).
- [Public Nacos protocol-fixture consumption](../../../../../adapters/configsource/nacos/v1/integration_test.go).
- [Opt-in real Nacos -> Framework -> settings -> PostgreSQL resource path](../../../internal/conformance/fixtures.md#live-framework-configuration-path), including temporary-table CRUD and watched pool replacement through a test-only Internal database bridge.
- [Independent-module application default and localized-log consumer](../../../../../framework/configuration/v1/testdata/consumer/consumer_test.go).

These checks do not certify production deployments, Nacos HA, authentication
refresh, control-store migration, CLI generation or a complete Worker runtime.
