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

# Errors, effects and independent evidence

[Documentation](../README.md) / Architecture

**Audience:** framework and integration maintainers.
**Status:** accepted architecture; implementation coverage is limited.

This topic states accepted cross-package obligations, not a claim that every
SDK or framework feature is implemented. See the [documentation map](../README.md)
for current availability and package contracts.

**Standard sections:** S06.

## Contents

- [Technical errors and framework meaning](#technical-errors-and-framework-meaning)

## Technical errors and framework meaning

Public capabilities share the [failure/v1 contract](../reference/failure/v1/interface.md),
not competing error systems per layer. Its numeric Code, readable Identifier,
component-owned data and explicit definition atlas are implemented. Shared public
operation mechanisms, configsource Adapters and Framework configuration now exist.
Other concrete service scenarios remain incremental. Internal SDK mechanisms do not
depend on those public semantics. The private
`internal/fault` foundation retains technical kinds, bounded context and original
multi-cause errors. Providers interpret native codes, responses and completion
signals without losing intentional cause inspection or inventing effect certainty.
Standard Go `error` preserves an original technical/native error as a cause.
The [public boundary fixture](../../internal/conformance/failure_v1_test.go) proves
actual parser-cause retention through numeric public errors without changing
Internal. Other mechanism fixtures retain their own local sentinels and evidence.
An already suitable error need not be wrapped merely to identify a layer. No
converter registry or automatic business policy is required. Detailed[T] is a
convenience composition with component-owned copying, not a fixed detail schema;
components may also implement Occurrence on their own types. Copying, lifecycle,
data limits and safe projection remain owner contracts, not inferred from reflection.

Keep these responsibilities separate:

[I18n](../reference/i18n/v1/interface.md) now implements explicit component resource
catalogs and presentation wrappers. Static code explanation takes its own locale;
runtime presentation reads settings or an explicit bound preference. It preserves
the original occurrence/native causes and exposes any localization issue separately.
The failure core remains locale-independent and does not consult mutable defaults.
Localization belongs at an explicitly assembled presentation/log boundary. A
settings-bound Presenter selects language per presentation; changing that preference
does not require recreating an Adapter. A replaceable Presenter/catalog may be
resource-held, but error construction must also work before or after that instance
exists. Logging the original error alone does not implicitly select a language.

| Concern | Owner and contract |
| --- | --- |
| Public semantic identity and operation location | Public failure/v1 core and component definitions; operation owners supply location/details, without SDK imports or inferred business disposition |
| Technical kind, bounded context and original causes | Private `fault`; no Run/Item/framework-attempt semantics, public error dependency or retry/disposition flags |
| Native evidence and technical effect | Provider, interpreted for the exact operation/mode; preserve confirmed, partial, and unknown scope |
| Public-operation attribution and handoff | Adapter; preserve mapping between execution ownership and the technical evidence actually available |
| Retry, toleration, reconciliation, or termination | Framework/public capability policy with declared business input; not inferred solely from a native error category |
| Human presentation and localization | Presentation resources/boundary; language must not change machine identity or effect semantics |

Data and errors can coexist. Accepted, committed, visible, failed, partial, and
unknown effects are useful distinctions, not a mandated universal enum. A request
timeout or missing acknowledgement does not establish no effect; an affected-row
count or batch acknowledgement does not establish every Item's outcome. Preserve
main-operation and cleanup failures without letting one overwrite the other.
Capability results should expose relevant facts, not a giant cross-SDK result DTO.

The required evidence path is explicit: **Provider → Adapter → framework evidence
boundary**, alongside the operation's public result. It must work for asynchronous
and partial completion, not only a final return value. The framework owns which
facts require reliable recording, acknowledgement before advancement, and the
response to recording failure, process loss, or late/duplicate evidence. A local
callback is not proof of durable recording. Internal does not independently turn
each SDK operation into a control-database write.

The independent [public operation runtime](../reference/adapters/v1/interface.md)
now reserves required evidence before admission, retains submitting stacks/guards/
descendants and gives an Inbox separate custody from caller waiting. Delivery retry
requeues facts, not SDK mutations; acknowledgement requires actual local release.
It neither imports Internal mechanisms nor supplies the durable Framework protocol.

The outer boundary freezes higher-level attribution before submission and retains
it with independently owned evidence, not only on the caller's waiting stack. The
current [boundary fixtures](../../internal/conformance/boundary_test.go) use bounded
typed envelopes to prove association across handled errors, two concurrent Items,
early wait cancellation and late callbacks, including absent business data. This
does not implement a production Adapter, a durable registry or recovery protocol.

A business handler may tolerate a failed operation without failing its Item. It
must not erase uncertain effects or other evidence required for correctness merely
by catching the error and returning success. Conversely, reporting an error must
not independently fail an Item or Run. Missing required evidence must remain
missing/unknown, not be manufactured from logs. Local public custody is implemented;
durable persistence, recovery and business disposition remain separately scoped.

Preserve intentional in-process `errors.Is`/`errors.As` behavior. At durable or
Temporal boundaries, preserve the supported semantic information through explicit
versioned contracts rather than serializing a native Go error graph. Temporal
cancellation, timeout, retry, and application-failure meanings must not be flattened
into a generic error; the detailed mapping and replay tests remain future work.

Diagnostics are bounded projections, never the only result path or terminal
ledger. Safe presentation must not emit credentials, raw configuration, private
payloads, arbitrary SQL/URLs, or unrestricted native error text. Native cause
inspection is not a promise that printing an entire error chain is safe. Audit
SDK log hooks, callbacks, returned dynamic types, and option escape hatches as well
as wrapper methods. Ordinary public capabilities must not leak uncontrolled client
ownership through these paths. Advanced native borrowing requires a separately
approved contract; this standard neither exposes all SDKs nor bans all native use.

The internal acceptance helpers check promoted public fields as well as dynamic
method sets, and probe diagnostic hooks without inferring panic from ordinary
text. A failed helper is not by itself evidence of a production ownership escape
or secret disclosure. See the [SDK acceptance guide](../reference/internal/conformance/diagnostics.md#field-and-diagnostic-probe-boundaries)
for the executable field, formatting, logging and runtime JSON boundaries and
their limits; they are testing aids, not a security sandbox or SDK certification.

Diagnostic failure must not recursively depend on the failing exporter or silently
block required cleanup. Run/Item identifiers, source names, and custom metadata
require explicit privacy and cardinality rules; being useful in a trace does not
automatically make a value safe as a metric label.

## Implementation references

[Fault interface](../reference/internal/fault/interface.md), [diagnostics](../reference/internal/fault/diagnostics.md) and [call evidence](../reference/internal/invocation/evidence.md).

This topic carries forward the [accepted integration standard](internal-sdk-integration.md)
from [Issue #3](https://github.com/frost-leo/fathomry/issues/3), including the
[Issue #15](https://github.com/frost-leo/fathomry/issues/15) boundary refinements.
