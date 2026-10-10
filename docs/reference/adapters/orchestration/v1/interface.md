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

# Orchestration contracts

[Documentation](../../../../README.md) / Public Adapter reference

**Audience:** orchestration consumers and concrete Adapter authors.
**Status:** implemented provider-independent accounting and attribution data.
**Package:** `github.com/frost-leo/fathomry/adapters/orchestration/v1`.

## Category and provider responsibilities

This category owns `Budget`, `Policy` and `Attribution`. It imports only public
operation contracts and the standard library. It creates no Client, Worker,
source, operation runtime or generic workflow/business model. Native options,
validation, policy derivation and lifecycle remain in
[the Temporal provider](../temporal/v1/interface.md).

`Budget.WorkBytes` and `WorkerBytes` describe different admitted root families;
`EvidenceBytes` covers independently retained evidence. `Policy` keeps source,
native and public envelopes distinct and separates operation, Worker and task
receivers. Values are declared byte/count allowances, not measured RSS, a remote
quota or proof of service capacity. Concurrent owners/generations require explicit
composition. Data alone neither reserves capacity nor grants native authority.

The provider validates its frozen selection and returns a Policy; callers create
their Runtime/receivers from that data. Zero or arbitrary caller-constructed values
do not certify a valid provider policy. Private native limits remain provider-owned
and are not inferred from the larger public bridge allowance.

## Attribution and compatibility

`SourceID` distinguishes process-side source constructions, even with equal names;
it is not a physical socket or remote-cluster identity. `UseID` distinguishes
retained uses; zero denotes the source owner. `Generation` is the actually
selected resource generation, not its later replacement. Attribution does not
prove a remote effect, execution completion or physical release.

Attribution is deliberately inspectable, redacted by ordinary fmt/slog and refuses
implicit JSON serialization/reconstruction. Typed-nil slog values are safe.
Budgets are ordinary numeric configuration data with explicit byte fields.

The existing `temporal.Budget` and `temporal.Policy` names alias these canonical
category types; exported field literals remain valid. Their reflected Go package
identity now belongs to the category, not to Temporal. This is not a promise about
unversioned gob/interface type-name persistence.

`temporal.Attribution` remains a provider-defined type over the same fields,
preserving Temporal's diagnostic marker and serialization error code. Explicit
conversion to/from the category value is supported; the category's generic
serialization refusal does not allocate a provider error facility.

## Executable boundaries

[Policy tests](../../../../../adapters/orchestration/v1/policy_test.go),
[metadata tests](../../../../../adapters/orchestration/v1/metadata_test.go) and
[the independent consumer](../../../../../adapters/orchestration/v1/testdata/consumer/main.go)
cover data-only use without a Temporal SDK or provider dependency.
[Temporal category controls](../../../../../adapters/orchestration/temporal/v1/category_contract_test.go)
verify actual shared-policy consumption, exact native limits, parent/use identity
and unchanged provider diagnostics. These are local controls, not a service result.

The [Adapter tree map](../../../../../adapters/README.md) owns role/file conventions.
