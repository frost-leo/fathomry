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

# Configuration and named-source selection

[Documentation](../README.md) / Architecture

**Audience:** framework and integration maintainers.
**Status:** accepted architecture; implementation coverage is limited.

This topic states accepted cross-package obligations, not a claim that every
SDK or framework feature is implemented. See the [documentation map](../README.md)
for current availability and package contracts.

**Standard sections:** S03, S04.

## Contents

- [Named sources and selection](#named-sources-and-selection)
- [Configuration ownership and revisions](#configuration-ownership-and-revisions)

## Named sources and selection

One Provider implementation must support multiple independently configured named
instances. A connection source is the identity of an assembled resource instance
within a documented composition scope, not a global singleton, a credential, or
the identity of a business Item. Equivalent endpoints do not authorize automatic
pool, session, account, or lifecycle sharing. Validate duplicate identities and
invalid selected configurations before constructing resources. Reuse an instance
for its declared lifetime rather than implicitly rebuilding it for each operation;
that lifetime is not necessarily the entire process.

| Identity | Responsible boundary and distinction |
| --- | --- |
| Provider identity | Identifies the selected technical implementation; neither a source instance nor its SDK version |
| Source identity | Assigned and bound by composition; identifies the actual resource used, with unambiguous scope |
| Effective source-configuration revision | Identifies the settings applied to that instance; distinct from configuration format and Run input |
| External account / credential scope | Domain and credential owners define authorization and account-bearing state; refreshed tokens do not change the account identity |
| Quota scope | Describes the actual shared allowance, potentially spanning sources, accounts, applications, and workers; not necessarily the ownership scope |
| Technical correlation | Per-call Call/Parent and optional opaque Owner; no framework Run/Item/attempt meaning in shared internal mechanisms |
| Execution attribution | Frozen and retained by the outer operation/evidence boundary: relevant Run/Item/framework attempt and effect ownership; never a mutable “current Run” field on a shared client |

Composition determines the allowed source set. A developer can receive a bound
public capability or select from permitted, already-assembled sources through
the outer capability boundary, without acquiring client ownership. Unknown,
unavailable, or disallowed selection must be explicit; it must not silently choose
another source. Validate required capability, mode, and account/session scope and
retain the actual selected identity. User metadata cannot overwrite these facts.
Background tasks without a business Run must not manufacture one for correlation.

Including a Provider in a build, constructing instances at startup, and choosing
an available source for a call are different decisions. Whether configuration can
choose among compiled construction implementations remains open; permitted runtime
source selection is not thereby prohibited. No dynamic plugin loader is implied.

Framework control persistence and business database access are separate uses of
a technical database capability. Control Adapters own control-schema operations;
business I/O capabilities permit the business operations granted to their sources.
Reusing PostgreSQL support does not expose arbitrary control-database access or
imply a shared pool. Explicit sharing must document permissions, contention,
transaction boundaries, and shutdown ownership. Neither different source names
nor Go interfaces prove service/process isolation. A request spanning sources
must not acquire an implied cross-source transaction guarantee.

## Configuration ownership and revisions

Framework-supplied composition owns configuration input authorization, precedence,
recursive overlays, secret resolution, and provenance. Base configuration,
environment-specific YAML, local overrides, and environment variables must have a documented precedence;
nested overrides retain unrelated sibling fields. A future Viper or configuration-
center integration such as Nacos does not move loading/SDK management to business
code. Loaders must preserve authorized layer distinctions and use the established
preparation/merge once, not create a second precedence or null policy. Providers
receive explicit, resolved typed settings. They must not
read process environment or global application settings during construction or calls.

The relevant capability/Provider owns its field meanings, units, bounds, default
values, option interactions, and supported modes. Shared configuration mechanisms
own consistent preparation and reporting, not one schema that erases SDK differences.
Composition validates the requested combination and required dependencies before
exposing a usable instance. Parsing settings, constructing a client, checking
readiness, and creating remote resources are separate operations and permissions.

SDK parsers may themselves consult environment, files, defaults, or callbacks.
The Provider must account for these inputs at the authorized preparation boundary
or reject that mode; passing a typed struct alone does not prove deterministic
configuration. Do not temporarily rewrite process-wide environment to simulate
per-instance configuration. Unused optional capabilities must not require services
or credentials solely because another Provider exists in the repository.

The instance must retain an accurate effective configuration and safe provenance,
not an aliased caller-owned map or an unchanged label over mutable settings.
Specify copying, borrowing, mutation, and concurrent-use rules for mutable values
and callbacks. Do not promise generic deep copying of arbitrary SDK objects.

Configuration schema evolution is owned by the format maintainer. A particular
source's revision is owned by composition and changes according to its declared
effective-setting semantics. Defaults and behavior-affecting options participate
in compatibility decisions. An unsupported configuration format must be rejected
before resource construction, not decoded using another version's defaults.
Dynamic credentials have a separately owned refresh
lifecycle: preserve account identity and secret references, not tokens in frozen
Run input, public diagnostics, or configuration-revision material.

The [named-source contract](../reference/internal/resource/interface.md) establishes the implemented
list/null/empty/duplicate/unknown-key rules, revision identity and freezing behavior.
File/environment-key loading and reload/rotation handoff are not implemented and
need their own explicit contracts before support is claimed. A replacement or reload
must not retroactively relabel settings
already used by in-flight work or weaken its execution constraints.

## Implementation references

[Resource interface](../reference/internal/resource/interface.md) and [configuration contract](../reference/internal/resource/configuration.md).

This topic carries forward the [accepted integration standard](internal-sdk-integration.md)
from [Issue #3](https://github.com/frost-leo/fathomry/issues/3), including the
[Issue #15](https://github.com/frost-leo/fathomry/issues/15) boundary refinements.
