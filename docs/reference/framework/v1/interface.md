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

# Framework-layer catalogs

[Documentation](../../../README.md) / Public package reference

**Audience:** Framework feature authors and independent projects.
**Status:** implemented explicit static registration; no application startup.
**Package:** `github.com/frost-leo/fathomry/framework/v1` (Go name `framework`).

`Catalogs(frameworkModules, adapterModules...)` prepares the two layers'
selected definitions/resources/bindings using the existing
[Adapter assembly](../../adapters/v1/interface.md) and failure/i18n validators.
Framework's `Module` is a plain static contribution, not a service registration.
It includes Framework admission conditions once; the underlying assembly includes
Adapter admission and i18n definitions once. Explicit duplicates always reject.

Features retain complete owner-qualified identities, such as
`fathomry.framework.configuration.invalid_schema`. Framework modules are separate
from configured source instances, business identity, SDK/API versions and source
revisions. Fact selection belongs to the same occurrence as its condition.

`CatalogSet.Errors`, `.Messages` and `.Bindings` expose the existing immutable
query capabilities. Declaration borrowing, quotas, freshness, English fallback and
owned query copies follow the Adapter/foundation contracts. Framework remaps
Adapter assembly admission refusals to its own catalog conditions, retaining the
approved static cause; suitable i18n occurrences remain unchanged.

No concrete Adapter, SDK, configuration source, workflow or CLI is imported.
Inventory is available before configuration exists. A configuration convenience
entry adds its own feature modules, but does not own this layer-wide facility.
Later Framework features can use the same registration entry without depending
on configuration loading.

See [configuration](../configuration/v1/interface.md) for the first consumer and
[executable layer-tree controls](../../../../framework/configuration/v1/catalog_test.go).
This is neither a global service locator nor a promise that an entire application
can be started through one load function.
