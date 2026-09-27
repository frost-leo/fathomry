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

# Adapter-layer catalogs

[Documentation](../../../README.md) / Public package reference

**Audience:** Adapter authors and independent project composition.
**Status:** implemented static registration and offline inspection; no runtime registry.
**Package:** `github.com/frost-leo/fathomry/adapters/v1` (Go name `adapters`).

## Declare implementations once

A `Module` contributes existing failure definitions, original named i18n resource
bytes, module grouping and checked bindings. It has no factories, contexts, native
clients, source settings or selected locale. Each feature owns its vocabulary.
For example, configuration belongs to `fathomry.adapters.configsource`; the
concrete local implementation belongs to its `.viper` child. A complete condition
is `fathomry.adapters.configsource.viper.read`, not a runtime instance label.

Call `Catalogs(modules...)` to prepare one immutable `CatalogSet`. It includes
the Adapter layer's own admission conditions and shared i18n definitions.
Explicit duplicate declarations reject even when equal, including explicitly
repeating a built-in. Multiple source instances reference one selected module;
they never contribute repeated vocabularies. Registration is local to this
returned set, not global, ambient, automatic or performed in `init`.

The returned `Errors`, `Messages` and `Bindings` are the existing
[failure](../../failure/v1/interface.md) and [i18n](../../i18n/v1/interface.md)
catalogs. They retain complete inspection, exact identity, English completeness,
freshness and same-occurrence binding rules. Module IDs are not namespace authority.
Changing a resource language does not change condition identity.

## Calling and ownership

Declarations are borrowed only while `Catalogs` runs; do not mutate them
concurrently. Feature `Definition`, `Sources`, `Bindings` and `Module`
functions return independent declarations. Prepared handles share immutable
storage, while query results own mutable slices/bytes. There is no locale in a set.

Assembly checks outer counts and aggregate message bytes, including built-ins,
before flattening bounded slices. Nested declaration validation, including cyclic
input, remains with failure/i18n. Immediate parent/child contributions are assembled
before those validators run; absent intermediate namespaces are not invented.
All existing foundation quotas still apply.

`Catalogs` performs no configuration, filesystem, environment, client or service
startup. This package imports no concrete Adapter or Framework.
Framework's static entry reuses the bounded assembly with its own declarations;
this does not make Adapter operations depend on Framework.

Errors are directly usable without catalogs. Bare definition-admission sentinels
are deliberately mapped to `ErrCatalog`/`ErrLimit` occurrences; existing i18n
occurrences remain intact. No failed localization path retries an operation.
Use a bounded condition-only fallback when presentation cannot be prepared.

## Evidence and limits

[Registration controls](../../../../adapters/v1/catalog_test.go) cover invalid roots,
duplicate/bounded declarations, cycles and detached resources.
[Configuration catalog tests](../../../../framework/configuration/v1/catalog_test.go)
exercise the complete layer topology and English/Chinese bindings.
These are static inventories, not source readiness, an SDK plugin interface,
a durable registry or a historical translation-retention service.
