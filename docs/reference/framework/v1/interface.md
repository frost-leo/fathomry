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

# Framework metadata preparation

[Documentation](../../../README.md) / Public package reference

**Audience:** independent projects and explicit composition entry points.
**Status:** implemented offline metadata composition; no application host.
**Package:** `github.com/frost-leo/fathomry/framework/v1`.

## Responsibility

`Prepare` combines explicitly supplied existing `i18n.Component` metadata into
a caller-owned `Catalogs` pair. Both Errors and Messages are returned on success;
any failure returns a zero pair and the original error. The underlying failure
and i18n engines remain the validation, allocation and localization authorities.

There is no component registry, implicit core/provider discovery, process default,
locale selection, service client, resource Scope or Runtime. No retired assembly
error numbers are reassigned. Existing CLI and generated projects are unchanged.

## Calling contract

Supply the complete chosen component set, optionally including
`i18n.CoreComponents()` and `configuration.Component()`. Input metadata and
resource filesystems are borrowed for the call without concurrent mutation.
Existing i18n component, document, definition and byte bounds apply before the
combined definition slice is constructed.

Catalog preparation reads supplied resource filesystems; it does not discover
files. A hostile filesystem or caller callback is not forcibly interrupted.
Returned catalogs retain their existing immutable contracts. Editing the pair
does not register it or mutate either catalog.

## Evidence

[Package overview](../../../../framework/v1/doc.go),
[implementation](../../../../framework/v1/catalog.go) and
[tests](../../../../framework/v1/catalog_test.go) cover paired success/failure,
duplicate metadata and existing limit/error identities.

This is the thin shared boundary selected in [#165](https://github.com/frost-leo/fathomry/issues/165),
not restoration of the withdrawn Framework runtime.
