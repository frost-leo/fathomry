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

# Version presentation interface

[Documentation](../../../README.md) / [Version interface](../interface.md)

**Audience:** applications presenting version facts or version failure codes.
**Status:** implemented optional resource-first English/Simplified Chinese messages.
**Package:** `github.com/frost-leo/fathomry/version/presentation`.

## Responsibilities and call sequence

Compose `Resources()` with any business-owned resources, then call
`i18n.Prepare` explicitly. Pass the resulting immutable catalog, an explicit
locale and a `version.Build` to `Summary`. Pass an exact supported
`failure.Code` to `Error` for a localized version failure message.

No catalog construction, file I/O, locale environment discovery or global
registration occurs during rendering. Resources are embedded from
[external JSON files](../../../../version/presentation/locales), not Go text
literals. Every Resources call returns independent bytes. Mutating those bytes
cannot modify another call or a prepared catalog.

## Meaning, ownership and failure boundaries

Summary deliberately reports the application's declaration, Go toolchain and
target, not a dependency inventory. It distinguishes an absent declaration from
declared clean/modified main source. Unknown target/toolchain scalars use the
language-neutral machine marker `?`; development toolchains use `(devel)`.
It does not assert verified provenance or an official release.

The presenter projects only exact permitted scalar strings from immutable Build
state. It accepts no arbitrary formatter/serializer objects or generic argument
map. Error accepts only this version package's exact codes; foreign codes return
`i18n.MessageNotFound` without inspecting or formatting arbitrary error graphs.

Each call renders one complete message through the existing catalog, preserving
`i18n.Result`'s requested, matched and actual resource locale, fallback reason and
snapshot identity. An unsupported locale such as German falls back to English.
There is no concatenation of independently localized fragments with misleading
single-language metadata.

Catalog failures are returned as presentation errors; they do not erase successful
metadata, rewrite the original failure occurrence or change version identity.
The caller keeps the original machine result and may display its stable code.
Output is plain text; callers own terminal/HTML/channel escaping.

Prepared catalogs and Build handles support concurrent readers. Request data is
not retained by the presenter and no background work or close obligation exists.
See the [i18n contract](../../i18n/interface.md) for bounds, locale matching, trusted
resource authoring and source freshness.

## Compatibility and evidence

Message IDs occupy `fathomry.version.*`; arbitrary resource order cannot override
a collision. English source contracts and matching Chinese source fingerprints
are maintained together. Message parameters, source revisions, packaged resource
snapshots and software versions are separate compatibility axes. Custom resources
must obey the established source contract or deliberately select a new compatible
composition; no dynamic remote translation loader is supplied.

[Presentation tests](../../../../version/presentation/presentation_test.go)
cover exact external-resource goldens, current translation fingerprints, English/
Chinese/fallback results, render failures, original machine/failure identity,
concurrent readers, resource copying, formatter rejection and license/text
placement. The independent SDK consumer also renders these packaged resources.
