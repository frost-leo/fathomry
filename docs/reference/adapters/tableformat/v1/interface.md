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

# Table-format category interface

[Documentation](../../../../README.md) / Public Adapter reference

**Audience:** table-format Adapter consumers and implementers.
**Status:** implemented SDK-independent vocabulary, not a Catalog engine.
**Package:** `github.com/frost-leo/fathomry/adapters/tableformat/v1`.

## Responsibilities

`Budget` separates source lifetime, operation-family work and independent evidence.
`Policy` carries a concrete provider's recommendation without constructing or
resizing shared mechanisms. `Info.Clone` detaches preparation provenance;
`Attribution` distinguishes physical source/use identity from Fixed/Follow generation.

`Phases` are independently observed facts, not a mandatory sequence. Upload,
Catalog submission, acknowledgement and later reload are different effects.
`Effect` makes no durability, retry or rollback promise.

## Caller obligations

Choose a concrete provider. Sum overlapping source/generation and family limits
before creating shared runtimes; retired generations remain charged until actual
release. Declared logical bytes are not a process RSS limit. Deliberate fields can
be sensitive; runtime projections refuse implicit serialization and redact logs.

The [Iceberg contract](../iceberg/v1/interface.md) owns native selection,
authority, callbacks, streaming and cleanup. Category tests include a standalone
consumer and alias/privacy controls; they are not service qualification.
