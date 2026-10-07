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

# HTTP Adapter vocabulary

[Documentation](../../../../README.md) / Public package reference

**Audience:** independent applications and concrete HTTP Adapter authors.
**Status:** implemented accounting and attribution vocabulary; no shared HTTP runtime.
**Package:** `github.com/frost-leo/fathomry/adapters/httpclient/v1`.

## Responsibilities

`Budget` describes root-family work and independent result reservations.
`Policy` separates source-resident work/evidence from aggregate Runtime/Inbox
recommendations. Concrete providers calculate these from their exact prepared
Internal selections; this package owns no SDK defaults or arithmetic controller.

`Info` identifies actual source preparation and non-secret layer provenance.
Its revision is an opaque preparation identity, not a settings hash or public
resource generation. `Clone` detaches nested slices. `Attribution` freezes public
operation identity and the actual borrowed generation. A zero generation also
covers direct use and failed acquisition; it does not establish absence.

`Attempts` counts observations, not necessarily physical wire sends. In
particular, zero with `Exact=false` does not prove no remote effect.

## Boundaries and copying

Only `adapters/v1` is a foundational dependency. There is no provider registry,
source owner, HTTP client interface, request DTO, stream signature, context
merger, shared Complete meaning or shared error facility. Those decisions stay
with each concrete provider. Accounting is declared local capacity, not hard RSS,
remote concurrency, a distributed quota or production qualification.

Runtime attribution and preparation metadata refuse ordinary serialization and
redact diagnostics. Deliberate field inspection is caller-controlled. Policies
and budget fields are data, not live ownership.

## Executable evidence

The [first provider](../nethttp/v1/interface.md) demonstrates direct and
Framework Fixed/Follow use. Shared dependency/type-alias checks, detached-copy
tests and a separate-module consumer are maintained in
[`adapters/httpclient/v1`](../../../../../adapters/httpclient/v1).
