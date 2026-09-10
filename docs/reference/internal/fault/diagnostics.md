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

# Technical error diagnostics

[Documentation](../../../README.md) / Internal package reference

**Audience:** internal and future boundary authors.
**Status:** implemented occurrence guards, not universal redaction or a wire format.
**Package:** `github.com/frost-leo/fathomry/internal/fault`.

Start with the [fault interface](interface.md). Safe ordinary presentation and
intentional native-cause inspection are separate responsibilities.

## Ordinary presentation

Error/Format expose the technical kind, not native messages or context. Pointer
slog presentation is restricted as well. These paths do not traverse native
Error/Format/LogValue hooks. [Tests](../../../../internal/fault/errors_test.go) use
native errors whose presentation panics to detect accidental traversal.

Diagnostic is an explicit value projection. Its labels must already be safe;
a bare caller-constructed Kind/Context is not a validated occurrence or redaction API.

## Deliberate inspection

Unwrap and errors.Is/As intentionally expose original causes. Printing those causes
independently is not sanitized. Never turn SDK text into Kind, secret-bearing labels,
unbounded metric labels or durable payloads. A future public capability must
explicitly own any native-cause inspection it exposes.

## JSON and future protocols

Non-nil Error occurrences, including copies, refuse ordinary JSON encoding and
reconstruction. Nil pointers retain ordinary Go absence/null behavior, not
reconstruction or mutation of an existing occurrence. Kind/Context and other
metadata are not all subject to the Error method set.

Standard errors.Join/fmt.Errorf wrappers do not inherit JSON refusal. Runtime
Go error graphs are not approved database, message or Temporal DTOs. Future
formats need an owner, schema/version, bounds, absent/unknown meanings and
historical-reader/migration or rejection behavior.

The default Temporal converter does not preserve arbitrary multi-cause Go trees.
The [pinned converter source](https://github.com/temporalio/sdk-go/blob/v1.46.0/internal/failure_converter.go#L65-L209)
is reference evidence, not a dependency. No Temporal bridge, durable ledger,
localization or Item/Run terminal policy is implemented here.
