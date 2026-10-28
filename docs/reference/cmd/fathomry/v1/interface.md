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

# Public CLI foundation

[Documentation](../../../../README.md) / Public package reference

**Audience:** callers and maintainers of the minimal command entry.
**Status:** implemented invocation foundation; project and business command APIs withdrawn.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/v1` (package `cli`).

## Contract

`Run(ctx, args, options)` handles only the minimal
[official CLI](../interface.md). Supply non-nil input, output and error-output
streams and a caller-owned context. Callers synchronize shared writers.
An empty language selects English; a zero cleanup timeout selects five seconds.

`Components` supplies offline command metadata. Nil error/message catalogs
select core and command catalogs together. A custom pair must include the
required metadata; supplying only one catalog rejects. No provider catalog,
configuration loader, process default or service client is created.

`ExitCode` exposes the executable's process policy.
`ExecutionError` retains mutation-report custody and resource-cleanup continuation
from the generic engine; it is not a durable outcome or database API.

## Withdrawn surface

`Project`, `Workflow`, `Handle`, database configuration/resolver types,
generated project registration and build inspection are removed. This is an
explicit pre-release breaking withdrawal, not a compatibility shim. Old commands
and inspection arguments fail closed. Rebuilding them requires separate work.

## Verification

[Public-entry tests](../../../../../cmd/fathomry/v1/cli_test.go) cover help,
machine output, removed-command rejection, explicit streams and cancellation.
[Concurrent entry tests](../../../../../cmd/fathomry/internal/app/app_test.go)
exercise independently owned invocations. No runtime or service qualification is
provided by this CLI foundation.
