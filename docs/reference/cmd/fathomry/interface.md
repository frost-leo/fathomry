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

# Fathomry command line foundation

[Documentation](../../../README.md) / Command-line reference

**Audience:** users and maintainers of the official executable.
**Status:** minimal CLI foundation; all former leaf commands and project inspection
protocols are withdrawn under [#161](https://github.com/frost-leo/fathomry/issues/161).
**Source:** [cmd/fathomry](../../../../cmd/fathomry/main.go).

## Available behavior

The executable supports root help, `help`, `--help`, `--lang` and
`--output text|json`. With no arguments it shows root help. It does not load
project configuration, start services, create files or install database assets.
The entry captures `FATHOMRY_LANG`; an explicit `--lang` overrides it.

```sh
go run ./cmd/fathomry --help
go run ./cmd/fathomry --lang zh-CN --output json help
```

The old `new`, `build`, workflow, schema, database, error-catalog and
translation-query commands reject with a usage error. No replacement commands
are registered. The former `--fathomry-project=v1|v2` protocol also rejects.

## Invocation and cleanup

Explicit streams, bounded argument/output admission, localized safe diagnostics,
public failure identity and cancellation remain in the private command engine.
Command construction acquires no resources. Its lazy Resource scope retains
cleanup authority, including continuation after incomplete cleanup. Technical
errors and uncertain mutation facts are not erased by output or cleanup failures.

Exit codes are 0 for success, 2 for invalid usage/admission, 130 for cancellation,
and 1 for execution/output/cleanup failure. The executable owns these codes;
`go run` may wrap a child process's exit status. A closed stdout is reported
through the output-error policy rather than terminating on SIGPIPE.

## Go entry and evidence

The [public entry contract](v1/interface.md) describes `Run` and its explicit
dependencies. It is not a plugin or application-host SDK.
[Process tests](../../../../cmd/fathomry/main_test.go) cover executable behavior,
signals, cleanup and closed stdout. The
[private mechanism tests](../../../../cmd/fathomry/internal/command/run_test.go)
and [mutation tests](../../../../cmd/fathomry/internal/command/mutation_test.go)
retain invocation/output and effect/cleanup assertions.
