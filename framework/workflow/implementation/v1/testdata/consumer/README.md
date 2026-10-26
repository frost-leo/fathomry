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

# Independent Activity binding consumer

[Binding contract](../../../../../../docs/reference/framework/workflow/implementation/v1/interface.md)

**Audience:** independent Go workflow authors.
**Status:** executable offline example with three pure functions and a checked
single-Activity invocation. It does not run
a Temporal Worker or sample real host telemetry.

## Files and identity

This fixture is copied into an independently generated module by
`TestIndependentProject`. Its imports intentionally use
`example.org/binding-consumer`, not the framework module.

```text
main.go
workflows/example/server_health/v1/
  workflow.go
  workflow_test.go
  nodes/sample/
    contracts.go
    cpu.go
    memory.go
  nodes/summary/
    summarize.go
```

The directories express Namespace / workflow / business version / node. Each
Activity has one file, with its explicit binding helper beside the function.
Shared port structs are local to their node. `workflow.go` gathers the helpers;
`main.go` supplies that registration to the single public CLI entry. Directory
names do not auto-register anything. The sample reuses the existing health
declaration as data instead of generating a second graph definition.

## Run from a Fathomry source checkout

These commands need the repository toolchain and network/cache access for module
preparation. All generated files and disposable binaries go into a unique job:

```sh
root="$PWD"
job="$(mktemp -d /home/frost/tmp/codex-build/jobs/build.XXXXXX)"
go build -o "$job/fathomry" ./cmd/fathomry
"$job/fathomry" new "$job/consumer" \
  --name binding-consumer --module example.org/binding-consumer \
  --framework-source "$root"
cp -R framework/workflow/implementation/v1/testdata/consumer/. "$job/consumer/"
cd "$job/consumer"
GOWORK=off go mod tidy
GOWORK=off go test -race ./workflows/...
GOWORK=off go run . workflow vet
"$job/fathomry" build .
./bin/binding-consumer --fathomry-project=v2
./bin/binding-consumer
```

Vet and inspection report one complete version, three unique bindings and three
call sites without calling them. Ordinary execution uses the prepared Registry's
`Invoke` boundary to check input, call the pure CPU fixture once and validate/encode
`{"value":42}`. This is a synchronous local call, not a Temporal Worker or DAG
executor. The independent test also verifies missing input refuses before dispatch. Build
creates `bin/binding-consumer` only after full inspection. Generated configuration
files are unchanged and no Boot or control database is needed for these commands.

The maintained integration gate also checks ABI refusals, dependency isolation,
changed-artifact/unchanged-manifest behavior and old-output preservation:

```sh
go test -race ./framework/workflow/implementation/v1 \
  -run '^TestIndependentProject$' -count=1
```
