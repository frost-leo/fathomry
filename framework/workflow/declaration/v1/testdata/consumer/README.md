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

# Independent health declaration example

[Workflow interface](../../../../../../docs/reference/framework/workflow/declaration/v1/interface.md)

**Audience:** developers adding declarations to an existing independent Go project.
**Status:** executed by `TestIndependentConsumer`; no real health sampling or Workflow runs.

The [health declaration](../health/declaration.go) is the single Go authoring source.
[main.go](main.go) is a small project-owned exporter, not a private CLI import or
generated Boot extension. [main_test.go](main_test.go) checks its public API and
failed/short output behavior.

## Run from a development checkout

The example uses explicit local source selection because no published release is
implied. This recipe writes only disposable project/build files. Run from the
Fathomry checkout in a POSIX shell with its Go 1.27+ toolchain:

```sh
framework=$PWD &&
go_tool="$(go env GOROOT)/bin/go" &&
job=$(mktemp -d /home/frost/tmp/codex-build/jobs/build.XXXXXX) &&
mkdir -p "$job/project/health" &&
cp framework/workflow/declaration/v1/testdata/health/declaration.go "$job/project/health/" &&
cp framework/workflow/declaration/v1/testdata/consumer/main*.go "$job/project/" &&
"$go_tool" build -mod=readonly -o "$job/fathomry" ./cmd/fathomry &&
cd "$job/project" &&
"$go_tool" mod init example.org/workflow-consumer &&
"$go_tool" mod edit -go=1.27.0 -require=github.com/frost-leo/fathomry@v0.0.0 &&
"$go_tool" mod edit -replace="github.com/frost-leo/fathomry=$framework" &&
GOWORK=off "$go_tool" mod tidy &&
GOWORK=off "$go_tool" test -mod=readonly -race -count=1 ./... &&
GOWORK=off "$go_tool" build -mod=readonly -o "$job/exporter" . &&
"$job/exporter" > "$job/bundle.tmp" &&
mv "$job/bundle.tmp" "$job/health.bundle.json" &&
"$job/fathomry" workflow vet "$job/health.bundle.json" --output json
```

Module preparation may acquire pinned dependencies. The exporter and checker need
no configuration, credentials, database or Temporal Server. An installed CLI can
check the exported file without the project source or Go toolchain.

Each preparation step must succeed before the next one runs. The temporary output
is unique to this job; a build or exporter failure stops before publication or
checking, without relying on shell errexit. After editing the declaration, rerun
the guarded build/export/rename/check chain, not an old exporter by itself: a failed
build can leave the previous executable in place. Do not treat a cached test result
as regeneration. A previous valid file remains a valid old snapshot, not proof of
fresh source or matching binaries.

The native Namespace `example`, synthetic metric contracts and policy values are
illustrative. Binding implementations and real servers are intentionally absent.
The complete file contains both modes and their exact parent declaration; no JSON
copy is hand-maintained by the developer.

The maintained independent test additionally verifies the selected module path,
offline operation after dependency preparation, an unchanged old-file check after
source edits, explicit regeneration producing a conflict, failed export leaving
the earlier artifact untouched, and failed rebuilding with an existing executable.
On POSIX systems it executes the documented regeneration chain without errexit;
Windows shell-recipe behavior is not qualified. It does not certify distribution
of a future release.
