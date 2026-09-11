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

# Run tests and verification

[Documentation](../README.md) / Development

**Audience:** maintainers verifying approved code or documentation changes.
**Status:** executable local workflow, not real-service support evidence.

Run commands from the repository root. Start with the affected contract's smallest
meaningful check, then broaden. Do not weaken assertions to make a failure disappear.

## Prerequisites

- Use the Go version required by [go.mod](../../go.mod). Recorded local verification
  covers Go 1.26.0 and Go 1.26.4/linux/amd64, not a claim about the latest patch.
- Put the selected toolchain's `bin` first in `PATH` and use `GOTOOLCHAIN=local`
  so child Go commands use it too. Race runs need a suitable CGO/race environment;
  ordinary CGO-disabled checks are separate below.
- Make dependencies available. `go mod download` may need network on a new checkout.
  Build fixtures seed an isolated file proxy using the cached YAML v3.0.5 archive,
  then disable registry/sumdb access. They do not use a real service.
- Service tests require separate authorization, isolated resources and cleanup;
  no production credentials are provided to ordinary tests.

## Focus the check

This workspace-selection subtest initializes its own SDK fixture and runs alone:

```sh
go test -race -count=1 -timeout=3m ./internal/compatibility -run '^TestIndependentConsumerBuildSelectionAndBehavior$/^workspace-selection$'
```

Also run the complete group after changes to build/metadata fixtures:

```sh
go test -race -count=1 -timeout=3m ./internal/compatibility -run '^TestIndependentConsumerBuildSelectionAndBehavior$'
```

For package contracts and executable documentation:

```sh
go test -race -count=1 ./internal/resource ./internal/invocation ./internal/fault
go test -count=1 -run '^Example' ./...
go doc -all ./internal/resource
```

These are local fixtures, not a public framework tutorial. The
[conformance reference](../reference/internal/conformance/fixtures.md) separates
mechanism, native parser, synthetic SDK/module and missing service evidence.

Configuration/type and conformance acceptance regressions have explicit controls:

```sh
go test -count=1 ./internal/resource -run 'Test(Generic|PreparedAnonymous|YAMLExplicit|YAMLTag|CleanupHistory)'
go test -count=1 ./internal/conformance -run 'Test(Runtime|Facade)'
```

`TestGenericConversionsCompileBoundary` first compiles valid aliases/settings,
then requires the compiler's intended cross-type conversion diagnostic in isolated
fixtures. Keep illegal casts out of ordinary compiled test files. Runtime/Facade
negative controls execute in deadline-bounded child processes and require the
matching conformance failure, successful return from the helper and no canary
disclosure. A PASS from a historical false-certification witness is not acceptance.

## Standard repository checks

For the internal Viper v1 integration, exercise the real consumer, raw preparation
handoff and rejecting controls before broadening to full checks:

```sh
go test -race -count=1 -timeout=3m ./internal/configsource/viper/v1
go test ./internal/configsource/viper/v1 -run '^$' -fuzz '^FuzzLoad$' -fuzztime=10s -parallel=2
go test ./internal/configsource/viper/v1 -run '^$' -fuzz '^FuzzQuery$' -fuzztime=10s -parallel=2
GOMAXPROCS=4 go test ./internal/configsource/viper/v1 -run '^$' -bench '^BenchmarkLoading$' -benchmem -benchtime=10x -count=5
go run ./internal/configsource/viper/v1/testdata/consumer
```

The benchmark compares equal useful results and input/I/O conditions, not an
unbounded native reader with a bounded integration. Report the additional
SDK/preflight parsing and raw-copy costs for preparation separately from native
query costs. Percentiles are bounded in-process samples (up to 128 per run), not
production tail-latency guarantees. See the [native profile and bounds](../reference/internal/configsource/viper/v1/interface.md).

Load/query tests own the corresponding core behavior. Integration tests exercise
real files, strict preparation and the consuming executable; platform-specific
and benchmark files remain separate where their execution conditions differ.
Coverage reports help locate missing paths, not certify all inputs. Do not replace
the real SDK merely to force an unreachable defensive branch to reach 100 percent.

The authoritative CI commands are in [checks.yml](../../.github/workflows/checks.yml).

For the Nacos v2 integration, use the selected toolchain's `bin` first in PATH:

```sh
go test -race -count=3 -timeout=3m ./internal/configsource/nacos/v2
go run ./internal/configsource/nacos/v2/testdata/consumer
go test ./internal/configsource/nacos/v2 -run '^$' -fuzz '^FuzzOptions$' -fuzztime=10s -parallel=2
go test ./internal/configsource/nacos/v2 -run '^$' -fuzz '^FuzzProtocol$' -fuzztime=10s -parallel=2
GOMAXPROCS=4 go test ./internal/configsource/nacos/v2 -run '^$' -bench '^BenchmarkRead$' -benchmem -benchtime=10x -count=3
```

The benchmark compares equal owned native-session lifetimes and raw results,
not a warm SDK cache against a cold connection. The consumer reports the actual
SDK in its own binary; its loopback service is not deployment acceptance.

The opt-in service gate **writes and deletes generated test keys**. Run it only
with explicitly authorized isolated resources and credentials:

```sh
FATHOMRY_NACOS_TEST_CONFIG=/path/to/private-nacos-fixture.json \
  go test -tags=nacos_service -race -count=1 -timeout=4m \
  ./internal/configsource/nacos/v2 -run '^TestNacosServiceReadWriteWatchAndCleanup$'
```

The bounded mode-0600 JSON fixture contains `http_url`, `grpc_address`, `namespace`,
`username`, `password`, `admin_username`, `admin_password`, optional `root_ca_pem`,
`allow_insecure` and explicit `allow_writes`. Use a read-only reader and a separately
authorized fixture publisher. The gate verifies generated-key absence before
writes, deletes those keys and checks absence during cleanup. It does not alter
roles, deploy services, raise platform limits or restart remote nodes. Its
connection-interruption test drops only this client's connection. Periodic
reconciliation is set beyond the bounded push wait to avoid false push acceptance.
Compiling with `-run '^$'` runs no real-service test. See the
[Nacos profile](../reference/internal/configsource/nacos/v2/interface.md) for current
version, native limitations, upstream-upgrade TODO and unexecuted service modes.

For a normal implementation change:

```sh
go mod tidy -diff
go mod verify
test -z "$(gofmt -l .)"
go vet ./...
go test -race -count=1 -timeout=10m ./...
go build ./...
git diff --check
bash .agents/skills/fathomry-development/scripts/new-issue_test.sh
```

The scaffold check is offline and does not publish issues. Signing, PR checks and
merge remain separate gates in [contribution policy](../../.github/CONTRIBUTING.md).

## Repeated concurrency and compatibility checks

Choose repeats and bounds proportionate to the changed behavior:

```sh
go test -race -count=20 -timeout=5m ./internal/resource ./internal/invocation ./internal/fault ./internal/conformance
go test -race -count=3 -timeout=3m ./internal/compatibility
go test -race -shuffle=20260910 -cpu=1,4 -count=3 -timeout=5m ./...
CGO_ENABLED=0 go test -count=1 -timeout=10m ./...
```

Whole-test shuffling does not establish nested subtest independence; select important
subtests explicitly. Independent-module checks do not force child `-race`. The
CGO-disabled suite must execute its import/build checks, not silently skip them.

## Bounded fuzz checks

Use the existing targets, not a new test framework:

```sh
go test ./internal/resource -run '^$' -fuzz '^FuzzPrepare$' -fuzztime=10s -parallel=2
go test ./internal/compatibility -run '^$' -fuzz '^FuzzBuildMetadataPrivacy$' -fuzztime=10s -parallel=2
go test ./internal/conformance -run '^$' -fuzz '^FuzzPrivateLiteralDiagnostics$' -fuzztime=10s -parallel=2
go test ./internal/fault -run '^$' -fuzz '^FuzzTechnicalFaultContext$' -fuzztime=10s -parallel=2
```

Record the corpus, executions, failures and limits when relevant. Bounded success
is neither exhaustive proof nor a throughput comparison. For ownership/result
changes, use independent native/workload oracles and deliberately broken controls.
Compilation failure or timeout is not the intended contract rejection unless the
test explicitly owns that behavior.

## Record what was proved

Record exact commit/worktree, toolchain/platform, commands and observed outcomes,
including failed-before/fixed-after evidence. Payloads/errors/native allocations
retain their stated ownership and bounds. The finite microbenchmark measures
mechanism overhead, not SDK throughput or a native-memory cap. Sustained fixtures
check declared high-water limits, not every physical resource.

The [build test](../../internal/compatibility/consumer_test.go) executes SDK behavior
and inspects that same binary. Fathomry intentionally contributes no public package
there; a separate in-module probe covers framework-as-main. Do not substitute
inspector metadata or manufacture passing support records from startup facts.

Actual SDK termination, session/account isolation, service effects, native buffers
and workload/SLO limits still need concrete integration evidence. No Temporal
command, converter or history/payload format is introduced by these foundations;
future changes to those boundaries need appropriate replay evidence. Keep raw
logs in local issue literature, not the product manual.
