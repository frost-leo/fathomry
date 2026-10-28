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

# Conformance fixtures and evidence classes

[Documentation](../../../README.md) / Internal package reference

**Audience:** integration test and evidence maintainers.
**Status:** executed local fixtures and an opt-in isolated resource-service profile,
not production SDK/service certification.
**Package:** `github.com/frost-leo/fathomry/internal/conformance`.

Start with the [conformance interface](interface.md). These examples explain
what evidence can establish, not a service support matrix.

## Execution-shape fixtures

Resource facade and invocation privacy tests use the shared checks.
The [contrasting test integrations](../../../../internal/conformance/integration_test.go) provide
copyable usage patterns, not a universal capability interface:

- Pull-style consumption returns a non-owning stream facade, reports partial data
  and uncertainty, and retains ownership through stream close.
- Asynchronous delivery occurs before the submit stack returns. A pre-reserved
  guard protects that stack; duplicate completion does not erase the first result.
- A background session has independent establishment/lifetime/message budgets,
  a bounded message channel, incrementally drained child evidence, and separate
  stop, final cleanup report, join and local-release events.
- Finite calls vary payload bytes and concurrency, expire queued work, and retain
  completed-but-unreceived evidence. No callback reacquires its own source permit.

`TestBrokenIntegrationsFailRealContractTests` runs faulty integrations in bounded
Go test subprocesses. Exposing owner shutdown, omitting the submit guard,
promoting unknown output, exposing private causes/payloads, replacing native
identity, and conflating missing/empty output each cause actual testing failures.
Owning callback fields and pointer-only shutdown methods reachable through a
copied value are also rejected. The parent fails if any of the eight mutations
unexpectedly passes or discloses the canary.
The [helper contract regressions](../../../../internal/conformance/checks_test.go) and
[diagnostic regressions](../../../../internal/conformance/privacy_test.go) use the same
parent/child direction with valid controls. Each invalid child must return from
the helper, exit 1 with the expected conformance failure, and disclose no canary;
an unexpectedly accepted fixture, crash or timeout fails its parent. They cover
promotion/hiding/method sets, safe literal diagnostics, fmt and text/JSON hooks,
nil receivers, nested groups/containers, resolution/traversal limits and runtime
JSON refusal/panic boundaries. Text-only and JSON-only log faults independently
exercise both handlers. These are helper acceptance facts, not SDK support.
The actual consumer SDK-upgrade experiment separately proves that compilation
can pass while the previously accepted retry contract fails.

## Current traceable integration record

The [executable record example](../../../../internal/conformance/record_test.go)
`TestTraceableFixtureRecordNeverClaimsServiceSupport` links:

- Standard revision `0c3f9d82947229a861a30fbf9d1730fa02ca44ea`,
  particularly S03-S08 and S10-S11.
- Implementation ID `gh-6-reviewed-worktree`, resolved to the #6 PR's reviewed
  commit/content manifest; before approval this denotes an uncommitted worktree,
  not a released module or attested artifact.
- Executed pull, async, background and bounded-workload subtests. Passed/failed
  evidence comes from actual `testing.T.Run` results.
- The available actual Go/Framework/YAML facts in the test executable and the
  source format/revision/limits used by its recorded finite-handoff subtest, with
  payload size, evidence capacities and execution budget in the effective profile.
  The contrasting-shape tests have their own documented combinations; they are
  not silently combined into that single baseline.
- Explicit missing transfer-SDK and skipped/not-authorized service evidence.
  The test asserts that adding a service requirement cannot become supported.

Incomplete development/test-binary provenance stays unknown even when the fixture
tests pass. No startup code is allowed to manufacture a passing baseline from its
current metadata.

| Evidence class | Current evidence and limits |
| --- | --- |
| Common mechanism | Private fault/resource/invocation/compatibility suites, standard Go error wrapping, boundary envelopes, mutation subprocesses and independent import checks; in-process only |
| Capability contract | Local transfer vocabulary and independent lifetime/output oracles across pull, async, session and finite shapes; no real transaction/delivery guarantee |
| Fixed native dependency | YAML v3.0.5, checksum `h1:N6y/pJk8buWs9NY5ERU2HSMfm+IuD/OtfdAnq6kESPw=`; source configuration suite plus [the native YAML boundary test](../../../../internal/resource/yaml_test.go). Upstream decoder option differences are executed, not equated with Fathomry semantics |
| Synthetic module execution | `TestIndependentConsumerBuildSelectionAndBehavior` builds an independent application with file-proxy SDK/provider fixtures. MVS, retry-default changes, versioned/local replacement, local modification and workspace cases execute without network |
| Real service | None in this historical record. Its skipped entry does not represent the separate resource-service profile below |
| Temporal/replay | Not applicable: no Temporal runtime dependency, Workflow command, converter or serialized history/payload format changed |

The fixed YAML library is configuration evidence, **not** an async/stream service
SDK or transfer Provider. Its decoder source was inspected at upstream commit
`e16c7af9361b241fa02d91582fb59ce4954d8afc`.
[Upstream v3.0.5 decoder](https://github.com/yaml/go-yaml/blob/v3.0.5/yaml.go).
No candidate from the sibling SDK inventory was promoted into runtime dependencies.

The bounded fixture envelope varies 0, 1, 4,096 and 65,536 payload bytes with
concurrency 1 and 4. The largest simultaneous tracked fixture payload is 262,144
bytes; independent evidence reservations are 128 bytes per call, at most 5 slots
including a queued caller. Background tests own one 4,096-byte payload buffer and
a channel of one message, and drain 32 child results. These are tracked fixture
payloads/declarations, not heap/RSS/native-library hard caps or throughput claims.

## Public owner and native service composition

[`resource_service_test.go`](../../../../internal/conformance/resource_service_test.go)
is excluded from ordinary tests by the `resource_service` build tag. It composes
the public settings/resource packages with existing Internal Nacos and PostgreSQL
capabilities, without requiring or introducing an Adapter. Native assembly and
receipt handling are test-only integration details; the public holder still
depends only on public failure/settings and the standard library.

Run only against explicitly authorized development services, from the repository root:

```sh
FATHOMRY_RESOURCE_TEST_CONFIG=/absolute/path/private-fixture.json \
  go test -race -tags=resource_service ./internal/conformance \
  -run '^TestResourceService$' -count=1 -timeout=6m -v
```

The private JSON schema is the test's `resourceServiceFixture` declaration. The
file must be regular, at most 128 KiB, and inaccessible to group/other users
(normally mode 0600). Both top-level and Nacos `allow_writes` must be true.
Inputs explicitly supply Nacos HTTP/gRPC endpoints, namespace, reader/admin
credentials and transport choice, plus the selected PostgreSQL address, port,
database, credentials and TLS/plaintext choice. Do not commit this file, put
credentials in command arguments, or copy deployment inventories into evidence.
Plaintext flags authorize only isolated development checks, never a production
default or proof of TLS behavior.

The tests establish absence before creating random unique Nacos keys, then delete
them and verify absence during cleanup. PostgreSQL requires CONNECT and TEMP
permission in the selected database; writes use a random session-local temporary
table with ON COMMIT DROP. No existing keys, permanent tables, database migration,
server restart or deployment modification is part of the test.

Observed obligations include:

- real Nacos invalidations, explicit reread/validation, coherent accepted settings
  publication and the public accepted-view Watch handoff;
- malformed/invalid data not replacing accepted settings, a real native SQL
  readiness failure retained through the public construction error, and last-good
  availability while a failed candidate's native ownership is cleaned;
- separate Fixed/Follow adoption, new connections on a replacement pool, and the
  old transaction retaining its original backend and temporary session state;
- interrupted Close retaining the borrowed transaction until commit/release,
  confirmed temporary-table removal, native cleanup and evidence-inbox draining;
- independently registered old/new Nacos subscriptions, whose client generations
  remain borrowed until explicit subscription close and lease release.

The owner-authorized #98 audit executed these scenarios against development
services, including repeated race runs. Native reconnection/CAS/denial tests remain
owned by the [Nacos package](../configsource/nacos/v2/interface.md). The profile does
not certify all SDKs, HA/TLS, control-store migration or automatic subscription
transfer. Nacos's error-only Close is translated conservatively: an error retains
ownership unless completion is separately established; this profile does not
prove recovery from every terminal native cleanup error.

## Withdrawn Framework configuration fixture

The former live Framework configuration fixture was removed with its product
APIs in [#161](https://github.com/frost-leo/fathomry/issues/161).
Its [historical source](https://github.com/frost-leo/fathomry/blob/b39c55071a6af2949e15ffa6a02a5443ab3c9173/internal/conformance/configuration_service_test.go)
preserves earlier evidence, not a runnable current command or replacement
configuration-host qualification. The independent Internal resource/native
fixtures above remain separate.

## Consuming-binary fixture

The independent build test seeds an isolated local module proxy with synthetic
fixtures and the repository's already-cached selected YAML zip; it disables
network/sumdb access and owns a writable temporary module cache for cleanup.
The consuming probe explicitly imports/executes YAML, declares its selected version
and executes SDK behavior. The parent inspects that same built executable via
`debug/buildinfo.ReadFile` and private `FromBuildInfo`. Fathomry is an intentionally
unused requirement there, with absent Framework metadata: this particular probe
deliberately imports no public package. The in-module probe executes `Inspect` with real
Fathomry-as-main/VCS facts; synthetic BuildInfo tests cover dependency normalization,
not real external framework consumption. The probes' selected JSON output is private
test transport, not a public serialization contract. The independent import checks
do not force `-race`; race coverage is an explicit in-module verification.
