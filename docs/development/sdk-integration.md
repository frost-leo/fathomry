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

# SDK integration acceptance and upgrade guide

Status: executable acceptance foundation for [Issue #6](https://github.com/frost-leo/fathomry/issues/6),
not acceptance of a production Provider. Follow S01-S11 of the
[accepted standard](../architecture/internal-sdk-integration.md); the original
accepted revision is `0c3f9d82947229a861a30fbf9d1730fa02ca44ea`.
Later integrations record the exact applicable revision/sections, not just a link
to a moving branch. This guide does not authorize the next integration or service use.

## Reusable standard-testing support

Fathomry mechanism and Provider tests import
`github.com/frost-leo/fathomry/internal/conformance`. This is internal maintainer
tooling, not a public Provider-extension SDK or a production dependency. It is
shared across `source`, `operation` and `failure` contracts rather than owned by
one runtime package. Putting it under `operation/operationtest` would still expose
an unnecessary public API and misstate that scope.

Independent business projects use the public capability/diagnostic contracts and
standard `testing` assertions; they do not import this internal tool. The
[consumer boundary test](../../source/boundary_test.go) proves public composition
and compatibility diagnostics work, an external internal-tool import is rejected
by Go, and production foundations do not import the test helper or `testing`.
Supporting externally implemented Providers with a public conformance SDK would
need its own approved extension contract; it is not inferred from selectable
first-party Provider imports. This does not prevent internal Provider packages
from sharing the current acceptance suite.

| Helper | Executable obligation |
| --- | --- |
| `Expected[T]` / `Result` | Actual source/configuration/limits and execution attribution; shape/nesting; present versus missing; technical finality versus local release; primary/cleanup identities; attempt evidence and independent typed-value oracle |
| `Cause[T]` | Original native `errors.As` type/pointer/evidence without formatting it |
| `Receive` | Deadline-bounded independent evidence reception, matched by Call ID rather than arrival order; duplicates/missing evidence; explicit idempotent release only after local subtree completion |
| `Accounting` | Separate active/queued counts and byte reservations and outstanding receipt count/bytes at synchronized checkpoints |
| `Facade` | Method-only facade's exact dynamic/addressable-copy method allowlist, no exported state/callback fields, and no nil facade |
| `Private` | Injected secret canaries through fmt and text/JSON slog, including panic/bound failures, without echoing secrets on failure |
| `Runtime` | The privacy checks plus refused JSON encoding and reconstruction of runtime types |

`Value` must derive expected data/effect facts from an independent input/native
oracle, not copy the integration's output. Present data without an oracle is a
test failure. Check each returned dynamic handle/callback surface under its own
allowlist: a stream may legitimately close itself, not the shared client.
Reflection cannot audit arbitrary closures or provide a Go security sandbox.

Use a separate caller-owned cleanup budget and native stop/join path. `Receive`
requires a deadline and at most 1,024 expectations; timeout is a test failure,
not permission to discard unfinished ownership. Its local release is not durable
recording. Returned payloads/errors retain their capability-owned immutable,
bounded inspection contract; these helpers do not generically deep-copy them.

The existing source facade and operation privacy tests now use the shared checks.
The [contrasting test integrations](../../internal/conformance/integration_test.go) provide
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
The actual consumer SDK-upgrade experiment separately proves that compilation
can pass while the previously accepted retry contract fails.

## Before granting a public capability

- Identify its independently selectable implementation, actual consumers,
  execution shapes, native dependencies and non-goals. Do not expose an umbrella
  import of unrelated Providers.
- Record the applicable standard revision/sections and exact implementation
  commit or reviewed worktree manifest, not merely an issue number.
- Prepare the typed configuration before construction. Test defaults, overlays,
  malformed/lower-layer input, formats, bounds, aliasing and secret provenance.
  Derive the diagnostic profile from those same effective settings.
- Freeze required guarantee selection. Record Provider implementation module,
  Go/Framework/SDK/replacement facts, service/protocol/native evidence, SDK/service
  mode and critical options separately. Apply explicit unknown/untested policy.
- Prove original resource identity across aliases, shared allowance, no owning
  client escape, partial initialization cleanup and unchanged delegation semantics.
- Exercise the real public operation path; merely calling `source.Bind` does not
  install admission or evidence wrapping.
- Bound logical calls, SDK attempts/retries, queues, unresolved receipts, record/
  batch size, bytes, SDK buffers/prefetch/decompression, sessions, background work,
  native memory and temporary disk as relevant. State the owner, units, scope,
  overload behavior and termination path for each. Reservations are not RSS.
- Independently verify partial/unknown/empty results and per-owner attribution.
  Never expand aggregate ACKs into per-Item commit/visibility/transaction claims.
- Keep required evidence independent of business error handling and diagnostics.
  Recording/export failure cannot replace the primary result or invent durability.
- Test shutdown at saturation. Drive necessary native drain/stop through existing
  responsibility or an explicitly owned control path; do not put the only
  progress action behind a final resource release waiting for those same calls.
- Audit raw native errors, formatters/log hooks, callbacks and returned handles
  with injected privacy canaries. Preserve original inspectable causes and runtime
  JSON guards. Local release cannot acknowledge durable receipt storage.
- Obtain explicit isolated-service authorization for any service test. Record
  actual version/features/options, actions, cleanup and limits. No production
  credentials or implicit infrastructure are permitted.

Preserve `Resolve` as early technical evidence; `Finish` as final technical/
cleanup evidence **without release**; `Release` as positive producer-use ending
with descendant protection; and `Complete` only when both kinds of facts are
known. `Execute` owns callback completion, and its shortcut requires callback
return to establish its own use has ended or already-retained later responsibility.
Borrowing scopes already admitted before owner shutdown may finish independently.

## Upgrade comparison

Capture old and candidate combinations with the actual consuming executable.
Run the same correctness workload/oracles and hold data size, concurrency,
service conditions and guarantees fixed. Change one mechanism at a time where
possible; record contradictory results, not just successful compilation/throughput.

| Axis | Minimum comparative evidence |
| --- | --- |
| Defaults/configuration | Omitted/zero/null/overridden settings, parser options and hidden environment/file defaults; same effective profile cannot name different behavior |
| Retry | Actual attempts, limits, authentication refresh/background traffic, amplification, uncertain prior effect and attribution |
| Cancellation/completion | Expired queued work, caller wait versus native use, async callback ordering, retained handles, cleanup/stop/join and independent budgets |
| Error identity | Public semantic revision, intended `errors.Is/As`, unchanged causes and separate cleanup failures; no retry decision from error text |
| Results | Partial/unknown/empty/missing, accepted/committed/visible meaning, per-owner granularity, late/duplicate output and record-size bounds |
| Resources | Count/bytes/queue/receipts plus actual native/prefetch/worker limits; overload and unfinished work; no new acquisition behind a held permit |
| Durable/Temporal contracts, if changed | Owner/version, old reader interpretation, migration or refusal, relevant replay and conversion evidence |

Compilation, a semantic-version range or connection health cannot replace any
applicable behavioral gate. Missing or skipped service evidence stays missing.
Record measured allocations/latency tails/throughput only when actually measured;
state unmeasured native/RSS/disk dimensions and unsupported modes explicitly.

## Current traceable integration record

The [executable record example](../../internal/conformance/record_test.go)
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
| Common mechanism | Existing source/operation/failure suites plus shared assertions, mutation subprocesses and public consumer test; in-process only |
| Capability contract | Local transfer vocabulary and independent lifetime/output oracles across pull, async, session and finite shapes; no real transaction/delivery guarantee |
| Fixed native dependency | YAML v3.0.5, checksum `h1:N6y/pJk8buWs9NY5ERU2HSMfm+IuD/OtfdAnq6kESPw=`; source configuration suite plus [the native YAML boundary test](../../source/yaml_test.go). Upstream decoder option differences are executed, not equated with Fathomry semantics |
| Synthetic module execution | `TestIndependentConsumerBuildSelectionAndBehavior` builds an independent application with file-proxy SDK/provider fixtures. MVS, retry-default changes, versioned/local replacement, local modification and workspace cases execute without network |
| Real service | None selected/authorized/run. The record's skipped service entry does not represent a passing service test |
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

## Reproduce and hand off

Run from the repository with its selected dependencies available:

```sh
go mod tidy -diff
test -z "$(gofmt -l .)"
go vet ./...
go test -race -count=1 -timeout=10m ./...
go build ./...
go test -race -count=20 -timeout=2m ./source ./operation ./failure ./internal/conformance
go test -race -count=3 -timeout=3m ./compatibility
go test ./compatibility -run '^$' -fuzz '^FuzzBuildMetadataPrivacy$' -fuzztime=10s -parallel=2
go test ./source -run '^$' -fuzz '^FuzzPrepare$' -fuzztime=10s -parallel=2
git diff --check
bash .agents/skills/fathomry-development/scripts/new-issue_test.sh
```

The independent build test seeds an isolated local module proxy with synthetic
fixtures and the repository's already-cached selected YAML zip; it disables
network/sumdb access and owns a writable temporary module cache for cleanup.
The probes' explicitly selected JSON output is private test transport, not a
public version-diagnostic serialization contract.

Record the exact tested commit/worktree, actual toolchain/platform, commands and
outputs, each real-service combination or explicit absence, limits, failed
experiments and outstanding acceptance gates on the PR/reference handoff. Local
verification here uses Go 1.26.4/linux/amd64; the module minimum/CI selection remains
Go 1.26.0. No new SDK, infrastructure, benchmark platform, publishing automation
or complete business Framework/Adapter was introduced.

An SDK-specific issue still requires its own owner-approved scope, native source
review, supported guarantee contract and authorized service verification. Do not
start it or manufacture a backlog from this guide.
