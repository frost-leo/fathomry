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

# Develop and verify an SDK integration

[Documentation](../README.md) / Development

**Audience:** maintainers of an approved integration issue.
**Status:** maintainer workflow, not Provider certification.

Use this procedure to establish actual capability and evidence, not merely to
make a client compile. It does not authorize selecting a new SDK or using a service.
Follow the [integration architecture](../architecture/sdk-integration.md) and record
the exact standard revision/sections, not only a moving-branch link.

## Prerequisites

- An owner-approved scope, required guarantees, modes and non-goals.
- Relevant pinned SDK source and actual dependency/build information.
- Explicit resource, callback, evidence and cleanup owners.
- Separate authorization for isolated-service work; no production credentials.

## 1. Establish the integration boundary

The [#15 contract map and migration](../architecture/package-boundaries.md) separate ordinary
business use from maintainer integration. Configuration preparation and ownership
live in `internal/resource`; the complete call/result/evidence/observation protocol
lives in `internal/invocation`; build/compatibility mechanisms remain private too.
Their errors use `internal/fault`, never public framework attribution. Examples
importing them are in-module maintainer fixtures.
Business authors declare allowed configuration/capabilities and write business
logic. Future loaders (including configuration-center SDKs), resource assembly,
admission and evidence loops remain framework responsibilities. The CLI,
`fathomry new`, loader and full execution entry are not implemented here.

## 2. Implement and test the guarantees

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
- Exercise the real public operation path; merely calling `resource.Bind` does not
  install admission or evidence wrapping.
- Bound logical calls, SDK attempts/retries, queues, unresolved receipts, record/
  batch size, bytes, SDK buffers/prefetch/decompression, sessions, background work,
  native memory and temporary disk as relevant. State the owner, units, scope,
  overload behavior and termination path for each. Reservations are not RSS.
- Independently verify partial/unknown/empty results and per-owner attribution.
  Never expand aggregate ACKs into per-Item commit/visibility/transaction claims.
- Freeze framework Run/Item/attempt attribution at the boundary before native
  submission. Preserve it with required evidence through early wait returns and
  late callbacks; do not insert framework semantics into technical correlation.
  Preserve the native multi-cause tree through standard Go `error`. The
  [boundary tests](../../internal/conformance/boundary_test.go) use local sentinels
  and ordinary wrapping plus a test-owned attribution envelope, not a replacement
  public error API, production Adapter or unbounded lookup table.
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

## 3. Record the exercised combination

Use the [conformance interface](../reference/internal/conformance/interface.md)
and [fixture examples](../reference/internal/conformance/fixtures.md) for independent
data/lifetime oracles. Read [diagnostic probe limits](../reference/internal/conformance/diagnostics.md)
before interpreting a helper failure or declaring a facade safe.

Derive the Profile from the exact constructed settings and capture the
[actual build](../reference/internal/compatibility/build-info.md). Record which
mechanism/capability/SDK/service layers actually executed. Apply
[assessment rules](../reference/internal/compatibility/assessment.md) without turning
missing, skipped or synthetic evidence into supported-service claims.

## 4. Compare an SDK upgrade

Capture old and candidate combinations with the actual consuming executable.
Run the same correctness workload/oracles and hold data size, concurrency,
service conditions and guarantees fixed. Change one mechanism at a time where
possible; record contradictory results, not just successful compilation/throughput.

| Axis | Minimum comparative evidence |
| --- | --- |
| Defaults/configuration | Omitted/zero/null/overridden settings, parser options and hidden environment/file defaults; same effective profile cannot name different behavior |
| Retry | Actual attempts, limits, authentication refresh/background traffic, amplification, uncertain prior effect and attribution |
| Cancellation/completion | Expired queued work, caller wait versus native use, async callback ordering, retained handles, cleanup/stop/join and independent budgets |
| Error identity | Technical kinds, intended `errors.Is/As`, unchanged causes and separate cleanup failures; review any actual future public semantic contract separately, with no retry decision from error text |
| Results | Partial/unknown/empty/missing, accepted/committed/visible meaning, per-owner granularity, late/duplicate output and record-size bounds |
| Resources | Count/bytes/queue/receipts plus actual native/prefetch/worker limits; overload and unfinished work; no new acquisition behind a held permit |
| Durable/Temporal contracts, if changed | Owner/version, old reader interpretation, migration or refusal, relevant replay and conversion evidence |

Compilation, a semantic-version range or connection health cannot replace any
applicable behavioral gate. Missing or skipped service evidence stays missing.
Record measured allocations/latency tails/throughput only when actually measured;
state unmeasured native/RSS/disk dimensions and unsupported modes explicitly.

## 5. Verify and hand off

Run the [testing workflow](testing.md), including relevant focused faults, negative
controls and full checks. Identify the exact commit/worktree, actual toolchain,
command and observed output. Record each service combination or its absence,
limits, contradictory results and outstanding acceptance gates.

Keep raw logs/research in local issue literature; publish established contracts
and accepted architecture here. Follow [contribution policy](../../.github/CONTRIBUTING.md)
for signed publication/review/merge. This guide does not create another integration
issue or authorize a follow-on backlog.
