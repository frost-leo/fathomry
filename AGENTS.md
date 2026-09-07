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

# Fathomry Engineering Instructions

Fathomry is a Go framework for durable, observable data workflows on Temporal.
Use the `fathomry-development` skill for project discussion, issue implementation,
verification, and handoff. This file defines engineering judgment, not task status.

## Reason from evidence

- Start with the owner's observable outcome, constraints, and non-goals. Separate
  facts, hypotheses, proposed mechanisms, and accepted decisions; report uncertainty.
- Use first-principles reasoning: derive necessary behavior from invariants,
  protocol/SDK guarantees, ownership, and resource limits before copying a pattern.
  A proposed folder layout or abstraction is not a requirement.
- Seek disconfirming evidence. Compare material alternatives and test the cheapest
  assumption that could invalidate the design. Do not mistake agreement for proof.
- For feedback mechanisms, identify the target, measured signal, actuator, delay,
  disturbance, and saturation limits. Examine overload, retry amplification, and
  unstable reactions. Engineering-control thinking does not mandate a new controller.
- Use controlled comparisons and ablation: hold workload and correctness guarantees
  fixed, change one mechanism, and measure the result against a baseline. Report
  contradictory results and costs, not just the favorable throughput number.
- Scale the method to the decision. A small reversible edit needs a small check;
  important changes need explicit failure cases and evidence, not ceremonial reports.

## Write idiomatic Go

- Give each package a clear owner and dependency direction. Use concise lowercase
  names, meaningful filenames, and useful Go documentation for exported contracts.
- Prefer native standard-library capabilities and concrete types. Introduce small
  consumer-owned interfaces only for a real boundary; do not mirror an entire SDK.
- Pass caller-owned contexts explicitly. Define ownership of goroutines, streams,
  timers, pools, and connections; bound concurrency and prove termination paths.
- Specify copying, borrowing, mutation, and concurrent-use semantics. Never present
  shared mutable state as a safe snapshot or hide resource ownership in globals.
- Handle errors at meaningful boundaries; preserve intentional `errors.Is`/`As`
  behavior. Separate observed effect evidence, retry policy, and human presentation.
  Cancellation or a timeout does not prove an external mutation never happened.
- Keep runtime handles out of durable DTOs. Define versions, bounds, units,
  absent/zero/unknown semantics, privacy, compatibility, and migration explicitly.
  Module, SDK, wire, workflow/mode, and deployment versions are different axes.
- Observe useful outcomes without leaking secrets or creating unbounded metric
  cardinality. Measure allocations, latency tails, memory, and resource use when
  they matter; do not add caches or pools without evidence.
- Use `gofmt`, standard Go checks, focused tests, and relevant race/fuzz/benchmark
  cases. Verify malformed input, aliasing, cancellation, cleanup, retries, and partial
  effects where applicable. Temporal command or serialization changes need replay
  evidence; ordinary wall-clock/goroutine code does not belong in Workflow logic.

## Keep boundaries explicit

- One session implements one approved issue. Discuss and prepare one next issue
  with the owner afterward; do not implement it in the same session or manufacture
  a backlog. If unfinished, hand off the current issue instead.
- Product code stays here. Sibling `../reference` holds working requirements,
  per-issue discussions/designs/evidence, and pinned SDK sources. `docs/` contains
  only established API documentation and accepted architecture, with truthful status.
- Preserve unrelated changes. Sign project commits and obey repository branch
  rules. Owner-authorized Codex work is not independent human review.
- Do not expose secrets, private payloads, or deployment inventories. Reference
  access does not authorize production writes, publishing private material, or
  additional infrastructure. Missing evidence remains missing.
- First-party code, comments, and maintained documents use English; localized human
  text belongs in resources. Preserve historical source material and upstream notices.
  Use the full `.github/LICENSE_HEADER` in valid comments or permitted metadata;
  skill frontmatter comes before its Markdown license notice.

The session skill locates supporting research and current task material on demand.
Do not preload all documentation, issue history, upstream skills, or SDK trees.
