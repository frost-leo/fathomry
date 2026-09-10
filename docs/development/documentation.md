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

# Writing documentation

[Documentation](../README.md) / Development

**Audience:** contributors and agents maintaining Fathomry documentation.
**Status:** accepted documentation policy for this repository.

Organize documentation around a reader's question, not the order of issues or
source files. Architecture explains cross-package decisions; package references
state calling contracts; development guides help maintainers perform a task.
This policy is canonical. Contribution and agent instructions link here rather
than duplicating it.

## Choose the right home

| Reader's question | Location | Content owner |
| --- | --- | --- |
| Where do I start, and what actually exists? | `docs/README.md` | Documentation navigation and current capability status |
| Why are responsibilities divided this way? | `docs/architecture/<topic>.md` | A cross-package architectural topic, its invariants and tradeoffs |
| What does this package require and guarantee? | `docs/reference/<package-path>/interface.md` | The actual package's calling contract |
| What are the details of one package behavior? | `docs/reference/<package-path>/<topic>.md` | One substantive topic within that package |
| How do I implement, verify or maintain something? | `docs/development/<task>.md` | An executable maintainer workflow |
| Why was a proposal considered; what did an experiment show? | Sibling `../reference/` workspace | Working research, issue discussions and raw evidence, not product documentation |

For example, `internal/resource` maps to
`docs/reference/internal/resource/interface.md`. This is published technical
reference for an internal package, not permission for an external Go module to
import it. `docs/reference/` is distinct from the sibling research workspace.

Create directories only for real content. Do not create empty tutorial, CLI,
deployment, Provider or language trees to suggest future capabilities exist.
Add a runnable tutorial when its actual end-to-end path exists. Do not invent a
public package, Go interface, wrapper or configuration field to populate a page.

## Give every page one job

- Use a descriptive English title and lowercase, hyphenated filenames.
- Use inline code for identifiers, paths, field names and literal values.
- Follow the title with navigation to the documentation map, an **Audience** and
  a truthful **Status**. State the package/import scope when relevant.
- Lead with the answer or purpose. Put historical issue/commit provenance near
  the relevant evidence or at the end, not in place of an introduction.
- Use headings for reader questions and meaningful behaviors. Add a short
  contents list when a long page spans several independently useful sections.
- Keep one authoritative description of each rule. Link related architecture,
  contract details and procedures instead of copying entire sections.
- Split when a topic has its own useful reading purpose, not for every type or
  field. Small packages can have only `interface.md`; a package README is optional
  and should add navigation rather than duplicate that contract.

## Write architecture by topic

An architecture page should explain:

1. The problem, relevant scope and current implementation status.
2. Responsibilities across packages/roles and the direction of dependencies.
3. Invariants and the failures those invariants prevent.
4. Material alternatives, selected tradeoffs and limitations.
5. Links to the packages that implement the decision and its supporting evidence.

Do not turn an architecture page into a full API listing or an issue transcript.
An accepted requirement for a future integration is not an implemented guarantee.
The [integration-standard index](../architecture/internal-sdk-integration.md)
preserves existing S01-S12 identifiers; its topic pages own the normative text.

## Use interface.md as the package contract entry

Here, interface means the package boundary, including functions and concrete
types. It does not require introducing Go `interface` declarations or mirroring
an SDK. The entry must cover the following when applicable:

1. Package path, actual callers, responsibilities and non-goals.
2. Provided capabilities and required caller-owned inputs/dependencies.
3. Key types/functions and the valid call sequence, linked to source documentation.
4. Input/output meanings, including nil, zero, absent, unknown, units and bounds.
5. Ownership, copying/borrowing, concurrency, cancellation and termination duties.
6. Errors and partial effects: what a result proves and what it does not prove.
7. Links to deeper topic pages and executable examples or contract tests.

Use a concise structure rather than empty template sections:

```markdown
# <Package> interface

[Documentation](<relative-link>) / Internal package reference

**Audience:** <actual callers>.
**Status:** <implemented contract and important limits>.
**Package:** `<import-path>`.

## Responsibilities
## Capabilities and call sequence
## Caller obligations
## Results and failure boundaries
## Details and executable evidence
```

Exact signatures and symbol-level behavior stay with the Go declarations. Package
comments introduce the package once; Markdown connects the package-wide contract
and the specialized topics. Do not hand-maintain a second complete signature list.
When behavior changes, update the affected comments, tests and documentation in
the same change; disagreements are defects, not independent interpretations.

## Write guides as tasks

State the goal, prerequisites and the directory from which commands run. Order
the steps and give an observable completion check. Link contract details rather
than hiding reference tables inside the procedure. Clearly label commands that
need network access, CGO, isolated services or additional authorization.

Use existing executable examples and tests. Distinguish a successful build, a
test double, SDK execution and real-service execution. A skipped test, a source
review or a version string is not a supported-service result. Do not describe the
intended `fathomry new` command as runnable while the CLI remains unimplemented.

## Preserve evidence, versions and links

- State whether a page describes an implemented contract, accepted architecture
  or a maintainer procedure. Keep proposals and raw logs outside product docs.
- Keep module, SDK/service, configuration, source revision, public contract and
  durable/Workflow format versions distinct. State relevant limitations explicitly.
- Link directly to authoritative sources. Pin upstream code when a claim depends
  on exact behavior; distinguish source observations from executed tests.
- Use relative links for repository pages and source. Keep existing standard
  anchors or provide a migration index when relocating their content.
- Update current navigation and incoming links when moving a page. Do not rewrite
  commit-pinned historical URLs or preserve duplicate full documents as redirects.
- Follow the repository's English-language and full-license-notice rules. Do not
  expose secrets, private payloads or deployment inventories in prose or examples.

## Review before handing off

1. Confirm the page's audience, category, status and topic match its contents.
2. Check every local target and section anchor, including links from the main README.
3. Compare named types/functions, defaults, bounds and lifecycle claims with code.
4. Run changed example/test commands where practical. Record exactly what executed;
   do not count a syntax check as runtime evidence or rerun unrelated service tests.
5. Check that no acceptance condition or cited counterexample vanished during a move.
6. Run `git diff --check`; use `go doc -all ./internal/<package>` and the relevant
   existing tests for contract changes. Documentation-only changes need no new
   site generator or testing framework.

## Basis for this policy

The separation of explanation, reference and task-oriented writing adapts
[Django's documentation organization](https://docs.djangoproject.com/en/6.0/#how-the-documentation-is-organized)
and [Diataxis](https://diataxis.fr/), not their site infrastructure. Contract entries
are informed by [OpenTelemetry's API specification](https://github.com/open-telemetry/opentelemetry-specification/blob/cda67786e931171a4d1d7bda2085e75e2f8ee41f/specification/trace/api.md)
and [raft's caller-oriented package documentation](https://github.com/etcd-io/raft/blob/cdd290a4482584c1dbefec5591614d02943a16af/doc.go).
`interface.md` is Fathomry's convention, not a Go requirement. Symbol documentation
continues to follow [Go Doc Comments](https://go.dev/doc/comment).
