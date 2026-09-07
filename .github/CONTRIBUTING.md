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

# Contributing to Fathomry

The repository is maintained by `frost-leo` and owner-authorized Codex sessions.
PR creation is restricted to write-capable collaborators. Public visibility is
not an invitation for unsolicited code submissions; outside readers may use
Issues or Discussions for non-sensitive feedback.

## Plan and discuss

Keep one independently reviewable outcome per issue and pull request. Describe
the problem, constraints, non-goals, alternatives, and observable acceptance
criteria before implementing a substantial change. Architectural proposals need
evidence from the exact dependency versions and representative workloads.

Keep unconfirmed future work in planning notes or Discussions. Create an
implementation issue only when the owner confirms its scope and intends to
schedule it. Do not bulk-create a roadmap backlog, milestone, priority, or `ready`
state from repository-setup authorization. Record genuine reported defects as
triage issues without implying that their implementation has been approved.

One session implements one previously confirmed issue. After verification, discuss
one next outcome with the owner, prepare its reference materials, and publish that
issue only after scope approval. Leave its implementation to the next session.
If the current issue is incomplete, hand off that issue instead of opening another.

Questions belong in Discussions. Bugs and concrete work belong in Issues.
Never include credentials, private endpoints, production payloads, or customer
data in examples, logs, screenshots, or reports.

## Branches and pull requests

Before the first release, `develop` is the default branch and `main` does not
exist. Bootstrap it with one signed, file-free root commit only. Put every actual
file, including repository initialization, on a topic branch and open a PR to
`develop`. No later content is pushed directly to either integration/release branch.

At the first owner-approved release, create `main` at that same empty root, open
the release PR from `develop` to `main`, and merge it with a merge commit after
verification. Then switch the default branch to `main` and update repository
policy as part of the release. Do not create `main` at an unreleased content commit.

| Branch | Purpose | Base and merge target |
| --- | --- | --- |
| `main` | Releases only; absent before the first release | Receives approved release PRs from `develop`, or a release-scoped urgent hotfix |
| `develop` | Active integration | Receives ordinary topic PRs and synchronization from `main` |
| `feat/<slug>` | New behavior | `develop` |
| `fix/<slug>` | Bug correction | `develop` |
| `refactor/<slug>` | Structural change without intended behavior changes | `develop` |
| `docs/<slug>` | Documentation | `develop` |
| `test/<slug>` | Tests and verification | `develop` |
| `perf/<slug>` | Measured performance improvement | `develop` |
| `chore/<slug>` or `ci/<slug>` | Maintenance and automation | `develop` |
| `hotfix/<slug>` | Urgent stable-line fix | Branch from `main`; PR to `main`, then synchronize into `develop` |

Use lowercase, hyphenated slugs; include an issue number when useful, for
example `feat/42-client-lifecycle`. These are naming prefixes, not permanent
branches to create in advance. Autonomous dependency-update PRs are not enabled.

1. Branch from the appropriate up-to-date base.
2. Open a draft PR early for substantial work and use the PR template.
3. Include verification evidence and resolve review conversations.
4. **Squash-merge short-lived topic branches into `develop`.**
5. **Use merge commits for `develop` → `main` promotion and `main` → `develop`
   synchronization.** Do not squash one long-lived branch into the other: retain
   their shared ancestry. Rebase merging is disabled.
6. Delete merged topic branches. Never delete `main` or `develop`.

Do not force-push shared long-lived branches. Rewriting your own topic branch
requires coordination with its reviewers; use `--force-with-lease`, not blind
force pushes. Never publish secrets or unrelated local work.

PR titles and commit subjects identify the actual related GitHub issue, for example:

```text
gh-42 Add project initialization
gh-51 Preserve conditional-write outcomes
gh-63 Separate database provider ownership
```

Use this commit body structure and an actual, existing issue number:

```text
gh-42 Add project initialization

Generate a standard Go project with explicit configuration and version boundaries.

- Add the minimal project skeleton and module declaration.
- Verify generated files and document the supported configuration layers.

Refs #42
```

Use the `gh-N Title` subject consistently; do not mix unrelated title conventions.
Do not invent issue numbers. Every project-authored commit must be cryptographically
signed and verifiable on GitHub. A `Signed-off-by` footer is not a signature.
Configure `commit.gpgsign=true` locally and use `.github/commit-message.txt` as the
commit template. Never silently fall back to unsigned commits if a key is locked.

Signature verification is enforced by GitHub. Message structure and actual issue
linkage are checked by the maintainer. Do not assume the template is a server gate.

Record the work type in labels, not an additional conflicting commit-title scheme.
Maintainers edit the final squash message to retain a summary, concrete details,
and `Refs #N`; do not dump every temporary commit message into the permanent history.

Use `Refs #123` for PRs targeting `develop`. GitHub closing keywords are tied to
the default branch; do not assume they close an issue on a non-default-branch
merge. Maintainers close an implementation issue after its PR is merged into
`develop` and acceptance criteria are met. They do not wait for a `main` release;
release availability is tracked separately. Use explicit closure regardless of
which branch is currently the default.

## Review and checks

Both long-lived branches require a PR, current passing `Repository checks` and
`Go checks`, and resolved conversations. Force pushes and deletion are blocked,
including for administrators. See [repository policy](repository-policy.json).

The initial repository has one human maintainer. Required approval count is zero
so an author is not blocked waiting for a self-approval GitHub does not permit.
CODEOWNERS still routes review requests. Raise the requirement to at least one
independent approval when a second write-capable maintainer is available.

CI checks repository whitespace and required assets immediately. Once application
code introduces `go.mod`, the stable `Go checks` job uses standard Go formatting,
module-tidiness, vet, race-enabled test, and build commands. Until then it explicitly
reports that application checks are not applicable.

```bash
git diff --check
# Run after a Go module and application packages exist:
go mod tidy -diff
go vet ./...
go test -race ./...
go build ./...
```

External-service tests must be opt-in, use isolated development resources, and
clean up their own writes. No production credentials are made available to PR CI.

## Engineering expectations

[AGENTS.md](../AGENTS.md) defines the project's reasoning and Go standards.
The `fathomry-development` skill defines the session and reference-material workflow;
do not maintain competing copies of those instructions here. Draft material stays
in the sibling `reference` workspace. Publish established API and accepted
architecture documentation in `docs/`, with truthful implementation status.

## Labels

The canonical label catalog is [`labels.json`](labels.json).
Maintainers choose one `type:*`, one `priority:*`, and one active `status:*` when
triaging; `area:*` and `concern:*` labels may be combined. Closing an issue is its completion
state—there is no redundant `status: done` label.

Label changes require a reviewed catalog update and a deliberate remote sync.
Routine automation does not delete/recreate labels or mutate repository policy.

GitHub's native Issue Types are organization-level; this personal repository uses
the explicit `type:*` labels and issue forms. They are not presented as native types.

## Preserve remote context

An issue records scope, prerequisites, acceptance criteria, and unresolved risks.
A PR records the selected implementation and actual evidence. A commit records
the applied change. At checkpoints, update the issue with what changed, exact
verification, remaining work, and the next action. Move durable decisions into
the appropriate published architecture document, and link the issue/PR. Unaccepted
designs stay in the issue's reference folder. Chat history is not the project database.

Use native issue dependencies plus explicit `Depends on #N` links for ordered work.
Do not begin a dependent issue merely because its label says ready if its recorded
prerequisite is still open. Do not close an issue while required work remains.
