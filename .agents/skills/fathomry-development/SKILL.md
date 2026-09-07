---
name: fathomry-development
description: Continue Fathomry project discussion or one approved GitHub issue, locate its sibling reference materials, implement and verify it, then prepare one owner-approved next issue for another session. Use for Fathomry session onboarding, development, review, and handoff; not for unrelated repositories.
license: GPL-3.0-or-later
---
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

# Fathomry Session Workflow

## Enter without loading the whole project

1. Identify the current request: discussion/preparation, implementation, review, or
   handoff. A question about a possible change is not implementation approval.
2. Resolve the product Git root and its sibling `reference` directory. Read only
   `reference/README.md`, then the chosen `issues/gh-N/README.md` if present. The
   reference layout contract is `fathomry.reference/v1`. Do not crawl the entire lab.
3. Check the working branch, HEAD, and uncommitted changes. Read the actual selected
   GitHub issue's scope and latest handoff, plus an associated PR if needed. Verify
   repository ID, issue ID, and commit against local notes; a reused number or stale
   summary must not attach old work to a new repository.
4. Treat the issue/PR as the approved work record, local reference material as
   evidence, and source/tests/runtime as facts. Load only linked requirement sections,
   design passages, and exact SDK files needed for the current question. A missing
   reference folder is not permission to reconstruct private context or alter scope;
   report the missing material and continue only work that does not depend on it.

## Implement one issue

`develop` is the default before the first release. The sole direct bootstrap push
is a signed empty root commit on `develop`; all actual files, including setup,
enter through a topic PR to `develop`. Do not initialize `main` with content.
Only for the first approved release, create `main` at the empty root, merge the
`develop` release PR using a merge commit, and switch the default branch to `main`.
Afterward `main` accepts release work only. See `.github/CONTRIBUTING.md` for details.

- Confirm the owner's chosen outcome and real prerequisites. Do not auto-select a
  `ready` issue, invent acceptance criteria, or open future issues from a roadmap.
- Keep the work record in `reference/issues/gh-N/`. Use the scaffold helper described
  below for new folders; preserve existing material. Link shared requirements and
  SDKs rather than copy them. `README.md` stays the short entry, not a transcript.
- Apply `AGENTS.md` reasoning and Go standards. Trace callers and the selected SDK
  before wrapping it; inspect only relevant provider skills. Keep experiments outside
  the product tree unless they are maintainable tests or implementation assets.
- Use the branch/commit/PR policy in `.github/CONTRIBUTING.md` when entering that
  phase. Read `.github/commit-message.txt` and `.github/workflows/checks.yml` when
  needed instead of maintaining another copy of their rules here.
- Validate the observable outcome, not just a plausible diff. Record exact commands,
  tested commit/worktree state, results, unsupported cases, and relevant failure or
  performance evidence. Do not call absent/skipped application tests successful tests.
- Publish documentation into `docs/` only when it describes an established API or an
  owner-accepted architecture, with implementation status and compatibility explicit.
  Preserve the design's rationale and issue/evidence links; leave proposals in reference.
- Commit/sign, push, or merge only within authorization and repository protections.
  Complete an implementation issue after its PR is merged into `develop` and its
  acceptance gates pass; do not wait for a `main` release to prepare the next issue.

## Generate local issue material

The versioned helper is `scripts/new-issue.sh` beside this skill. The reference
workspace can expose it as `reference/scripts/new-issue.sh` using a relative symlink.
Run `--help` for usage; Bash, coreutils, and jq are required, plus gh for published
issues. It only reads GitHub and writes local material; it never publishes an issue.

- `reference/scripts/new-issue.sh --draft <topic>` prepares an offline draft.
- `reference/scripts/new-issue.sh <actual-number>` verifies GitHub identity and
  prepares `README.md`, discussion/design/verification templates, and `issue.json`.

The metadata schema is `fathomry.issue-reference/v1`. Repeating the same request
preserves files; an unmanaged folder or changed repository/issue identity is an
error, not permission to overwrite. For an already-prepared draft, preserve its
content when linking it to the published issue; do not replace it with a new scaffold.

## Discuss and prepare exactly one next issue

After the current issue is complete, discuss the next outcome with the owner. Do
not start its implementation in this session. Preparation can gather requirements,
SDK source, alternatives, risks, and acceptance experiments for that one outcome.

Keep unpublished material in `reference/drafts/<short-topic>/README.md`, without
inventing an issue number. Prepare a bounded outcome, non-goals, acceptance checks,
real prerequisites, source revisions, unresolved decisions, and a concrete starting
point. Drafts and successful predecessor tests do not imply owner approval.

When the owner confirms the scope and publication, create or reuse exactly one
matching issue. First inspect for an existing publication; if a write times out,
read back before retrying so a lost acknowledgement cannot create duplicates.
Use GitHub's returned number and ID, move the draft to `issues/gh-N/` without
clobbering another folder, and update links and `issue.json`: bind the actual
repository/issue IDs, set `kind` to `issue`, and clear `draft_slug` to null while
preserving preparation files. Leave implementation for the next session. Never
bulk-create milestones, priorities, or a chain of speculative tasks.

The public issue must contain a self-contained, sanitized task summary and useful
upstream source/revision links. Local filesystem links are not remotely accessible.
Do not upload private reference material or publish another repository implicitly;
state any material that the owner must separately provide on another machine.

## Leave a compact handoff

Maintain one current summary in the issue body (or an explicitly identified latest
handoff comment) and mirror the pointer/state in the issue folder's `README.md`:

- Identity: repository/issue IDs, issue URL, branch, HEAD, PR, and dirty-file status.
- Scope: approved outcome and non-goals; completion or remaining acceptance gates.
- Evidence: verified facts and commands, with the revision they apply to.
- Uncertainty: hypotheses, blockers, unsupported cases, and decisions still needed.
- Next action: one concrete step; same issue if unfinished, next issue if approved.
- Material map: only the relevant requirement, design, SDK, and evidence paths.

Prefer roughly one screen of summary; link details instead of pasting transcripts
or every log. Never include secrets. On resume, verify the summary against current
state before trusting it. If the current issue is unfinished or the owner has not
approved the next one, stop with that fact rather than manufacture a new issue.
