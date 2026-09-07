#!/usr/bin/env bash
# fathomry
# Copyright (C) 2026  Frost Leo
# SPDX-License-Identifier: GPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -euo pipefail
umask 077

fail() { printf 'new-issue: %s\n' "$*" >&2; exit 1; }
usage() {
  printf '%s\n' \
    'Usage: new-issue.sh [--reference DIR] ISSUE_NUMBER' \
    '       new-issue.sh [--reference DIR] --draft TOPIC' \
    '' \
    'Create local issue material, never a GitHub issue.' \
    'Requires Bash, GNU coreutils, jq, and gh for published issues.' \
    'The reference workspace must already exist. Existing files are never overwritten.'
}

script_path=$(readlink -f -- "${BASH_SOURCE[0]}")
repo_root=$(cd -- "$(dirname -- "$script_path")/../../../.." && pwd -P)
reference="$(dirname -- "$repo_root")/reference"
kind=issue
number=
slug=
while (($#)); do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --reference)
      (($# >= 2)) || fail '--reference needs a directory'
      reference=$2; shift 2 ;;
    --draft)
      (($# >= 2)) || fail '--draft needs a topic'
      [[ -z "$slug" && -z "$number" ]] || fail 'choose one issue or draft'
      kind=draft; slug=$2; shift 2 ;;
    -*) fail "unknown option: $1" ;;
    *)
      [[ -z "$number" && -z "$slug" ]] || fail 'choose one issue or draft'
      number=$1; shift ;;
  esac
done
if [[ "$kind" == draft ]]; then
  [[ "$slug" =~ ^[a-z0-9]+(-[a-z0-9]+)*$ && ${#slug} -le 64 ]] || fail 'use a lowercase, hyphenated draft topic (maximum 64 characters)'
else
  [[ "$number" =~ ^[1-9][0-9]*$ ]] || fail 'use an actual positive issue number, without leading zeroes'
fi
command -v jq >/dev/null || fail 'jq is required'
[[ -d "$reference" && ! -L "$reference" ]] || fail 'reference must be an existing, non-symlink directory'
reference=$(cd -- "$reference" && pwd -P)
[[ "$reference" != "$repo_root" && "$reference" != "$repo_root/"* ]] || fail 'reference must stay outside the product repository'
[[ -f "$reference/README.md" ]] || fail 'reference/README.md is missing'
grep -Fq 'fathomry.reference/v1' "$reference/README.md" || fail 'unsupported reference layout'
policy="$repo_root/.github/repository-policy.json"
license_file="$repo_root/.github/LICENSE_HEADER"
repository=$(jq -er '.repository | select(type == "string")' "$policy")
[[ "$repository" =~ ^[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+$ ]] || fail 'invalid repository identity in policy'
repository_id=null
issue=null
draft_slug=null
if [[ "$kind" == issue ]]; then
  command -v gh >/dev/null || fail 'gh is required for published issues'
  remote=$(gh api --hostname github.com --method GET "repos/$repository")
  repository_id=$(jq -er --arg name "$repository" \
    'select(.full_name == $name) | .id | select(type == "number" and . > 0 and floor == .)' <<<"$remote") || fail 'repository identity mismatch'
  remote=$(gh api --hostname github.com --method GET "repos/$repository/issues/$number")
  issue=$(jq -ec --arg number "$number" --arg url "https://github.com/$repository/issues/$number" '
    select(.pull_request == null and (.number | tostring) == $number and .html_url == $url)
    | select(.id | type == "number" and . > 0 and floor == .)
    | {number, id, url: .html_url}' <<<"$remote") || fail 'expected an issue, not a PR or another identity'
  title=$(jq -er '.title | select(type == "string") | gsub("[\r\n]"; " ")' <<<"$remote")
  parent="$reference/issues"
  folder="gh-$number"
  title="$folder: $title"
else
  draft_slug=$(jq -cn --arg slug "$slug" '$slug')
  title="Draft: $slug"
  parent="$reference/drafts"
  folder=$slug
fi
metadata=$(jq -n --arg kind "$kind" --arg name "$repository" \
  --argjson repository_id "$repository_id" --argjson issue "$issue" \
  --argjson slug "$draft_slug" --arg created "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --rawfile notice "$license_file" '{
    schema: "fathomry.issue-reference/v1", kind: $kind,
    repository: {name: $name, id: $repository_id}, issue: $issue,
    draft_slug: $slug, created_at: $created,
    license: "GPL-3.0-or-later", license_notice: ($notice | rtrimstr("\n") | split("\n"))
  }')
[[ ! -L "$parent" ]] || fail 'refusing a symlinked issue/draft parent'
mkdir -p -- "$parent"
target="$parent/$folder"
if [[ -e "$target" || -L "$target" ]]; then
  [[ -d "$target" && ! -L "$target" ]] || fail 'existing target is not a regular directory'
  for file in README.md discussion.md design.md verification.md issue.json; do
    [[ -f "$target/$file" && ! -L "$target/$file" ]] || fail 'existing folder is unmanaged or incomplete; inspect it without overwriting'
  done
  jq -e --argjson expected "$metadata" '
    .schema == $expected.schema and .kind == $expected.kind
    and .repository == $expected.repository and .issue == $expected.issue
    and .draft_slug == $expected.draft_slug' "$target/issue.json" >/dev/null || fail 'existing folder belongs to another identity or schema'
  printf 'Preserved existing scaffold: %s\n' "$target"
  exit 0
fi

staging=$(mktemp -d "$parent/.new-issue.XXXXXX")
cleanup() {
  if [[ -n "$staging" && -d "$staging" ]]; then rm -r -- "$staging"; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
markdown_header() {
  printf '%s\n' '<!--'
  cat -- "$license_file"
  printf '%s\n\n' '-->'
}
printf '%s\n' "$metadata" > "$staging/issue.json"
{
  markdown_header
  printf '# %s\n\n## Identity\n\n' "$title"
  printf '%s\n' "$metadata" | jq -r '
    "- Repository: `\(.repository.name)`; ID: `\(.repository.id // "not yet bound")`.",
    (if .issue then "- Issue: [#\(.issue.number)](\(.issue.url)); ID: `\(.issue.id)`."
     else "- Issue: not published; this draft does not authorize implementation." end)'
  cat <<'MARKDOWN'

## Scope and handoff

- Outcome/non-goals: confirm with the owner; use the linked issue as the approved scope.
- State: material scaffold only, implementation not started.
- Branch/HEAD/PR: not recorded; inspect the actual worktree before continuing.
- Evidence: no checks have been run by this scaffolder.
- Open decisions: record only what affects this task.
- Next action: inspect existing requirements and discuss the bounded outcome.

## Material map

- [Discussion](discussion.md): owner decisions and unresolved questions.
- [Design](design.md): alternatives, contracts, and rationale.
- [Verification](verification.md): commands, observations, and limitations.
- [Workspace index](../../README.md): shared requirements, research, and SDK sources.

Keep this entry compact. `issue.json` is identity metadata, not live GitHub status.
MARKDOWN
} > "$staging/README.md"
{
  markdown_header
  cat <<'MARKDOWN'
# Discussion

Status: not recorded. Distinguish owner decisions from proposals and hypotheses.
Record the outcome, constraints, non-goals, open questions, and approval to publish.
Do not treat this generated file or an existing issue number as implementation approval.
MARKDOWN
} > "$staging/discussion.md"
{
  markdown_header
  cat <<'MARKDOWN'
# Design

Status: proposed, not accepted. Record the necessary contracts and ownership,
material alternatives, exact source revisions, compatibility and failure modes.
Reference shared SDKs instead of copying them. Publish only accepted architecture
or established API material into the product's docs directory.
MARKDOWN
} > "$staging/design.md"
{
  markdown_header
  cat <<'MARKDOWN'
# Verification

Status: not run. Record acceptance checks, tested commit/worktree state, exact
commands and results, edge/failure cases, and what remains unverified. Scaffold
creation is not evidence that implementation, testing, or review has completed.
MARKDOWN
} > "$staging/verification.md"
# Publish a complete directory without replacing a concurrent creator's target.
mv -Tn -- "$staging" "$target"
[[ ! -d "$staging" ]] || fail 'target appeared concurrently; existing work was preserved'
staging=
printf 'Created local scaffold: %s\nNo GitHub objects were created or modified.\n' "$target"
