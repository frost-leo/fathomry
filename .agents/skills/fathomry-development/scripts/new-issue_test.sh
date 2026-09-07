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
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
helper="$script_dir/new-issue.sh"
repo_root=$(cd -- "$script_dir/../../../.." && pwd -P)
temporary=$(mktemp -d)
trap 'rm -r -- "$temporary"' EXIT
mkdir "$temporary/bin"
export PATH="$temporary/bin:$PATH"
export TEST_REPOSITORY
TEST_REPOSITORY=$(jq -r .repository "$repo_root/.github/repository-policy.json")
export TEST_REPOSITORY_ID=123 TEST_ISSUE_ID=700 TEST_MODE=normal
export TEST_LOG="$temporary/requests.log" TEST_TITLE='Safe title'
: > "$TEST_LOG"
cat > "$temporary/bin/gh" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
[[ $# == 6 && $1 == api && $2 == --hostname && $3 == github.com && $4 == --method && $5 == GET ]] || exit 97
printf '%s\n' "$6" >> "$TEST_LOG"
[[ "$TEST_MODE" != offline ]] || exit 1
if [[ "$6" == "repos/$TEST_REPOSITORY" ]]; then
  name=$TEST_REPOSITORY
  [[ "$TEST_MODE" != redirect ]] || name=another/repository
  jq -cn --arg name "$name" --argjson id "$TEST_REPOSITORY_ID" '{full_name:$name,id:$id}'
else
  number=${6##*/}
  [[ "$TEST_MODE" != wrong-number ]] || number=999
  jq -cn --arg repo "$TEST_REPOSITORY" --argjson number "$number" \
    --argjson id "$TEST_ISSUE_ID" --arg title "$TEST_TITLE" --arg mode "$TEST_MODE" \
    '{id:$id,number:$number,title:$title,html_url:("https://github.com/"+$repo+"/issues/"+($number|tostring))}
     + (if $mode == "pr" then {pull_request:{url:"example"}} else {} end)'
fi
MOCK
chmod +x "$temporary/bin/gh"
checks=0
pass() { checks=$((checks + 1)); printf 'ok %d - %s\n' "$checks" "$1"; }
reference() {
  mkdir -p -- "$1"
  printf 'Layout contract: fathomry.reference/v1\n' > "$1/README.md"
}
reject() {
  if "$helper" "$@" > "$temporary/rejected.log" 2>&1; then
    printf 'Unexpected success: %s\n' "$*" >&2; exit 1
  fi
}

"$helper" --help >/dev/null
for value in 0 01 -1 ../escape 1/2; do reject "$value"; done
reject --draft ../escape
reject --draft Mixed_Case
reject --draft
reject --reference
reject --draft topic 7
pass 'invalid arguments rejected before any writes'

ref="$temporary/space reference"
reference "$ref"
TEST_MODE=offline "$helper" --reference "$ref" --draft scope > /dev/null
[[ ! -s "$TEST_LOG" ]]
folder="$ref/drafts/scope"
[[ $(find "$folder" -maxdepth 1 -type f | wc -l) == 5 ]]
jq -e '.kind == "draft" and .issue == null and .repository.id == null and .draft_slug == "scope"' "$folder/issue.json" >/dev/null
pass 'offline draft produces all five files without GitHub calls'

printf '\nOwner note: preserve this.\n' >> "$folder/design.md"
find "$folder" -type f -exec sha256sum {} + | sort > "$temporary/before"
"$helper" --reference "$ref" --draft scope > /dev/null
find "$folder" -type f -exec sha256sum {} + | sort > "$temporary/after"
cmp "$temporary/before" "$temporary/after"
pass 'repeat preserves every existing byte and timestamp metadata'

TEST_TITLE="\$(touch $temporary/injected)" "$helper" --reference "$ref" 7 > /dev/null
folder="$ref/issues/gh-7"
[[ ! -e "$temporary/injected" ]]
grep -Fq "\$(touch " "$folder/README.md"
jq -e '.schema == "fathomry.issue-reference/v1" and .repository.id == 123 and .issue.id == 700 and .issue.number == 7 and .draft_slug == null' "$folder/issue.json" >/dev/null
pass 'published issue identity validated; title treated as inert data'

find "$folder" -type f -exec sha256sum {} + | sort > "$temporary/before"
"$helper" --reference "$ref" 7 > /dev/null
TEST_REPOSITORY_ID=124 reject --reference "$ref" 7
TEST_ISSUE_ID=701 reject --reference "$ref" 7
find "$folder" -type f -exec sha256sum {} + | sort > "$temporary/after"
cmp "$temporary/before" "$temporary/after"
pass 'same identity is idempotent; changed repository/issue IDs cannot overwrite'

for mode in offline redirect pr wrong-number; do
  isolated="$temporary/$mode"
  reference "$isolated"
  TEST_MODE=$mode reject --reference "$isolated" 8
  [[ ! -e "$isolated/issues" ]]
done
pass 'API failure, redirect, PR, and wrong number leave no issue directory'

mkdir -p "$ref/issues/gh-9"
printf 'Existing material\n' > "$ref/issues/gh-9/README.md"
reject --reference "$ref" 9
[[ $(cat "$ref/issues/gh-9/README.md") == 'Existing material' ]]
pass 'unmanaged material is preserved, not adopted or overwritten'

ln -s "$folder" "$ref/issues/gh-10"
reject --reference "$ref" 10
isolated="$temporary/symlinks"
reference "$isolated"
ln -s "$ref/issues" "$isolated/issues"
reject --reference "$isolated" 11
ln -s "$ref" "$temporary/reference-link"
reject --reference "$temporary/reference-link" --draft another
reject --reference "$repo_root" --draft another
pass 'symlink targets/parents and product-tree destinations rejected'

rm -- "$folder/verification.md"
reject --reference "$ref" 7
[[ ! -e "$folder/verification.md" ]]
pass 'incomplete existing scaffold is reported without fabricating replacement evidence'

isolated="$temporary/race"
reference "$isolated"
"$helper" --reference "$isolated" --draft shared > "$temporary/race-one.log" 2>&1 & first=$!
"$helper" --reference "$isolated" --draft shared > "$temporary/race-two.log" 2>&1 & second=$!
first_status=0; second_status=0
wait "$first" || first_status=$?
wait "$second" || second_status=$?
[[ $first_status == 0 || $second_status == 0 ]]
[[ $(find "$isolated/drafts/shared" -maxdepth 1 -type f | wc -l) == 5 ]]
[[ -z $(find "$isolated" -name '.new-issue.*' -print) ]]
pass 'concurrent creation publishes one complete scaffold and cleans staging'

cat > "$temporary/bin/mv" <<'MOCK'
#!/usr/bin/env bash
exit 1
MOCK
chmod +x "$temporary/bin/mv"
reject --reference "$ref" --draft failed-publication
[[ ! -e "$ref/drafts/failed-publication" ]]
[[ -z $(find "$ref" -name '.new-issue.*' -print) ]]
pass 'publication failure exposes no partial target and removes temporary files'

printf 'PASS: %d local scaffold scenarios; GitHub calls used a read-only fixture.\n' "$checks"
