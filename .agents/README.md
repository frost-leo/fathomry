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

# Agent Resources

`skills/fathomry-development` is the first-party session workflow: implement one
approved issue, then discuss and prepare one next issue with the owner. Its source
is versioned with this repository; it is not an upstream-vendored lock entry.
Its `scripts/new-issue.sh` helper generates local reference folders. A relative
symlink under `reference/scripts` provides the workspace entry; no second source
copy is maintained. Its fixture-based tests run in the repository CI check.

The other `skills/` directories contain pinned official guidance, not application
runtime dependencies. `skills.lock.json` records their provenance and content hashes.
Upstream MIT and Apache-2.0 notices are preserved; the project license does not
replace them. Candidate-service research lives outside the repository, in the
sibling workspace's `reference/research/service-skills.md`; it is not an installation
manifest or a requirement to load every provider guide.

The Temporal entry point has one documented compatibility adjustment: its
top-level `version` field is moved to `metadata.version` for Codex's skill schema.
The guidance itself is unchanged. Redis license files are copied from their
upstream repository root into each redistributed skill. These adjustments are
recorded explicitly rather than hidden behind an unmodified provenance claim.
The MinIO entry point moves `compatibility` into `metadata.compatibility` to pass
the installed Codex skill validator; its operational guidance is unchanged.

- **temporal-developer**: Temporal SDK/workflow/activity development and replay.
  Fathomry uses Go: load the Go and relevant core references rather than every
  language-specific guide.
- **redis-core**: Redis data structures, commands, and modeling fundamentals.
- **redis-connections**: Client connection, pooling, retries, and timeout behavior.
- **redis-clustering**: Cluster topology, routing, and deployment constraints.
- **redis-observability**: Redis operational signals and diagnosis.
- **redis-security**: Authentication, TLS, authorization, and deployment hardening.
- **mc**: MinIO's S3-compatible CLI guidance, only for explicitly scoped object-store
  diagnosis or administration. It is not a `minio-go` programming guide and does
  not select AIStor-only capabilities for a community MinIO deployment.

These skills are advisory evidence. Verify actual APIs and behavior against the
project's selected SDK/service versions. Installing a skill does not authorize
production changes, publishing, secret access, or new dependencies. Follow the
project's `AGENTS.md` and the user's current scope.

Review updates as dependency changes: compare upstream revisions, instructions,
licenses, and relevant examples; retain provenance and run the appropriate checks.
Do not quietly mix modified files with an unchanged upstream revision marker.

Do not install every skill in a vendor's collection. Review trigger scope, shell
commands, credential handling, outside services, bundled files, language/SDK fit,
and cross-skill prerequisites. A skill must not select a service architecture,
copy another project's contribution workflow, or upload conversation memory.
MCP servers and CLI executables are separate installations and authorizations.

For each lock entry, `tree_sha256` hashes the concatenation of sorted regular
files: UTF-8 relative POSIX path, NUL byte, file bytes, NUL byte. Paths are relative
to that skill's directory; the included upstream license and documented local
adjustments participate in the hash. Symlinks are not permitted.

Repository-local skills are discoverable under `.agents/skills`; see the official
[Codex skills guide](https://developers.openai.com/codex/skills) and
[AGENTS.md guide](https://developers.openai.com/codex/guides/agents-md).
