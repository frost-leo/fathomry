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

# Fathomry

A Go framework for durable, observable data workflows powered by Temporal.

## Direction

Fathomry focuses on explicit contracts, cohesive infrastructure clients,
predictable resource ownership, and durable workflow execution. Reliability,
observability, bounded resource use, and measured performance guide development.

The project is in early development. The [named-source foundation](docs/api/named-sources.md)
provides typed configuration preparation, explicit resource ownership, and a shared
public error kernel. The [controlled-call foundation](docs/api/controlled-calls.md)
adds process-local admission, completion tracking and independent technical-evidence
handoff. Concrete SDK integrations and workflow orchestration are not
implemented. Repository automation and contribution guidelines are available.

## Participate

- Read [AGENTS.md](AGENTS.md) and the
  [maintainer workflow](.github/CONTRIBUTING.md) before changing the repository.
- Use [Issues](https://github.com/frost-leo/fathomry/issues/new/choose) for
  actionable bugs, features, research proposals, and implementation tasks.
- Use [Discussions](https://github.com/frost-leo/fathomry/discussions) for questions
  and exploratory conversations.
- Follow [SECURITY.md](.github/SECURITY.md) for sensitive reports.

Maintenance is owner-led with Codex assistance. Only write-capable collaborators
can create PRs; public readers can inspect the project and share non-sensitive
feedback through Issues or Discussions. See [AGENTS.md](AGENTS.md) for session
entry points and [.agents/README.md](.agents/README.md) for pinned skill provenance.

`develop` is the integration and default branch before the first release. Every
file change, including initialization, enters it through a topic PR. The only
direct bootstrap push is a signed empty root commit. `main` is created from that
empty root for the first approved release PR and receives releases only; it then
becomes the default branch.

## Development workspace

The working layout is `lab/fathomry` for the product repository and `lab/reference`
for requirements, per-issue preparation, discussions, draft designs, and pinned
SDK source. Reference materials are workspace-local and do not arrive with a
Git clone; the product build must not depend on them. Public Issues retain enough
approved scope and sanitized evidence to identify missing handoff material.

Use the repository's `fathomry-development` skill to continue one confirmed issue,
then prepare one next issue with the owner. `docs/` is reserved for established
APIs and accepted architecture. The
[internal architecture and SDK integration standard](docs/architecture/internal-sdk-integration.md)
defines the technical foundation, responsibility boundaries, and integration
acceptance requirements. It does not claim that the runtime or SDK integrations
are implemented. The [named-source API](docs/api/named-sources.md) records the
implemented configuration/lifecycle boundary and its remaining integration obligations.
The [controlled-call API](docs/api/controlled-calls.md) documents the call/result
contracts and their local-only, non-durable limits.

## License

GNU General Public License v3.0 or later. See [LICENSE](LICENSE).
