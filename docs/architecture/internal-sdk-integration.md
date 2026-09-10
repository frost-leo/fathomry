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

# Internal Architecture and SDK Integration Standard

[Documentation](../README.md) / Architecture

**Audience:** framework and integration maintainers.
**Status:** accepted standard index; topic pages own section bodies.

This is the navigation and stable-section index of the accepted integration
standard, not a duplicate of its normative text. Topic pages own the rules.
Existing S01-S12 identifiers and section anchors remain so earlier references
still lead to the applicable topic. Cite the repository revision and section IDs
when recording an integration baseline, separately from SDK/configuration/wire versions.

The original accepted revision is `0c3f9d82947229a861a30fbf9d1730fa02ca44ea`
([Issue #3](https://github.com/frost-leo/fathomry/issues/3)). Later accepted changes,
including [Issue #15](https://github.com/frost-leo/fathomry/issues/15), refine the
current boundary. Commit-pinned historical references retain their original text.

“Must” states an obligation for an applicable implementation, not evidence that
service integrations or the full framework runtime exist. Shared mechanisms are
implemented; public capabilities/errors, loading/CLI and durable orchestration
remain absent. Start with the [documentation map](../README.md) for the actual
subset. Private research is supporting evidence, not a product dependency.

## Topics

| Section | Canonical topic |
| --- | --- |
| S01 | [Outer roles, public paths, and dependencies](package-boundaries.md#roles-and-collaboration) |
| S02 | [Shared internal mechanisms, independent Providers](sdk-integration.md#shared-mechanisms-and-independent-providers) |
| S03 | [Named sources and outer selection](configuration.md#named-sources-and-selection) |
| S04 | [Configuration responsibilities and effective revisions](configuration.md#configuration-ownership-and-revisions) |
| S05 | [Ownership, operation lifetime, and shutdown](resource-lifecycle.md#ownership-and-termination-guarantees) |
| S06 | [Shared errors, technical results, and reliable evidence](errors-and-evidence.md#technical-errors-and-framework-meaning) |
| S07 | [Data-intensive guarantees and resource limits](resource-limits.md#guarantees-granularity-and-bounds) |
| S08 | [Version owners and compatibility](versioning.md#version-owners-and-evidence) |
| S09 | [Professional naming and cohesive organization](package-boundaries.md#cohesive-package-organization) |
| S10 | [Representative execution shapes and counterexamples](sdk-integration.md#execution-shapes-and-counterexamples) |
| S11 | [Integration acceptance and traceability](sdk-integration.md#acceptance-and-traceability) |
| S12 | [Architectural decisions and remaining implementation choices](decisions.md#decisions-and-remaining-implementation-choices) |

## S01 — Outer roles, public paths, and dependencies

The normative text is maintained in [Package boundaries and responsibilities](package-boundaries.md#roles-and-collaboration).

## S02 — Shared internal mechanisms, independent Providers

The normative text is maintained in [SDK integration architecture](sdk-integration.md#shared-mechanisms-and-independent-providers).

## S03 — Named sources and outer selection

The normative text is maintained in [Configuration and named-source selection](configuration.md#named-sources-and-selection).

## S04 — Configuration responsibilities and effective revisions

The normative text is maintained in [Configuration and named-source selection](configuration.md#configuration-ownership-and-revisions).

## S05 — Ownership, operation lifetime, and shutdown

The normative text is maintained in [Resource ownership and lifetime](resource-lifecycle.md#ownership-and-termination-guarantees).

## S06 — Shared errors, technical results, and reliable evidence

The normative text is maintained in [Errors, effects and independent evidence](errors-and-evidence.md#technical-errors-and-framework-meaning).

## S07 — Data-intensive guarantees and resource limits

The normative text is maintained in [Data guarantees and resource limits](resource-limits.md#guarantees-granularity-and-bounds).

## S08 — Version owners and compatibility

The normative text is maintained in [Versions and compatibility](versioning.md#version-owners-and-evidence).

## S09 — Professional naming and cohesive organization

The normative text is maintained in [Package boundaries and responsibilities](package-boundaries.md#cohesive-package-organization).

## S10 — Representative execution shapes and counterexamples

The normative text is maintained in [SDK integration architecture](sdk-integration.md#execution-shapes-and-counterexamples).

## S11 — Integration acceptance and traceability

The normative text is maintained in [SDK integration architecture](sdk-integration.md#acceptance-and-traceability).

## S12 — Architectural decisions and remaining implementation choices

The normative text is maintained in [Design tradeoffs and open boundaries](decisions.md#decisions-and-remaining-implementation-choices).
