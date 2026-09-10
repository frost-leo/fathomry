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

# Versions and compatibility

[Documentation](../README.md) / Architecture

**Audience:** framework and integration maintainers.
**Status:** accepted architecture; implementation coverage is limited.

This topic states accepted cross-package obligations, not a claim that every
SDK or framework feature is implemented. See the [documentation map](../README.md)
for current availability and package contracts.

**Standard sections:** S08.

## Contents

- [Version owners and evidence](#version-owners-and-evidence)

## Version owners and evidence

| Version or identity axis | Accountable owner and required distinction |
| --- | --- |
| Framework module and internal contracts | Framework maintainers; document behavior changes and test integrations without assuming independent releases for every private package |
| Public capability/error/correlation contracts | Public contract maintainers; preserve or explicitly change identity, cause inspection, defaults, and result/guarantee semantics |
| Configuration schema | Format and capability maintainers; define supported formats, field meanings, defaults, and handling of unknown or incompatible input |
| Effective source-configuration revision | Composition; identify the settings used by that source without confusing them with the schema version or exposing secrets |
| Actual SDK and Go build | Build evidence plus Provider requirements; distinguish dependency declarations and reference/test versions from what the consuming binary actually contains |
| Service, protocol, and SDK mode | Provider evidence and deployment selection; record observed values, enabled features, critical options, and what was or was not verified |
| Public/persisted results, messages, objects, and error formats | Respective contract/format owner; specify historical interpretation, missing/zero/unknown values, compatibility, and migration or rejection |
| Workflow and mode versions | Framework and workflow owners; compatibility and frozen execution selection are not inferred from an SDK upgrade |
| Deployment/build correspondence | Deployment and framework execution owners; runtime provenance informs, but does not implement, code-consistency validation or Temporal routing |
| Business schema | Business contract owner declares fields and write rules; technical capability validates support; automatic creation/migration is not implied |

A framework dependency declaration does not lock every consuming project's SDK
selection. Main-module or workspace requirements and replacements affect the Go
build list. Record this distinction rather than treating `go.sum` as the consuming
binary's exact dependency selection. [Go module version selection](https://go.dev/ref/mod#minimal-version-selection)

Actual build evidence must distinguish the main application from the framework
and SDK dependencies. Missing metadata, local replacements, and native-library
provenance remain explicit, with sensitive local paths redacted. Build metadata
describes provenance; it is not an integrity attestation or a compatibility test.
[Go build information](https://pkg.go.dev/runtime/debug#BuildInfo)

Record **tested support**, **observed build/runtime facts**, and **unknown or
unsupported combinations** separately. State the complete tested combination,
including behavior-affecting modes and options. A version string or successful
connection alone is not acceptance evidence. Changing defaults, error identity,
result meaning, or execution guarantees requires an explicit compatibility decision
even when function signatures are unchanged.

Known unsupported required guarantees must be rejected, not silently weakened.
The deployment response to untested or unknown combinations remains open and must
be explicit before use; neither blanket acceptance nor blanket rejection is selected
here. Durable readers must not reinterpret unknown versions/values as known success.
Define historical-reader and migration/refusal policy before publishing such a
format; no universal version registry or migration engine is required.

## Implementation references

[Build facts](../reference/internal/compatibility/build-info.md) and [assessment](../reference/internal/compatibility/assessment.md).

This topic carries forward the [accepted integration standard](internal-sdk-integration.md)
from [Issue #3](https://github.com/frost-leo/fathomry/issues/3), including the
[Issue #15](https://github.com/frost-leo/fathomry/issues/15) boundary refinements.
