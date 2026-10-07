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

# Maintain public adapter error boundaries

[Documentation](../README.md) / Development

**Audience:** maintainers of public adapters.
**Status:** implemented responsibility checks across the whole Adapter tree,
including the public mechanism, configuration preparation, Viper/Nacos and the
data providers. This is not a new error system or a prescription to invent error
catalogs for capability-only packages or private helpers.

For the surrounding configuration, policy, ownership and verification contract,
see [Public Adapter maintenance](public-adapters.md). This page owns the error
and presentation boundary, not a second lifecycle standard.

## Keep declarations separate from occurrences

The shared [failure contract](../reference/failure/v1/interface.md) owns stable
`failure.Definition` and runtime `failure.Error` / `failure.Occurrence`.
A Definition describes a class; an occurrence adds location, causes and optionally
component-owned typed details. They are complementary, not competing designs.

Within every package that owns an error catalog:

| File | Responsibility |
| --- | --- |
| `definitions.go` | Explicit numeric codes, identifiers, owners and detached offline definitions |
| `error.go` | Occurrence construction, provider-specific native mapping, phase/cause semantics |
| `diagnostics.go` | Runtime/settings formatting, slog redaction and serialization restrictions |
| `resources.go` | Embedded locale data and offline filesystem accessors |

The [tree map](../../adapters/README.md) identifies those packages. Shared
capability metadata keeps its existing serialization-refusal semantics in
diagnostics without allocating new facilities. The private bridge owns bounded
error mechanics, not a provider catalog. An explicit new operation/preparation
failure may retain its semantic outer frame; forwarding an existing occurrence
is not the same operation as constructing that intentional frame.

Public import paths, exported APIs, code meanings and locale resources do not
change when declarations move between files. Do not renumber errors, copy private
SDK errors into shared public DTOs, add ambient registration or construct a client
during metadata lookup. File separation alone does not prove absence of I/O;
keep the executable offline CLI and independent-consumer checks.

## Preserve the actual semantic boundary

A directly supplied public occurrence is already classified and stays intact.
Transparent wrappers around an entirely public graph retain its original core,
typed-detail accessor and original cause graph, while hiding wrapper text.
A heterogeneous public aggregate has no newly invented single provider owner.

An explicit native semantic frame remains meaningful: a database operation error
containing another cause does not automatically become that cause's classification.
Each provider retains its own kind-to-code table, native precedence and fallback.
Mixed/partial command facts remain in their component results; no error code is
retry, rollback, effect or business-completion policy.

The private [adapter error bridge](../reference/adapters/internal/errorbridge/interface.md)
shares bounded graph inspection and redacted forwarding only. It creates no
runtime, registry, SDK abstraction, public error facility or retry engine.
Already-public subtrees are opaque to native reclassification. Incomplete
inspection never authorizes dropping original causes or inventing success.
Already-forwarded aggregates remain classified across repeated composition;
retained original graphs are for deliberate inspection, not reclassification.
Adapter-internal deduplication must also use bounded containment rather than an
unbounded errors.Is walk over caller-owned cancellation causes.

Viper/Nacos use the same public-only forwarding precheck while retaining their
established direct-native mapping. Viper's neutral joined close-cause rule uses
bounded native-kind inspection; it must not become an unbounded graph walk or a
generic first-cause classification policy. Deliberate native error inspection and
external errors.Is/As remain separate from the Adapter's bounded internal search.

SQL primary/cleanup composition keeps primary identity and known operation
details. Wrapped details are matched to the current core, never selected from an
unrelated nested occurrence. Unknown component detail schemas remain opaque and
inspectable rather than being reclassified as a database failure.

## Verify behavior, not just layout

Use the affected packages' `error_contract_test.go`, existing error/diagnostic/
catalog tests, independent consumers, and shared conformance checks. Cover:

- bare, wrapped and joined public occurrences, exact core/native `errors.Is/As`,
  typed details, explicit native outer frames and cleanup joins;
- private canaries in wrapper/native text across Error, fmt, slog and JSON;
- bounded cycle/wide-graph inspection and non-nil empty-wrapper refusal;
- unchanged definition/locale data, both CLI capability owners and native pins;
- Redis mixed domains, aggregate EXECABORT and full supported batch causes.

Run focused race checks first, then the repository's normal tidy/format/vet/race/
build checks and bounded graph fuzzing. Record source manifests and actual skips.
Protocol peers and external-module execution are not new production-service
qualification; any real service run still needs explicitly isolated authority.
