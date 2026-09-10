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

# Viper v1 configuration interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework configuration and integration maintainers.
**Status:** implemented internal local profile; no public configuration loader.
**Package:** `github.com/frost-leo/fathomry/internal/configsource/viper/v1` (Go name `viper`).

## Responsibilities and call sequence

This independently selectable implementation lives under the `internal/configsource`
capability grouping. The intermediate directories introduce no Go facade,
Provider registry, umbrella SDK import or configuration policy. Only this
package imports Viper; shared foundations do not depend back on it.

1. Composition selects authorized, static inputs and constructs `OptionsV1`
   bootstrap settings separately from the file/reader in each `LoadInput`.
2. `Load(ctx, inputs)` validates the entire bootstrap batch before acquisition,
   then reads each input into a fresh private Viper instance. It returns all
   documents in input order or a nil batch, never a successful prefix.
3. `Document.ValueCopy(key)` makes a native query and copies any returned
   collections. This is sensitive, potentially live inspection, not preparation.
4. For framework preparation, composition retains each original `RawCopy()`,
   assigns its authorized `resource.LayerKind`, and calls
   [`resource.Prepare`](../../../resource/configuration.md) once. It does not
   reconstruct input from normalized native values.

The [executable example and preparation proof](../../../../../../internal/configsource/viper/v1/integration_test.go)
show this separate composition. An explicitly bound environment string supplies
a variables document in that fixture; this is not a published environment-value
format or a public loader. Optional-file selection also remains test-owned
composition policy. Business code is not expected to assemble this internal API.

## Supported native profile

| Operation or setting | Meaning and limits |
| --- | --- |
| Explicit acquisition | Exactly one literal absolute file path or caller-owned reader per input; encoding is explicitly `yaml` or `json`, never inferred from a filename |
| Files | Stable regular files; normal OS symlink resolution, with stat before opening and again on the owned descriptor; no directory/home/environment expansion, search, source substitution or writeback |
| `ReadConfig` | One independent SDK document per input, not SDK merge; original bytes are retained before native normalization |
| Scalar `SetDefault` | Ordered entries, last assignment to a case-folded key wins; allowed types are documented on `Default`; no caller maps, slices, callbacks or custom types |
| Explicit `BindEnv` | Both key and exact case-sensitive name required; repeated keys append names in order; values are read live on query, not captured by binding |
| `AllowEmptyEnv` | False by default; true accepts an explicitly empty environment string instead of falling back |
| Native `Get` | Case-insensitive keys and `.` paths; bound environment > document > native default; explicit null may fall back to default; nested parent queries are not a merged enumeration of child environment bindings |
| Native types | YAML's native scalar types are retained, including timestamps and non-finite floats where the native codec accepts them; JSON numbers remain native `float64`, including rounding |
| Explicit copies | `ValueCopy` returns owned maps/slices; `RawCopy` returns original owned bytes for admitted input, preserving case, numeric lexemes, null, native JSON duplicates and document boundaries |
| Technical syntax restrictions | Bounded structural preflight before SDK decoding; YAML anchors/aliases, duplicate keys and multiple documents are rejected; no application field/type/null/schema policy is added |

A YAML preflight parser failure retains its own YAML cause. Duplicate YAML keys
are refused with ErrInput and no invented native cause: the pinned decoder would
otherwise allocate a diagnostic for every duplicate pair. Preflight matches its
exact `(node.Kind, node.Value)` comparison without case folding or tag normalization.
Errors subsequently
raised by the SDK retain the native `viper.ConfigParseError` chain. JSON preflight
does not replace native syntax-error interpretation. Native JSON duplicate-key
last-wins behavior remains observable; the original bytes still let preparation
reject duplicates. A native query pass is not a preparation pass.

No AutomaticEnv, implicit prefix/name derivation, key replacer, flags, custom
codec/decode hook, type-by-default conversion, remote provider, watcher, reload,
Set, AllSettings, MergeConfig/Map, write or raw SDK handle is exposed. Selected
SDK paths use its per-instance discard logger and do not call the process logger.

### Why not expose native merge?

Viper v1.21.0's `mergeMaps` calls the **package-level** Viper logger, including
configuration values, even for separately constructed instances. An object/scalar
conflict can log an error and retain the previous object while MergeConfig returns
nil. The integration neither rewrites global logging nor implements a replacement
merge under a native name. Independent reads preserve the selected native
capability; existing preparation owns framework overlays.
[Pinned implementation](https://github.com/spf13/viper/blob/394040caccbdf5821fa6839386a35f0fb1b1ee9e/viper.go#L1806-L1880).

### Why restrict negative query indices?

Viper v1.21.0 can panic when a negative integer reaches its slice lookup. Before
calling it, ValueCopy refuses negative values parsed by native `strconv.Atoi`
in any component after the root, even if a literal map key, default or environment
binding would have matched instead. This is an explicit query-only restriction,
not a reimplementation of native longest-prefix lookup or a blanket panic recovery.
Root negative numeric keys, indices `0`, `+0`, `-0`, and negative-looking
nonnumeric keys remain supported. Copied parent maps and original admitted bytes
retain excluded keys. Options/default/binding key definitions are not rewritten.

## Bounds and ownership

All limits are per Load unless stated otherwise. They bound admitted input and
SDK expansion, not process RSS or every caller-owned allocation.

| Budget | Limit and enforcement |
| --- | --- |
| Sources | 1–16; checked before reading anything |
| Raw document | 1 MiB each; file size is checked before reading, and every reader is limited during reading |
| Aggregate raw documents | 4 MiB; remaining allowance is applied before each acquisition |
| EOF witness | At most one extra byte on the failing acquisition, to reject oversize rather than certify a truncated prefix |
| Bootstrap entries | At most 64 defaults and 64 environment bindings per input |
| Bootstrap content | 64 KiB total paths, keys, names and default values; scalar accounting reserves up to eight bytes each; checked before acquisition |
| Keys / environment names | 256 UTF-8 bytes each; explicit nonempty names, no NUL or environment `=`; query path at most 64 parts |
| Paths | At most 4096 bytes, absolute and literal |
| YAML structure | At most 32,768 nodes, depth 64 from the document node; no anchor/alias expansion; checked before native value decoding |
| JSON structure | At most 32,768 tokens and nesting 64; checked before native value decoding |
| Live environment result | At most 1 MiB per query; oversize fails, without truncation or default fallback |
| Nonprogressing reader | 100 consecutive `(0, nil)` reads produce inspectable `io.ErrNoProgress` |

YAML's public parser builds a bounded-input node tree before the node/depth check.
Thus the node limit is **not** a cap on that preflight tree's allocation. Raw byte
caps apply before that allocation; native decoding cannot expand aliases afterward.
Duplicate checking happens before SDK decoding, avoiding the native duplicate-pair
error expansion. Unique mappings still encounter the SDK's pairwise comparison;
no universal parser CPU budget is promised. Native buffers, parser trees, decoded values, result copies and GC add costs beyond
raw input. Dense rejected input needs allocation evidence too. Caller-owned reader
buffers/error graphs and process environment storage are not claimed as bounded by
this package. Callers bound the number of concurrent loads and retained documents,
raw copies and query results; there is no global quota, pool or cache.

All input containers are borrowed during Load only. Retained bootstrap strings
are cloned rather than keeping a small slice of a large caller string allocation.
Native state is never exported or mutated after publication. Documents permit
concurrent read/copy operations; this is not a guarantee about concurrent Get/Set
on an arbitrary Viper. Callers must not mutate SDK globals such as SupportedExts
concurrently with native decoding.

The integration closes owned file descriptors on success and failure, before
returning; a close failure invalidates the batch and retains a separate ErrClose
cause. Borrowed readers are never closed and must obey io.Reader, including
accountable blocking, panic-free callbacks and caller-owned cleanup. No background
goroutine, stream, watcher, Close method or artificial managed resource is created.

Context cancellation is checked between synchronous phases and reads. It cannot
hard-interrupt blocked Stat/Open/Read/Close or a parser. A timeout does not prove an
input was never read. No goroutine is detached to manufacture prompt cancellation.
Static files/environment are required for a coherent preparation attempt; freezing
the result does not establish an atomic common-time snapshot across sources.

## Failure and diagnostic boundaries

`ProviderID` is `configsource.viper.v1`. The same code-owned implementation identity
qualifies both `fault.Context.Provider` and the `fathomry.configsource.viper.v1.*`
technical error namespace: ErrInput, ErrLimit, ErrRead, ErrDecode and ErrClose.
Different capabilities, providers and SDK majors must not share these identities.
Composition owns its own errors; it does not manufacture a Viper failure when the
integration was not responsible. The common fault package does not depend on, or
register, this concrete implementation. Ordinary fmt/slog output excludes paths, environment names,
credentials, payloads and native messages. Intentional `errors.Is/As` inspection
retains original read/parse/OS/cancellation and close causes and is not redacted.
Arbitrary reader causes are retained, not deep-copied or universally bounded.

LoadInput, OptionsV1, Default, Binding and Document have safe ordinary formatting and
refuse JSON encoding/reconstruction. RawCopy and ValueCopy deliberately expose
sensitive input; their returned bytes/values are not safe diagnostics. This is not
a claim to sanitize arbitrary caller logging, reflection or unsafe access.

For a stable structured-log projection, pass pointers to these runtime types to
`slog.Any`. Their explicit outer pointer `LogValue` methods also handle typed nil
pointers without accessing a receiver or invoking a promoted nil-pointer method.
Value forms retain fmt/TextHandler redaction. They deliberately do not implement
LogValuer: JSONHandler encounters their existing JSON refusal and emits a bounded
encoding-error marker, not raw fields, a recovered panic or a stack trace. This
method-set distinction does not relax runtime serialization/reconstruction guards.

Missing, unreadable and malformed files all fail Load. Only outer source selection
can declare absence optional; unreadable or malformed optional input is not silently
ignored. A later failed load cannot mutate or relabel a previous Document or
Prepared result. Nil/zero Documents have no loaded encoding/raw input and reject
ValueCopy; a successful native empty YAML document is distinct from prepared data.

## Version axes and applicable mechanisms

| Axis | Accountable contract |
| --- | --- |
| SDK major | `internal/configsource/viper/v1`; a capability/SDK-major import boundary, not an independently released Go module or blanket support for all Viper 1.x |
| Logical implementation | `configsource.viper.v1` in diagnostics and error kinds, aligned with the capability/SDK/SDK-major layout; not a resource name or an exact module version |
| Bootstrap settings | `OptionsV1` in `options.go`; independently versioned Go contract, with no reader handle, serialized format, unknown-version coercion or automatic migration |
| Actual SDK/build | Required Viper v1.21.0 and YAML v3.0.5; consumer builds may select different versions/replacements, so requirements and go.sum are not an exact lock |
| Document encoding | YAML or JSON, not a business schema version |
| Preparation format/revision | Existing Schema.Format/Input.Format and the opaque Prepared revision; never inferred from SDK or bootstrap versions |

The [actual consumer executable](../../../../../../internal/configsource/viper/v1/testdata/consumer/main.go)
executes the integration and preparation and uses compatibility.Inspect for selected
SDK/implementation build facts. [Its test](../../../../../../internal/configsource/viper/v1/integration_test.go)
independently inspects that same binary and checks the exercised versions and
replacements. Behavioral changes need the same native, preparation, fault,
ownership, privacy and performance oracles, not just compilation or a directory
rename. A future persisted bootstrap format needs its own explicit reader/version
and migration-or-refusal contract before support is claimed.

Applicable standard sections are S01–S11 at the accepted
[c0c6bf0 baseline](https://github.com/frost-leo/fathomry/blob/c0c6bf05a46a70470c5686d64ef8865e10f0875c/docs/architecture/internal-sdk-integration.md).
The integration reuses technical faults; the separate composition reuses
resource.Prepare; tests reuse conformance privacy/runtime/cause helpers and build
inspection. Resource leases, admission Access, invocation receipts, remote-effect
assessment and service compatibility claims are inapplicable to these finite
local reads. The inspection-only refusing factory in preparation tests merely
observes fresh settings copies; it constructs no managed Viper.

OptionsV1 is explicit acquisition bootstrap, validated before I/O. It is not the
business-resource schema passed to Prepare: requiring that bootstrap to be loaded
through its own acquisition path would be circular, and freezing live bindings
would change native semantics. Shared preparation remains authoritative for the
acquired application layers. The boundary therefore reuses applicable mechanisms
without converting every technical input into a managed resource or prepared DTO.

## Executable evidence and maintenance

- [Loading, options, versioned faults, resource bounds and input fuzzing](../../../../../../internal/configsource/viper/v1/load_test.go).
- [Native queries, live bindings, copies, concurrency and query fuzzing](../../../../../../internal/configsource/viper/v1/query_test.go).
- [Real-file preparation, independent negative controls and the consuming build](../../../../../../internal/configsource/viper/v1/integration_test.go).
- [Linux filesystem/resource observations](../../../../../../internal/configsource/viper/v1/load_linux_test.go).
- [Controlled benchmarks](../../../../../../internal/configsource/viper/v1/load_bench_test.go):
  direct bounded native operations versus integration, and bounded acquisition plus
  the same Prepare versus integration plus Prepare. Four identical inputs, mixed
  YAML/JSON, reader/warm local-file I/O, typical/near-limit and repeated/independent
  concurrent execution. Both paths assert useful values. Raw preservation adds
  parsing/copying cost; no numeric performance target or speculative cache is used.
  BenchmarkRejectedDocument additionally measures dense near-byte-limit YAML/JSON
  rejected by the pre-SDK structural guard, including YAML's preflight AST cost.
- [SDK verification workflow](../../../../../development/sdk-integration.md) and
  [testing](../../../../../development/testing.md).

Scope and implementation evidence belong to [Issue #19](https://github.com/frost-leo/fathomry/issues/19).
No public CLI/API, configuration center, full startup, production service or
Temporal command/history format is introduced. No replay or real-service support
is implied by these local checks.
