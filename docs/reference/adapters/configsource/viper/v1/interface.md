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

# Public Viper configuration Adapter

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go consumers and Framework scenario authors.
**Status:** implemented; local files/readers, native decoding and owned observation
are verified. No application loader or process-environment watcher is implied.
**Package:** `github.com/frost-leo/fathomry/adapters/configsource/viper/v1`.

## Bind once, select explicitly

New borrows an explicit public operation Runtime and required Inbox[Evidence].
Neither transfers ownership. Finite acquisition reserves evidence before native
dispatch. The caller owns shutdown and custody; retrying a failed evidence receiver
never rereads a file. Evidence is bounded metadata with original errors, not a copy
of configuration payloads.

Load accepts 1..16 ordered Inputs, each selecting exactly one literal absolute File
or borrowed Reader and loadable Settings. It returns the whole batch or no usable
prefix. Files close before return; borrowed Readers are never closed. No file
discovery, automatic dotenv loading, merge or environment mutation occurs.

Settings maps all supported native fields: encoding, defaults, explicit environment
bindings, allow_empty_env, automatic_env, env_prefix and ordered replacements.
Formats are yaml/yml/json/toml/dotenv/env. Defaults are ordered Scalar {kind,text}
records, not interface values. Kinds preserve native nil/string/bool and each
builtin integer/finite floating type. Integer text uses JSON decimal grammar
without fractions/exponents; fixed widths reject overflow. int/uint are
architecture-dependent; use fixed-width kinds for portable declarations.
Null requires empty text; absent Kind rejects. Duration observation settings use
nanoseconds, with native zero/default behavior.

## Different data profiles

- Document.RawCopy preserves original bytes. Keys and ValueCopy preserve native
  case/path/null/default behavior and live environment lookup.
- Capture freezes registered plus explicit native keys. ValuesCopy is an owned
  effective native map, not original syntax. Sources/environment must remain
  stable for a coherent capture; this is not a process-wide transaction.
- Decode[T] includes schema-derived mapstructure keys and explicit dynamic keys,
  then uses native weak conversion/duration/slice hooks. It does not replace
  [strict original-layer preparation](../../v1/interface.md).
- RawFile retains positive absence and present-empty separately, with no syntax
  parsing. The explicit byte limit is 0..1 MiB.
- Client.Source freezes explicit file paths into the shared complete-batch profile.
  Capture uses one bounded batch operation. Observe consumes native invalidations
  in an owned acquisition/handoff worker, without another timer or poller. It keeps
  one latest complete observation and marks overwritten observations with Gap.

Limits preserve the native profile: 16 sources, 1 MiB per document, 4 MiB aggregate,
64 KiB bootstrap declarations, 64 defaults/bindings/replacements each, 256-byte
keys, 64-depth/32,768-node structural bounds. Raw and native formats retain their
separate contracts; dotenv expansion remains refused.

## Watch and actual work

WatchSettings has explicit Paths, interval_ns (zero=1s; 10ms..5min) and queue_capacity
(zero=16; 1..64). Watch observes files only. Change.Index preserves the original
path position; -1 means whole-set synchronization. Resync covers initial observation,
failures and overflow. Consumers must not infer a complete change history.

Next cancellation affects only its wait. Close cancels and joins the actual native
worker; the same owner remains reachable after timeout. A shared Source Observer
retains its acquisition worker under a 24 MiB declared work reservation. Next only
waits for its bounded handoff; canceling that wait cannot cancel acquisition or
consume a native invalidation without completing its owned handoff. Runtime.Close
requests cancellation and waits for retained work, not merely expired waits.
Blocked OS calls and arbitrary Readers cannot be forcibly interrupted.

Watch retains one common-runtime slot and evidence reservation for its lifetime.
Its evidence resolves on actual cleanup, not successful Watch return. Do not block
all finite evidence reception behind one live lifecycle claim; retain that bounded
claim and receive other records independently. Close subscriptions before final
runtime shutdown; drain/ack evidence according to the application's sink policy.

## Errors, privacy and consumption

Definitions/Resources support offline code explanations and explicit localization.
No operation reads global settings or selects a locale. Runtime formatting/slog is
restricted; handles refuse JSON reconstruction. Settings is intentionally
serializable data: serializing it can disclose secrets. Explicit raw/native value
access and original causes are also sensitive.

[Load/query/ownership tests](../../../../../../adapters/configsource/viper/v1/load_test.go),
[Watch and privacy tests](../../../../../../adapters/configsource/viper/v1/watch_test.go),
[raw Source tests](../../../../../../adapters/configsource/viper/v1/acquisition_test.go) and the
[independent consumer](../../../../../../adapters/configsource/viper/v1/testdata/consumer/consumer_test.go)
exercise both native and strict use. Selecting Viper does not import Nacos.

## Package organization

The [Adapter tree map](../../../../../../adapters/README.md) defines this package's role
and file responsibilities; shared mechanisms, capability vocabulary, preparation
and concrete providers do not acquire identical APIs by convention.

Error definitions/resources remain offline. Runtime construction, translation
and native inspection live in `error.go`; formatting/serialization guards live
in `diagnostics.go`. Bare or transparently wrapped/joined public errors retain
existing ownership, details and causes. Explicit native frames keep their
provider classification; bounded internal graph search does not imply that
external `errors.Is/As` on arbitrary caller graphs is bounded.
