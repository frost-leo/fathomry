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

# Facade and diagnostic probes

[Documentation](../../../README.md) / Internal package reference

**Audience:** authors of bounded conformance fixtures.
**Status:** implemented probes with explicit inspection limits.
**Package:** `github.com/frost-leo/fathomry/internal/conformance`.

Start with the [conformance interface](interface.md). These checks complement
actual native/dynamic controls; they are not a runtime security sandbox.

## Field and diagnostic probe boundaries

`Facade` inspects struct field **types**, including anonymous private value/pointer
embeddings. It does not dereference nil embedded pointers or invoke allowed methods.
`reflect.VisibleFields` handles multi-level promotion, field hiding, equal-depth
field ambiguity and recursive types. Its field visibility is independent of the
method allowlist, not a complete Go selector/capability analysis. In particular,
field/method collisions are conservatively rejected even when the ordinary selector
is ambiguous. An outer method hiding a promoted callback is not sufficient:
ordinary external conversion to a new defined type can remove that method.
The [separate-package selector tests](../../../../internal/conformance/checks_test.go)
execute this conversion and the exposed callbacks without reflection or `unsafe`.
Rejection of a layout alone does not demonstrate a production ownership escape.

`Private` probes the hooks selected by `%v`, `%+v`, `%#v`, `%s` and `%q`, preserving
fmt's Formatter/GoStringer/error/Stringer precedence and ordinary container descent.
Private fields and contents hidden by an outer formatter are not independently
invoked. Supply each actually used value/pointer form; this check does not invent
pointer methods on an unaddressable value. Text marshaling, JSON encoding and
returned encoding-error formatting also have panic probes. An ordinary marshaling
error remains permitted, notably the intentional runtime JSON refusal.
Text logging follows native `TextMarshaler` → byte slice → fmt dispatch. Byte
slices, including named slice/byte-element types, bypass fmt hooks; arrays and
pointers do not enter that branch. Native fmt, text and JSON output controls in
`TestPrivateNativeByteLoggingDispatch` check this distinction independently.

For slog, guarded `LogValuer` calls use native `Value.Resolve`, separately for the
text and JSON handlers, and recursively resolve groups without modifying the
caller's attribute slices. Go 1.26 rejects a chain reaching its 100th `LogValue`
call, even when that call returns a terminal value; the helper rejects it too.
Detection does not search for panic words or fallback strings: ordinary text,
including a literal formatter failure example, remains valid.

Diagnostic hooks must be synchronous, bounded and repeatable. fmt/marshal probes
are additional invocations before native output checks, not transparent runtime
instrumentation. These helpers do not infer failure from an already-resolved slog
error or from a hook that internally suppresses its own failure. Such paths need
the integration's independent native oracle. Canary matching checks literal
rendered bytes, not arbitrary encodings or secret transformations. Outputs are
limited to 1 MiB; each structural probe allows at most 1,048,576 visited values
and depth 100 (root depth zero). Bounds reject the fixture; they cannot cap a
hook's internal allocation, interrupt blocked code or prevent every recursive
formatter. Keep fault fixtures in owned, deadline-bounded subprocesses.

Use a separate caller-owned cleanup budget and native stop/join path. `Receive`
requires a deadline and at most 1,024 expectations; timeout is a test failure,
not permission to discard unfinished ownership. Its local release is not durable
recording. Returned payloads/errors retain their capability-owned immutable,
bounded inspection contract; these helpers do not generically deep-copy them.
