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

# Conformance helper interface

[Documentation](../../../README.md) / Internal package reference

**Audience:** in-module tests using testing.TB.
**Status:** implemented test support, never a production dependency.
**Package:** `github.com/frost-leo/fathomry/internal/conformance`.

## Reusable standard-testing support

Fathomry mechanism and Provider tests import
`github.com/frost-leo/fathomry/internal/conformance`. This is internal maintainer
tooling, not a public Provider-extension SDK or a production dependency. It is
shared across the private fault/resource/invocation/compatibility foundations
rather than owned by one runtime package. Making it a public testing SDK would
expose an unnecessary API and misstate that scope.

Independent business projects cannot consume a public Fathomry package yet: the
framework/capability/error API is deferred. They do not import this internal tool.
The [import boundary test](../../../../internal/conformance/imports_test.go) smoke-compiles
an independent module, rejects all five internal packages and all four withdrawn
public packages, and audits the private production dependency graph. The smoke
compile is not execution of a public error contract. The
[internal composition fixture](../../../../internal/conformance/composition_test.go)
retains the complete preparation/assembly/call/evidence path. Dynamic receipt/stream
field/method checks supplement import rejection in maintainer fixtures.
Supporting externally implemented Providers with a public conformance SDK would
need its own approved extension contract; it is not inferred from selectable
first-party Provider imports. This does not prevent internal Provider packages
from sharing the current acceptance suite.

| Helper | Executable obligation |
| --- | --- |
| `Expected[T]` / `Result` | Actual source/configuration/limits and technical correlation; shape/nesting; present versus missing; technical finality versus local release; primary/cleanup identities; attempt evidence and independent typed-value oracle |
| `Cause[T]` | Original native `errors.As` type/pointer/evidence without formatting it |
| `Receive` | Deadline-bounded independent evidence reception, matched by `Call` ID rather than arrival order; duplicates/missing evidence; explicit idempotent release only after local subtree completion |
| `Accounting` | Separate active/queued counts and byte reservations and outstanding receipt count/bytes at synchronized checkpoints |
| `Facade` | Method-only facade's exact dynamic/addressable-copy method allowlist, no direct or promoted exported fields under `reflect.VisibleFields`, and no nil facade |
| `Private` | Injected secret canaries through fmt and text/JSON slog, with selected-hook panic probes and output/traversal bounds, without echoing secrets on failure |
| `Runtime` | The privacy checks plus refused JSON encoding and reconstruction of runtime types; panicking JSON callbacks fail without printing their payload |

`Value` must derive expected data/effect facts from an independent input/native
oracle, not copy the integration's output. Present data without an oracle is a
test failure. Check each returned dynamic handle/callback surface under its own
allowlist: a stream may legitimately close itself, not the shared client.
Reflection cannot audit arbitrary closures or provide a Go security sandbox.

## Caller obligations

Helpers report testing failures, not business results. `Expected` data must come
from an independent workload/native oracle, not copied output. Present values
require a `Value` oracle. `Receive` requires a context deadline and 1-1024 expectations,
with unique nonempty `Call` IDs and both `Final`/`Released` required. Timeout leaves
unfinished evidence owned; fixtures must retain their native cleanup path.

`Private` invokes selected hooks extra times; they must be bounded, synchronous
and repeatable. `Runtime` additionally checks real JSON guards using an independently
owned non-nil pointer to a zero value of the same runtime type (after one pointer
dereference of the supplied value when applicable), never a live handle. Ordinary
standard error wrappers have no such JSON-refusal contract. Local evidence
release is not durable acknowledgement.

## Details and executable evidence

- [Diagnostic probes](diagnostics.md), [fixtures/evidence classes](fixtures.md).
- [Helper source](../../../../internal/conformance/checks.go), [diagnostic source](../../../../internal/conformance/diagnostic.go).
- [Helper tests](../../../../internal/conformance/checks_test.go), [privacy tests](../../../../internal/conformance/privacy_test.go), [controlled integrations](../../../../internal/conformance/integration_test.go).
- [Integration workflow](../../../development/sdk-integration.md), [testing](../../../development/testing.md).

Run `go doc -all ./internal/conformance` for exact declarations. No public test
SDK or production capability is supplied by these helpers.

Foundation provenance: [Issue #6](https://github.com/frost-leo/fathomry/issues/6).
Facade and diagnostic hardening: [Issue #13](https://github.com/frost-leo/fathomry/issues/13).
