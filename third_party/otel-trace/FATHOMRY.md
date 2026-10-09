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

# Preserve span-link trace-state in OTLP export

This is the narrowly scoped, owner-authorized local correction for Fathomry
issue #128, compatibility revision `v1`. Its upstream remains
`go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.47.0`, origin
`66cfc9520e205b7d450183532772401bc2b6674c`. [UPSTREAM.json](UPSTREAM.json)
records the original module/archive checksums and original file hashes.
The original Apache-2.0 [LICENSE](LICENSE) and source notices are unchanged;
first-party additions do not relicense upstream material.

## Correction and limits

The selected native SDK retains `SpanContext.TraceState` in both initial and
dynamic span links, but the upstream OTLP link converter leaves the protocol's
`trace_state` field unset. An independently decoded HTTP/protobuf request
therefore loses an accepted link field even though the native snapshot retains it.

The only modified upstream production file is
`internal/tracetransform/span.go`. Its link converter now assigns the original
span context's trace-state string to the corresponding OTLP field. No native
span ownership, sampling, ID, flags, attributes, timestamps, SDK API, transport,
retry policy or other signal transformation is changed. Empty trace-state
remains empty. This is field preservation, not an additional propagation policy.

For standalone module use, `go.mod` omits upstream monorepo-relative replacement
directives. Exact dependency requirements remain unchanged; `go.sum` includes
checksums for those original sibling requirements. This module does not copy or
patch the native trace SDK. Root replacements are local only: independent
consumers require the immutable replacement provided by the existing CLI
dependency policy because root replacements are not inherited and nested
modules are excluded from the parent module ZIP.

## Qualification and retirement

`internal/tracetransform/fathomry_link_tracestate_test.go` uses a real native
provider and recorder, then independently decodes the protobuf representation.
It covers initial and dynamic links with empty/nonempty trace-state and
unchanged IDs, sampled/remote flags and attributes. Fathomry's Internal tests
add an independent HTTP/protobuf peer and verify actual bounded Adapter data,
mutation, cancellation and export paths.

From this module directory:

```sh
go mod tidy -diff
go vet ./...
go test -race ./... -count=1
go build ./...
```

Keep the uncorrected native-versus-wire witness separate from passing checks.
Retire this replacement only after an explicitly selected upstream release
passes the same link-state, native, wire, dependency-delivery and lifetime
qualification. This local correction is neither an upstream release nor a
backend/deployment certification.
