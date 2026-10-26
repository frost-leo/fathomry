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

# Checked single-Activity invocation

[Documentation](../../../../../README.md) / [Binding interface](interface.md)

**Audience:** independent Go projects and future host integrators.
**Status:** synchronous local invocation for the required-value ABI is implemented.
This is not a Worker, DAG executor, Temporal converter or durable effect protocol.
**Package:** `github.com/frost-leo/fathomry/framework/workflow/implementation/v1`.

## Call sequence

Prepare one Registry through the [binding interface](interface.md), then:

```go
result, err := registry.Invoke(ctx, "read_cpu", []byte(`{"target":"local"}`))
```

Input is borrowed only until return and must not be concurrently mutated. The
method admits the raw JSON before typed decoding can erase missing/null/zero
distinctions, creates fresh typed argument storage, calls the checked function
once on the caller's stack, checks its output and returns owned encoded bytes.

No handler, custom codec or validator runs during admission. A handler may run
only after that admission and the final pre-dispatch context observation.
No resource acquisition, service initialization, retry, timer or goroutine is
introduced. Existing helpers, Prepare, metadata reads and CLI checks remain inert.
Calling the original borrowed function through Lookup deliberately bypasses Invoke.

## Required JSON value profile

`InvocationProfile` is `fathomry.activity-json-values/v1`, separate from ABI,
declaration, business and deployment versions. It accepts one JSON object,
including `{}` for an empty port struct, with exact case-sensitive named required
ports. Nested objects are closed and all their properties are required.

- Missing, unknown or duplicate decoded names, trailing values, malformed UTF-8
  and unpaired UTF-16 surrogates refuse.
- Null is accepted only at an explicitly nullable schema position. Required
  presence, zero, empty and null are different.
- Integers use exact decimal integer lexemes within ±(2^53−1), then the declared
  inclusive bounds. Fractions, exponent spellings, negative zero and overflow
  refuse. This intentionally narrow profile is not general JSON Schema conformance.
- Strings use declared Unicode code-point counts and exact enum membership, not
  byte lengths or Unicode normalization.
- Arrays enforce declared item counts and recursively checked values. Non-null
  arrays require a non-nil Go slice; an empty non-nil slice may be valid.
  A nullable outer pointer can be nil, but a non-nil pointer still needs a valid
  non-null pointee.
- The existing type ABI is unchanged. Defined int64 kinds, including time.Duration,
  retain integer semantics. Duration is its underlying nanosecond count, with no
  implicit unit conversion; an explicit JSON compatibility option prevents v2's
  special default refusal of that already-admitted type.
- Optional/batch/byte/protobuf/custom-codec/recursive shapes remain unsupported.

Output JSON is a detached value, not a canonical fingerprint or proof that an
arbitrary configured Temporal DataConverter would have identical semantics.

## Failures and effect evidence

For normally returned calls:

| Observation | Invoked | Output |
| --- | --- | --- |
| Admission/input refusal or pre-dispatch observed cancellation | false | nil |
| Handler error, invalid output, encoding/limit failure, observed post-dispatch cancellation | true | nil |
| Returned handler success and complete output checks | true | caller-owned bytes |

The outer error is `ErrInvocation`. Its distinct `InvocationDiagnostic` contains
phase (`admission`, `input`, `handler`, `output`), fixed reason and bounded path.
It is not a completed static diagnostic: `Diagnose` does not extract it.
Existing category codes such as `ErrInvalid`, `ErrLimit`, `ErrHandle` and
`ErrCanceled` can remain causes. Native handler errors and caller cancellation
causes remain reachable through `errors.Is/As`; deliberate native-cause access
can expose sensitive data. Cause-code matching alone does not identify the phase:
the handler can itself return an error with the same category.

Paths use numeric member/array positions for raw input and struct/array positions
for Go output. They do not echo input member names, payload values or handler type
names. Ordinary result formatting/logging hides payloads. Result JSON
serialization/deserialization refuses, including the default JSON logger's
fallback for nested structs/slices. Explicitly accessing Output and constructing
a caller-owned data envelope is sensitive access, not an implicit result protocol.

Output plus a non-nil handler error is not successful data. A valid output is not
proof of accepted/committed/visible external effects. Invalid output or cancellation
after dispatch does not establish rollback or no effect; no retryability is inferred.

## Cancellation, panic and ownership

Context observations occur during bounded traversal, immediately before dispatch
and after output validation/encoding. Observed final cancellation suppresses output
without losing already-observed failures. There is no atomic cancellation/dispatch
or cancellation/return fence: racing cancellation can coexist with dispatch or
with a successful return after the last observation. Later cancellation does not
retroactively change a returned result.

Handlers get the caller's context unchanged. A handler ignoring it can block the
caller indefinitely; this method creates no detached work to pretend otherwise.
Handler panic and runtime.Goexit retain ordinary Go behavior. Paths that do not
return produce no result or no-effect claim.

Fresh argument storage isolates calls through that argument graph, not through an
independent capture/global/unsafe reference. Captures and receivers keep their
caller-defined lifetime and concurrency rules. Handlers must not mutate returned
reachable storage concurrently while output validation/encoding consumes it.
Published bytes do not alias input or returned Go storage and belong to the caller.
Independent calls and immutable metadata reads are safe only to the extent the
borrowed handler/captures also support concurrency.

## Bounds and accounting

Each direction has independent budgets:

| Limit | Maximum |
| --- | --- |
| Actual encoded input/output bytes | 16 MiB each |
| Wire depth | 32 |
| Charged values/member names | 1,048,576 |
| Decoded string bytes, including names | 16 MiB |

Root depth is 1; each child value increases it by one. Nullable Go pointers add
no wire level. Charge every value once, including root, containers and null, and
each object member name once. Charge UTF-8 string bytes per serialized occurrence,
including names; shared backing storage is not deduplicated. Schema constraints
may be tighter.

Raw admission enforces budgets before typed allocation. Output gets cheap
count/length guards and bounded traversal before encoding, then a bounded writer
checks actual encoded bytes. Failure never returns truncated output. The writer
does not bound encoder-internal buffering: escaping a legal string may allocate
more than its decoded size before Write. No bound on handler allocations, process
RSS, arbitrary external effects or hard wall-clock latency is promised.

## Executable evidence

- [Invocation tests](../../../../../../framework/workflow/implementation/v1/invoke_test.go):
  input/output validity, enums, nullability, identity, privacy and concurrent calls.
- [Control tests](../../../../../../framework/workflow/implementation/v1/invocation_control_test.go):
  cancellation, panic, Goexit and a deliberately non-cooperative handler.
- [Limit tests](../../../../../../framework/workflow/implementation/v1/invocation_limits_test.go):
  exact/one-over encoded bytes and symmetric input/output accounting.
- [Independent consumer](../../../../../../framework/workflow/implementation/v1/testdata/consumer/README.md):
  public-only invocation plus the unchanged vet/build A/B controls.

```sh
go test -race ./framework/workflow/implementation/v1 -count=1
go test ./framework/workflow/implementation/v1 -run '^$' \
  -fuzz '^FuzzInvokeInput$' -fuzztime=5s -parallel=2
```

Scope: [issue #159](https://github.com/frost-leo/fathomry/issues/159).
