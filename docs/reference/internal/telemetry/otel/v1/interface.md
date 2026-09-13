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

# OpenTelemetry interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework composition and integration maintainers.
**Status:** implemented bounded internal profile; no backend/deployment certification.
**Package:** `github.com/frost-leo/fathomry/internal/telemetry/otel/v1` (Go name `otel`).

## Contents

- [Responsibilities and native choices](#responsibilities-and-native-choices)
- [Capabilities and call sequence](#capabilities-and-call-sequence)
- [Configuration and bounds](#configuration-and-bounds)
- [Data and association](#data-and-association)
- [Errors, effects and evidence](#errors-effects-and-evidence)
- [Ownership and shutdown](#ownership-and-shutdown)
- [Compatibility and executable evidence](#compatibility-and-executable-evidence)

## Responsibilities and native choices

This implementation consumes the shared resource, invocation, fault and
compatibility mechanisms. It does not implement a public logger, workflow
runtime, durable result ledger, scheduler or Collector. `conformance` is test-only.

The actual native SDK constructs log records and spans, propagates W3C context,
samples traces, and aggregates synchronous metrics. Small private native
processors capture logs/spans into an explicitly bounded queue. A native manual
reader collects metrics. Real native OTLP HTTP/protobuf exporters transmit them
during controlled `Flush` and final assembly cleanup.

Native background batch processors/periodic readers are **not** used. Their
timer-driven exports, overwrite/drop policies and global error handling do not
provide this profile's caller-owned export budgets and independent evidence
reservation. This queue does not imitate native batch-processor semantics:
full queues reject new data, and only explicit export drains accepted events.
There is no background export, timer, disk spool, automatic retry or delivery
callback. Composition owns flush scheduling and the evidence receiver.

The separate [Zap bridge](zapbridge/interface.md) and
[zerolog bridge](zerologbridge/interface.md) translate their existing, different
structured contracts without moving provider ownership into either logger.
Core telemetry imports neither logger; each bridge selects only its own logger
dependencies. No grouping-level aggregator is introduced.

## Capabilities and call sequence

| Capability | Implemented behavior | Deliberately absent |
| --- | --- | --- |
| Logs | String bodies, events, all OTel severities 0–24, typed/nested attributes, copied native records | Arbitrary body kinds, user marshalers, implicit error formatting, exit/panic effects |
| Traces | Start/End, parent or explicit new root, five native span kinds, links, attributes, events, safe exception annotation, explicit status, parent-based ratio sampling | Native span/provider escape, auto instrumentation, tail sampling, automatic business status |
| Metrics | Fixed manifest of int64/float64 counter, up/down counter, gauge and explicit-bucket histogram; cumulative collection; cardinality overflow | Observable callbacks, dynamic instruments/scopes, arbitrary views, delta/exponential aggregation, exemplars |
| Context | Explicit W3C traceparent/tracestate; opt-in baggage | Global propagator installation, authorization or automatic baggage-to-label copying |
| Export | Explicit count/byte-bounded event batching, native HTTP/protobuf, none/gzip, explicit HTTPS CA and optional mTLS; literal-loopback cleartext | gRPC, HTTP/JSON, proxies, redirects, environment credentials, automatic retries, backend storage guarantees |

1. Prepare `OptionsV1` with `Select`, supplying only authorized
   [resource layers](../../../resource/configuration.md). No network runs here.
2. Supply `resource.WithLimits` and call `resource.Assemble`. Construction
   acquires providers/readers/exporters and an owned HTTP transport, but does not
   establish backend readiness. Retain a failed assembly's cleanup responsibility.
3. Create an independent `invocation.Inbox[Result]` and `Bind` a `Client`.
   `LimitsV1` and `EvidenceBytesV1` describe bootstrap charges; changed
   effective overlays need corresponding resolved composition policy.
4. `Emit` queues logs; `Start` returns a span token and context; always
   `End` the span. `MeasureInt64`/`MeasureFloat64` record a declared
   instrument. Use explicit valid `fault.Correlation` for each operation.
5. Call `Flush` with its own context/correlation. Read the receipt's result,
   not merely the immediate Go error. Independently receive and release inbox
   deliveries, including failures and sampled-out spans.
6. Stop and close logging consumers before their telemetry dependency. Close
   assemblies, not clients. Each live span must end even after cancellation.

Finite calls return setup failures directly; accepted outcomes go to both receipt
and inbox. A `Start` failure after admission also publishes failed evidence in
the inbox, even though no span token is returned. A successful `Start` keeps
its receipt unresolved until `End`; `Span.Receipt` permits independent waiting.

The [consuming executable](../../../../../../internal/telemetry/otel/v1/testdata/consumer/main.go)
demonstrates preparation, admission, independent evidence, native export,
receiver observation, physical connection closure and build inspection. It is a
maintainer fixture, not startup boilerplate delegated to business projects.

## Configuration and bounds

[options.go](../../../../../../internal/telemetry/otel/v1/options.go) owns exact
field meanings, defaults, ranges and units. `Format` defaults to 1; unknown
formats and fields reject. Bootstrap zero defaults only where documented.
`SampleRatio == nil` defaults to 1; pointer-to-zero deliberately selects root
sampling zero. Explicit layer zero never reapplies bootstrap defaults.

Shared preparation preserves Defaults < Base < Environment < Local < Variables,
recursive object merge, array replacement and applicable null clearing. It
validates every layer before merging and freezes independent construction copies.
Do not concurrently mutate bootstrap maps/slices/pointers/layer bytes during
preparation; later changes do not alter selected instances.

| Bound | Default; range/meaning |
| --- | --- |
| Active / queued calls | 16 / 0; 1–64 / 0–64; zero queued calls rejects overload |
| Admission and execution timeout | 5 s; 1 ms–1 min, separately for each phase; raw `timeout_ns` uses nanoseconds |
| Borrowing nodes per call | At least 2: producer plus an actually entered native dial/handshake guard |
| Pending logs, spans and live-span reservations | 512 items / 4 MiB; 1–4096 items, logical bytes up to 16 MiB |
| Per-record/span input charge | 64 KiB; 1 KiB–1 MiB; live/ended sampled spans reserve the full allowance |
| Batch items | 128 or smaller queue size; additionally bounded by logical request charge before native serialization |
| Serialized request / response | 1 MiB / 64 KiB; 1 KiB–4 MiB / 1 KiB–256 KiB |
| Attributes / nodes / group depth | 64 per group, 256 total input nodes, depth 8; fixed framework association is additional |
| Span events / links | 32 / 16, within the same cumulative per-span input budget |
| Instrument scopes / instruments | One fixed scope per source; at most 32 declared instruments |
| Metric dimensions / cardinality | At most 8 declared scalar keys; default 128 series per instrument, 1–1024; instrument count × cardinality ≤4096 |

Record charge includes a 256-byte base, fixed association, key/value data and
per-attribute/array charges in
[data.go](../../../../../../internal/telemetry/otel/v1/data.go). Repeated span
mutations consume cumulative budget even when replacing a key. Metric strings
are at most 256 bytes and measurements have a separate 4096-byte input charge.

Event batching limits pre-serialization work; the SDK still serializes before
its exact request-size rejection. One oversized event may therefore be rejected
at export. Metrics are one bounded collection, not automatically split; a large
collection can exceed the request ceiling and fail. Both uncompressed protobuf
and outgoing gzip bytes are bounded before network entry. Responses are bounded
before native error/protobuf parsing. These are **not** RSS, total allocation,
latency, aggregate process-memory or distributed-account-quota guarantees.
Resident queues/aggregation, caller data, retained receipts and native cause
graphs are distinct costs. No arbitrary-precision metric arithmetic is promised.

`LimitsV1` charges configured working envelopes, including worst configured
metric-series work, rather than measuring heap use. All aliases share original
admission. Finite native operations serialize through one context-aware gate;
an export can backpressure recording while admission/gate waits remain bounded.
Live spans retain active calls: saturating all active slots with spans can reject
logging/flush calls until spans end. Independent sources have independent quotas.

### No ambient SDK configuration

Nonempty `OTEL_*` variables reject at preparation, construction and controlled
operation entry. Native `WithResource` still merges environment resources, and
some constructors evaluate defaults before explicit options. The package neither
clears environment nor temporarily changes global handlers. Authorized framework
environment acquisition must use explicit layers without leaving an ambient
OTEL namespace enabled for this profile.

Composition must keep process environment and SDK diagnostic globals stable.
Concurrent mutation by other code is outside this profile; this is not a sandbox.
SDK constructors can initialize their native default-resource cache even when the
actual providers receive explicit resources. No default detector attributes enter
the selected output resource.

Endpoints include exact paths; query/userinfo/fragment and redirects are refused.
HTTPS requires explicit CA PEM, optionally a client certificate/key pair, never
certificate filenames or insecure verification. Only literal loopback IPs allow
HTTP. The owned transport does not use environment proxies, system CA discovery
or response decompression. Platform DNS remains a native networking concern;
loopback verification is not DNS/deployment certification. Native diagnostic
logging must not be routed recursively into this pipeline.

## Data and association

`LogRecord`, `SpanInput` and mutator input storage is borrowed until return,
then independently frozen. Supported `slog.Attr` values are strings, bools,
signed integers, uint64 fitting int64, finite float64, duration, time, canonical
groups, nil and byte/string/int64/float64/bool slices. Named `AttributeMap` values
inside `slog.Any` preserve nested empty groups; raw `[]slog.Attr` passed to
`slog.Any` is normalized by slog and can drop empty children before this boundary.
No user LogValuer,
Stringer, reflection, arbitrary error or marshaler is called. Empty values/groups
remain distinct from absent fields.

Durations become integer nanoseconds; attribute times become UTC RFC3339Nano.
Log/span event timestamps use native nanoseconds; explicit start/log timestamps
are restricted to UTC years 1970–2261 to avoid Unix-nanosecond overflow. Local
year alone does not establish a valid instant. Zero time
selects the native observation time. Strings/keys require valid UTF-8 without
NUL; binary attributes may contain arbitrary bytes. Keys with `fathomry.`
prefix are reserved, including within groups.

Metrics accept only declared string/bool/int64/float64 dimensions. Negative
monotonic-counter increments reject; up/down counters permit negatives. Native
cardinality overflow preserves aggregate values rather than every original
label. Sampling, native arithmetic and aggregation affect interpretation; metrics
are not authoritative per-Item results.

Resource attributes describe the emitter; instrumentation scope describes its
code; record attributes describe an event. Resource and scope schema URLs are
separate optional declarations, not the SDK/protocol/configuration version.
The resource requires explicit `ServiceName`; optional resource/scope maps
contain bounded strings and do not override `service.name`.

Logs/spans receive fixed technical call/parent/owner and original source/scope
fields. These are not public Run/Item identity or authorization. Returned trace
contexts carry IDs/flags, not native span/provider handles. Unsampled trace IDs
can still correlate logs. Baggage is opt-in propagation only, never automatic
log attributes or metric dimensions. Carrier input is bounded to 32 headers and
8192 aggregate bytes; malformed W3C values retain native ignore semantics.

`Span.RecordError` accepts a `*fault.Error` and emits only safe kind metadata
as an exception event. Raw causes are not exported. It does not set status or
fail the invocation. `SetStatus` explicitly preserves native precedence
Unset < Error < Ok; status is not an Item/Run disposition.

## Errors, effects and evidence

[errors.go](../../../../../../internal/telemetry/otel/v1/errors.go) centrally
adapts code-owned technical kinds to `fault.Error`. Input/unsupported,
environment, limit, state, export, partial response, protocol, cleanup,
undelivered and recursion identities do not prescribe business retry or status.
Ordinary formatting/slog excludes endpoints, credentials, payloads and original
error text. Deliberate `errors.Is/As` retains native causes; native objects and
caller-owned cancellation causes remain borrowed, not cloned or automatically
size-capped.

One intentional native-cause adaptation removes `tls.RecordHeaderError.Conn`
after closing it. `errors.As` still exposes the native error type/header/message,
but not the closed connection and retained network owner. Other native causes
remain unchanged; raw native connection fallback is not supported.

`Result.SignalsCopy` returns independent slice storage. Each signal records:

- `Accepted`: local queue/measurement acceptance, not export.
- `Submitted`: items passed to native export, including failed batches.
- `Acknowledged`/`Rejected`: bounded receiver-declared item counts.
- `TransportCalls`: RoundTrip entries, not an invented exact TCP/SDK attempt count.
- `Effect`: no transport attempt, acknowledged, partial, or unknown; accumulated
  counts retain earlier batch facts when a later batch fails.
- `Err`: safe technical error retaining original causes when available.

HTTP 200 must carry an exact protobuf content type and a valid bounded OTLP
response. HTTP 101 protocol switching is rejected and closed before reading its
upgraded body, so it cannot bypass the HTTP cancellation boundary.
Partial rejection/warnings remain errors without retry. This package
independently observes partial responses as well as preserving native errors;
it does not assume HTTP 200 means full success. Non-200 success statuses,
malformed responses and invalid rejection counts reject.

A submitted event batch is removed even after export failure: resending an
unknown-effect batch automatically could duplicate events. Unsubmitted batches
remain queued; failure stops that signal's drain while other signals may continue
within the same context budget. Cumulative metric snapshots may reappear on a
later explicit flush; this is native cumulative temporality, not event replay.

The independent inbox reserves evidence before admission and rejects when full,
without entering the SDK. Export errors are returned directly into this path;
they do not depend on a working logger or `otel.Handle`. Optional invocation
observation remains lossy and cannot replace required results.

Export networking receives cancellation/deadlines and private control metadata,
not arbitrary caller context values. In particular, caller `httptrace` callbacks
must not receive owning connections. Record/span correlation still uses the
original operation context before export; exporting does not auto-instrument
its own HTTP requests.

## Ownership and shutdown

A Client/bridge never owns shutdown. Composition owns provider, capture queues,
manual reader, native exporters and HTTP transport through the assembly.
A started span must be ended; its initiating cancellation does not complete it.
An interrupted End remains resumable with a new caller-owned cleanup context.
Successful repeated End returns the original receipt, including after shutdown.

Assembly close refuses new admission and reports incomplete while live calls or
borrowers remain. It does not manufacture quiescence from cancellation. Once
admitted native work has stopped, cleanup performs one bounded final export,
shuts down providers and explicitly owned exporters, and closes owned connections.
Native HTTP dialing/handshakes are separately tracked: request cancellation can
return before a transport dial. Entered operation dials retain invocation guards,
so a final receipt can still have `Released == false`. Late dial callbacks cannot
acquire IO after their original scope or network owner has ended. TLS handshake
cancellation follows the original request, not net/http's detached dial context.

Final cleanup stops new dials, cancels entered ones and closes every registered
connection rather than relying only on an idle-pool close. If native work has
not returned before cleanup cancellation, a resource continuation joins it;
it does not repeat export or one-shot SDK shutdown. Close failures stop further
connection acquisition, bounding retained native close errors by existing work.
There are no private background processors/readers to join. Native cleanup and
export failures remain in resource history; repeated Close does not erase them.
Unsubmitted event loss during final close is separately `ErrUndelivered`.

Release is not rollback or receiver durability. It also does not zeroize frozen
configuration/PEM material, destroy caller-retained data, force arbitrary code or
kernel/DNS work to terminate, or give a total cleanup-history budget. Those
remain explicit composition/platform responsibilities.

## Compatibility and executable evidence

Selected versions, rechecked against current upstream releases:

- Core, trace/metric APIs/SDKs and trace/metric exporters: **1.46.0**.
- Logs API/SDK: **0.22.0 (beta)**; log exporters remain experimental.
- Generated OTLP protocol module: **1.11.0**, not the options contract version.
- The newer **1.47.0-rc.1** is an unselected RC, not a stable Logs upgrade.
  See the [upstream releases](https://github.com/open-telemetry/opentelemetry-go/releases/tag/v1.46.0)
  and [RC announcement](https://opentelemetry.io/blog/2026/go-logs-api-sdk-rc/).

`Profile` is a non-sensitive projection of frozen mode/limits, **not** complete
resource/metric schema disclosure or a compatibility certificate. Service mode
and version remain unknown. Use `compatibility.Inspect` on the consuming binary;
the package supplies no automatic Supported record or semver-wide guarantee.
Go requirements, selected module versions, configuration format, source revision,
OTLP schema and backend deployment are separate axes.

The OTLP dependency graph also selects mapstructure **2.5.0**, required by
grpc-gateway **2.30.0**. Viper's actual consumer expectation and behavioral tests
are requalified; Viper's runtime contract is unchanged. No unmodified historical
snapshot is treated as the actual consuming build.

Executable evidence is organized by responsibility:

- [Options](../../../../../../internal/telemetry/otel/v1/options_test.go),
  [errors/privacy](../../../../../../internal/telemetry/otel/v1/errors_test.go),
  [data/copying](../../../../../../internal/telemetry/otel/v1/data_test.go).
- [Logs](../../../../../../internal/telemetry/otel/v1/logs_test.go),
  [traces](../../../../../../internal/telemetry/otel/v1/traces_test.go),
  [metrics](../../../../../../internal/telemetry/otel/v1/metrics_test.go),
  [context](../../../../../../internal/telemetry/otel/v1/context_test.go).
- [Ownership](../../../../../../internal/telemetry/otel/v1/provider_test.go),
  [dial/connection ownership and native negative controls](../../../../../../internal/telemetry/otel/v1/network_test.go),
  [export failures](../../../../../../internal/telemetry/otel/v1/export_test.go),
  [TLS/mTLS and cleanup controls](../../../../../../internal/telemetry/otel/v1/export_tls_test.go).
- [Actual product combinations](../../../../../../internal/telemetry/otel/v1/integration_test.go):
  three signals through an independently decoding receiver, TLS/gzip,
  resource/scope/context, both loggers' local multi-sink rotation and failure evidence.
- [Consuming binary/build isolation](../../../../../../internal/telemetry/otel/v1/compatibility_test.go).
- [Opt-in Elastic service acceptance](../../../../../../internal/telemetry/otel/v1/elastic_service_test.go),
  built only with `otel_elastic_service`: explicit private HTTPS fixture, isolated
  data streams, actual product/native export, independent indexed-data queries
  and exact-target cleanup. Default retention is false; retaining test data
  requires explicit fixture authorization.

Test-owned protocol/TLS receivers establish only their tested transport boundaries.
Separately authorized Elastic **9.5.3** acceptance verified direct OTLP HTTPS
ingestion and independent queries for logs, spans, int64 counter/gauge and
float64 explicit-histogram data, including both logging bridges. Its existing
exponential-histogram setting accepts cumulative histograms; the backend stores
them in its native histogram representation rather than retaining SDK buckets.
The test verifies values, resource/scope, trace association and cleanup; this is
not a claim about every Elastic version, deployment or authentication
configuration. A separately authorized Playwright check confirmed the six
expected gauge samples in Kibana Discover with an explicit absolute time range;
it does not certify saved dashboards, alerting or all Kibana applications.
No Collector, Logstash, proxy, production retention/availability,
cross-platform filesystem or production performance is certified.
Workflow replay is inapplicable: no Workflow commands,
durable DTOs or Temporal instrumentation change. Working experiments, exact
run manifests, failures and handoff remain in the issue's sibling reference
workspace. Scope provenance: [Issue #34](https://github.com/frost-leo/fathomry/issues/34).
