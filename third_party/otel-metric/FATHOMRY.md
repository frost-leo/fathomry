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

# Exact integral histogram bucket comparison

This is the narrowly scoped, owner-authorized local correction for Fathomry
issue #128, compatibility revision `v1`. Its upstream remains
`go.opentelemetry.io/otel/sdk/metric v1.47.0`, origin
`66cfc9520e205b7d450183532772401bc2b6674c`. [UPSTREAM.json](UPSTREAM.json)
records the original module/archive checksums and all original file hashes.
The original Apache-2.0 [LICENSE](LICENSE) and source notices remain unchanged;
first-party additions do not relicense upstream material.

## Correction and limits

Upstream 1.47.0 fixes integral sum accumulation, but its explicit histogram
bucket search still converts every measurement to `float64`. An `int64`
measurement `9007199254740993` is then incorrectly placed below the inclusive
boundary `9007199254740992`. The same wrong integer bucket counts appear in
native collection and OTLP export. This is not the protocol's intentional
floating-point representation of histogram sum and extrema.

The only modified upstream production file is
`internal/aggregate/histogram.go`. Both cumulative and delta histogram searches
now compare an integral measurement with the floor of each finite boundary.
Boundaries at or above `2^63` include every `int64`; boundaries below `-2^63`
include none. The remaining conversion is in range and satisfies
`integer <= boundary` exactly when `integer <= floor(boundary)`. No admitted
measurement is rounded, changed or narrowed. The `float64` search path is the
original upstream operation. No instrument, aggregation state, temporality,
cardinality, exemplar, concurrency, export or public SDK API is replaced.

Histogram OTLP sum/min/max remain protocol-defined `double` fields. This patch
does not promise arbitrary-precision sums, change integer overflow semantics,
or add non-finite boundaries to Fathomry's accepted profile.

For standalone module use, `go.mod` omits six upstream monorepo-relative
replacement directives. Its exact dependency requirements are unchanged;
`go.sum` adds the corresponding original module checksums. This directory
contains only the metric module; the independent OTLP trace conversion correction
has separate provenance under `../otel-trace`. The repository's root replacement is local only:
independent consumers need the immutable replacement supplied by the existing
CLI dependency policy, because root replacements are not inherited and nested
modules are excluded from the parent module ZIP.

## Qualification and retirement

`internal/aggregate/fathomry_histogram_test.go` covers both temporalities,
repeated collection, positive/negative `2^53` neighbors, fractional negative
boundaries, `int64` extrema and unchanged float search behavior. An independent
`math/big.Rat` oracle compares exact integers with exact IEEE-754 boundaries;
deterministic random controls and the fuzz target use no float conversion of
the measurement. Benchmarks retain the incorrect upstream search as a labeled
cost baseline, not a correctness alternative.

From this module directory:

```sh
go test -race ./...
go test ./internal/aggregate -run '^$' -fuzz '^FuzzFathomryHistogramBucketExactness$' -fuzztime=20s
go test ./internal/aggregate -run '^$' -bench '^BenchmarkFathomryHistogramBucket$' -benchmem
```

Fathomry's provider tests additionally qualify native collection and independently
decoded OTLP integer fields on the actual consuming graph. Keep pre-correction
failures distinct from these passing checks. Retire this replacement only after
an explicitly selected upstream release passes the same exact comparison,
temporality, wire, consumer-graph and lifecycle qualification. This is neither
an upstream release nor backend/deployment certification.
