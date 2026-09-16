// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

module github.com/frost-leo/fathomry

go 1.27.0

require (
	github.com/andybalholm/brotli v1.2.1
	github.com/apache/arrow-go/v18 v18.6.0
	github.com/apache/iceberg-go v0.6.0
	github.com/aws/aws-sdk-go-v2 v1.41.7
	github.com/aws/aws-sdk-go-v2/credentials v1.19.16
	github.com/aws/aws-sdk-go-v2/service/s3 v1.101.0
	github.com/aws/smithy-go v1.25.1
	github.com/bogdanfinn/fhttp v0.6.9
	// TODO(gh-49): Recheck tls-client compatibility before SDK upgrades or framework
	// releases. Retire local fixes only after an official candidate passes request
	// isolation, racing winner/loser cleanup, cancellation, body integrity, proxy
	// isolation, canonical certificate pins, encoded framing, reconnect, profile
	// factories and native-extension regression gates. Preserve Fathomry contracts
	// and exact upstream/local-patch provenance; do not rely on a tag or
	// release note alone. Track evidence: https://github.com/frost-leo/fathomry/issues/49
	// Upstream BSD-4-Clause / GPL compatibility remains unresolved; implementation
	// delivery and preserving LICENSE do not establish distribution permission.
	github.com/bogdanfinn/tls-client v1.16.0
	github.com/bogdanfinn/utls v1.7.8-barnius
	github.com/chromedp/cdproto v0.0.0-20260714215040-dc233986426f
	github.com/chromedp/chromedp v0.16.0
	github.com/duckdb/duckdb-go-bindings v0.10505.0
	github.com/duckdb/duckdb-go/v2 v2.10505.0
	github.com/enetx/g v1.1.0
	github.com/enetx/http v1.0.29
	github.com/enetx/http2 v1.0.26
	github.com/enetx/http3 v1.0.9
	// TODO(gh-50): Requalify Surf and its consumed transport graph before upgrades.
	// Keep the local trust, copying, replay, framing, callback and release controls
	// until an upstream candidate passes their rejecting and independent tests.
	github.com/enetx/surf v1.0.206
	github.com/go-json-experiment/json v0.0.0-20260623181947-01eb4420fa68
	github.com/go-sql-driver/mysql v1.10.1
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.11.0
	github.com/jackc/puddle/v2 v2.2.2
	github.com/klauspost/compress v1.19.2
	github.com/minio/minio-go/v7 v7.3.0
	// TODO(gh-21): Recheck this pin and lifecycle compatibility before Nacos upgrades
	// or framework releases. An official stable SDK must fix BOTH registry shutdown
	// races and saturated-listener shutdown. Retire only workarounds made unnecessary
	// by passing the native counterexamples, race/isolation/privacy and real-service
	// read/listen/reconnect/close gates; preserve Fathomry's contracts. Track evidence:
	// https://github.com/frost-leo/fathomry/issues/21
	github.com/nacos-group/nacos-sdk-go/v2 v2.3.5
	github.com/quic-go/quic-go v0.61.0
	github.com/refraction-networking/utls v1.8.3-0.20260623165621-880e27d8b0e5
	github.com/rs/zerolog v1.35.1
	github.com/sardanioss/http v1.2.0
	// TODO(gh-51): Recheck the HTTPcloak, QUIC, HTTP/2 and UDP relay replacements
	// before dependency upgrades or framework releases. Retire only fixes made
	// unnecessary by passing native and Provider framing, input/instance isolation,
	// replay, callback/cancellation, socket-release and cold-cache regression gates.
	// Preserve exact source/license provenance and the independent API/configuration
	// and local compatibility versions. A new tag is not qualification evidence.
	// Track: https://github.com/frost-leo/fathomry/issues/51
	github.com/sardanioss/httpcloak v1.7.2
	github.com/sardanioss/net v1.2.10
	github.com/sardanioss/quic-go v1.2.29
	github.com/sardanioss/udpbara v1.1.0
	github.com/spf13/viper v1.21.0
	github.com/tam7t/hpkp v0.0.0-20160821193359-2b70b4024ed5
	github.com/trinodb/trino-go-client v0.333.0
	github.com/twmb/franz-go v1.21.6
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260911174156-65d23a567563
	github.com/twmb/franz-go/pkg/kmsg v1.13.1
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.22.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.46.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.46.0
	go.opentelemetry.io/otel/log v0.22.0
	go.opentelemetry.io/otel/metric v1.46.0
	go.opentelemetry.io/otel/sdk v1.46.0
	go.opentelemetry.io/otel/sdk/log v0.22.0
	go.opentelemetry.io/otel/sdk/metric v1.46.0
	go.opentelemetry.io/otel/trace v1.46.0
	go.opentelemetry.io/proto/otlp v1.11.0
	go.uber.org/zap v1.28.0
	go.yaml.in/yaml/v3 v3.0.5
	golang.org/x/net v0.58.0
	golang.org/x/sys v0.47.0
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
)

require (
	atomicgo.dev/cursor v0.2.0 // indirect
	atomicgo.dev/keyboard v0.2.9 // indirect
	atomicgo.dev/schedule v0.1.0 // indirect
	cloud.google.com/go v0.123.0 // indirect
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/apache/thrift v0.23.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.10 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.32.17 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.42.1 // indirect
	github.com/bdandy/go-errors v1.2.2 // indirect
	github.com/bdandy/go-socks4 v1.2.3 // indirect
	github.com/bogdanfinn/quic-go-utls v1.0.10-utls // indirect
	github.com/bogdanfinn/websocket v1.5.6-barnius // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/chromedp/sysutil v1.1.0 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/cloudflare/circl v1.6.2 // indirect
	github.com/cockroachdb/apd/v3 v3.2.1 // indirect
	github.com/containerd/console v1.0.5 // indirect
	github.com/creasty/defaults v1.8.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-amd64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-arm64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-amd64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-arm64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/windows-amd64 v0.10505.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.4.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/goccy/go-yaml v1.17.1 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/gookit/color v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.30.0 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/lithammer/fuzzysearch v1.1.8 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.23 // indirect
	github.com/miekg/dns v1.1.69 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/pelletier/go-toml/v2 v2.3.1 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/pterm/pterm v0.12.83 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/sagikazarmark/locafero v0.11.0 // indirect
	github.com/sardanioss/qpack v0.6.3 // indirect
	github.com/sardanioss/utls v1.10.5 // indirect
	github.com/sourcegraph/conc v0.3.1-0.20240121214520-5f936abd7ae8 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/substrait-io/substrait v0.87.0 // indirect
	github.com/substrait-io/substrait-go/v8 v8.1.0 // indirect
	github.com/substrait-io/substrait-protobuf/go v0.85.0 // indirect
	github.com/tinylib/msgp v1.6.4 // indirect
	github.com/twmb/avro v1.7.2 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/wzshiming/socks5 v0.7.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.46.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/exp v0.0.0-20260218203240-3dfff04db8fa // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/term v0.45.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260819154853-08b0e4226688 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260819154853-08b0e4226688 // indirect
	gopkg.in/ini.v1 v1.67.3 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/bogdanfinn/tls-client => ./third_party/tls-client

replace github.com/sardanioss/httpcloak => ./third_party/httpcloak

replace github.com/sardanioss/quic-go => ./third_party/httpcloak-quic-go

replace github.com/sardanioss/udpbara => ./third_party/udpbara

replace github.com/sardanioss/net => ./third_party/httpcloak-net

replace github.com/enetx/surf => ./third_party/surf

replace github.com/enetx/http2 => ./third_party/surf-http2

replace github.com/enetx/http3 => ./third_party/surf-http3
