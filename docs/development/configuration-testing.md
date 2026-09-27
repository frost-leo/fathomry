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

# Verify public configuration

[Documentation](../README.md) / Development guide

**Audience:** maintainers qualifying configuration changes.
**Status:** executable tests and bounded support profiles; not a production certificate.

## Local checks

From the repository root, select the module's actual toolchain before restricting
automatic selection. On the maintained workstation use the approved scratch area:

```sh
toolchain=$(go env GOROOT)
export PATH="$toolchain/bin:$PATH" GOTOOLCHAIN=local GOWORK=off
job=$(mktemp -d /home/frost/tmp/codex-build/jobs/build.XXXXXX)
export TMPDIR="$job"
go test -race -count=1 -timeout=3m ./adapters/... ./framework/... \
  ./internal/resource ./internal/configsource/...
go test -count=1 -run TestIndependentConfigurationConsumers -v \
  ./framework/configuration/v1
```

The independent test copies maintained consumer sources into an unrelated module.
It exercises the actual new public local/remote Capture/Observe and Load/Watch,
not a private factory. It also compiles rejecting generic tag-conversion controls
and a positive alias, rejects private imports and checks pure/local dependency
closure. Its extra module requirements pin only the repository's existing cached
transitive test dependencies for offline tidy; it adds no runtime replacements.

Focused lifecycle tests use controlled sources for deterministic slow-validation,
supersession, status-only recovery, concurrent waits, partial startup and timed-out
then joined cleanup. These are complementary to real Adapter consumers, not a
substitute. Run bounded parser/raw fuzz targets with scratch caches:

```sh
go test ./internal/resource -run '^$' -fuzz '^FuzzDocumentFormat$' -fuzztime=5s -parallel=2
go test ./internal/resource -run '^$' -fuzz '^FuzzJSONVariables$' -fuzztime=5s -parallel=2
go test ./adapters/configsource/internal/owned -run '^$' -fuzz '^FuzzBatchBoundary$' -fuzztime=5s -parallel=2
```

Use the existing [repository checks](testing.md) afterward. Keep sanitized evidence
and exact candidate manifests in the issue reference directory, never only in
expiring build caches. Changes to ordinary process-local configuration do not
establish Temporal replay or durable-serialization compatibility.

The module selects Go 1.27.0. Strict JSON boundaries use its standard
`encoding/json/jsontext` decoder; the nondefault `GOEXPERIMENT=nojsonv2` build
profile disables that package and is not supported. Consumer regressions cover
literal/JSON Unicode fidelity, invalid wire escapes, revoked-token recovery,
multi-attempt RPC causes and opaque caller cancellation hooks.

## Authorized real Nacos test

The public consumer's `TestAuthorizedNacosService` skips unless
`FATHOMRY_CONFIGURATION_NACOS_TEST_CONFIG` explicitly names an authorized fixture.
Do not discover a service or credentials automatically. The fixture must be a
regular mode-0600 file, at most 64 KiB, containing:

- `http_url`, `grpc_address`, `namespace`;
- reader `username`/`password`;
- `admin_username`/`admin_password` used only to manage test-owned keys;
- `root_ca_pem`, `allow_insecure`, `allow_writes`, `recorded_version`.

`allow_writes` must be true. Credentials/addresses must never enter source,
command-line values, logs, reference manifests or public artifacts. Pass only
the protected file path. The test HTTP transport explicitly disables ambient
proxies and redirects. The production Adapter independently has the same
no-ambient-proxy policy.

```sh
FATHOMRY_CONFIGURATION_NACOS_TEST_CONFIG=/path/to/private-fixture.json \
  go test -count=1 -timeout=4m -run '^TestIndependentConfigurationConsumers$' -v \
  ./framework/configuration/v1
```

The test first establishes absence of three cryptographically unique
`fathomry-gh96-*` keys. It registers cleanup before writing. Only those keys are
published/updated/deleted, and final Missing is checked through fresh public
reader and admin Captures. A bounded, owned local TCP forwarding fixture breaks
only the test's own gRPC sessions to exercise real re-registration and recovery.
It never restarts or changes the server, accounts, namespace or network policy.
That disconnect fixture currently requires the explicitly selected plaintext
service profile; TLS is tested separately with loopback fixtures.

Test-only REST management follows the
[official Nacos Admin API](https://nacos.io/en/docs/latest/manual/admin/admin-api/).
Those operations are not public Fathomry configuration-management features.

## Recorded qualification boundaries

The owner-authorized isolated Nacos 3.2.4 plaintext/password profile was exercised
through the independent public consumer. Native push completed before its explicit
five-minute reconciliation interval; disconnect recovery needed no later
publication. Malformed updates retained old typed data, deletion/recreation and
optional absence worked, anonymous reads/read-only writes were denied, and the
three generated keys were independently confirmed absent after cleanup.

That server refused empty publication (HTTP 400, code 10000). The Adapter's
present-empty raw acquisition contract is separately verified with protocol
fixtures; successful empty service publication is not claimed.
TLS/password verification, invalid protocol/size cases and typed RPC deadlines
have loopback evidence, not production TLS certification.

Local live behavior is qualified on Linux stable regular files, literal symlink
resolution and same-filesystem atomic replacement. No network-filesystem,
cross-filesystem transaction or Windows runtime guarantee is asserted.
In-place writes can expose valid intermediate documents; polling/debounce cannot
turn them into committed writer transactions.

Module/API versions, document FormatVersion, state cursors, source generations,
random preparation revisions, SDK v2.3.5 and service version 3.2.4 are distinct.
No multi-node/production deployment attestation, fleet quota, performance SLO,
client activation, real workflow execution or replay qualification is implied.
