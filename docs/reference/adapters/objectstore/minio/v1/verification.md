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

# MinIO Adapter verification

[Documentation](../../../../../README.md) / Public package reference

**Audience:** Adapter maintainers.
**Status:** executable local/protocol, public-consumer and opt-in isolated-service gates.

## Local checks

From the repository root, use the normal Go caches and a disposable job:

```sh
job=$(mktemp -d /home/frost/tmp/codex-build/jobs/build.XXXXXX)
TMPDIR="$job" go test -mod=readonly -race -count=1 -timeout=4m \
  ./adapters/objectstore/... ./internal/objectstore/minio/v7
TMPDIR="$job" go test -mod=readonly -race -count=1 ./cmd/fathomry/internal/app
TMPDIR="$job" GOMAXPROCS=4 go test -run '^$' -fuzz '^FuzzCursorContinuation$' \
  -fuzztime=5s -parallel=2 ./internal/objectstore/minio/v7
TMPDIR="$job" GOMAXPROCS=4 go test -run '^$' -fuzz '^FuzzSettingsPreparation$' \
  -fuzztime=5s -parallel=2 ./adapters/objectstore/minio/v1
```

Tests include actual native HTTP protocol calls, independent peer read-back,
lost completion/failed abort, partial acquisition, blocked input/output/body close,
same-key versions, non-monotonic opaque marker pairs, no-progress/invalid pages,
saturated finalization, evidence retry, strict settings, safe diagnostics and
Fixed/Follow replacement. The separate-module consumer builds and runs direct and
Framework Fixed routes and checks that importing Internal is rejected.

Run formatting, module tidy-diff, full vet/race/build and the offline CLI atlas on
the final candidate. Linux/amd64 runtime qualification does not establish another
platform's runtime behavior; cross-compilation is only build evidence.
No throughput, RSS ceiling, production SLO, AWS compatibility or HA claim follows
from these tests. No Temporal commands or durable payloads change, so replay is
not a substitute for these process-local I/O checks.

## Fresh isolated-service test

`integration_service_test.go` uses the independent `minio_service` build tag.
When selected, `TestMinIOServiceCoreExtensions` requires all three opt-ins and
fails if its fixture is missing; ordinary untagged tests cannot run service writes:

```sh
TMPDIR="$job" \
FATHOMRY_MINIO_TEST_CONFIG=/private/authorized-fixture.yaml \
FATHOMRY_MINIO_TEST_WRITES=1 \
FATHOMRY_MINIO_TEST_BUCKET_ADMIN=1 \
go test -mod=readonly -tags=minio_service -race -count=1 -timeout=4m \
  -run '^TestMinIOServiceCoreExtensions$' -v ./adapters/objectstore/minio/v1
```

Obtain explicit permission first. The private regular YAML file must have no
group/other access and be at most 64 KiB. The fixture reads only
`minio.api_endpoint`, `minio.root.username/password` and
`minio.fathomry.region`. No inventory or credentials are printed or copied.

This **test-only** administrator path creates one random absent bucket, enables
versions only on that bucket, and uses an independently owned SDK client for
read-back/cleanup. No existing bucket, policy, account or service configuration
changes. Only explicitly tracked keys/upload IDs and exact observed versions/
delete markers in this test-owned bucket are removed; unexpected data refuses
cleanup. Finally the empty bucket is removed and absence confirmed.

The workload exercises conditional/known/unknown uploads, serial multipart parts,
inspection/complete/abort, ranges, copy, tags, same-key versions and a delete
marker. Actual GET/HEAD/PUT capabilities are consumed; altered methods/paths/
conditions, missing signed headers and expired URLs must be rejected. Missing
signed headers can be HTTP 400 or 403; independent read-back additionally proves
that the prior content was not overwritten. Local issuer closure is checked not
to revoke an otherwise valid URL.

This verifies an authorized HTTP/versioned MinIO test combination using the
explicit test administrator, not a least-privilege IAM policy, deployed TLS,
static-token expiry, other S3 implementations or production durability. A missing
opt-in, skipped test, historical #38 test or an SDK-only signing probe is not new
Adapter service acceptance. Exact session commands/results and the tested
worktree manifest belong in the #104 reference material.
