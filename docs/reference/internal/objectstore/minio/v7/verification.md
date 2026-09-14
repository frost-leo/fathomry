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

# MinIO verification and service boundaries

[Documentation](../../../../../README.md) / [MinIO interface](interface.md)

**Audience:** maintainers reproducing acceptance and evaluating support.
**Status:** executable local SDK/protocol and opt-in real-service fixtures.
A skipped real-service test is not acceptance.

## Run local checks

From the repository root, with the intended Go toolchain's `bin` first in PATH:

```sh
go test -race -count=1 -timeout=3m ./internal/objectstore/minio/v7
go test -race -count=3 -shuffle=on -timeout=3m ./internal/objectstore/minio/v7
GOMAXPROCS=4 go test -run '^$' -fuzz '^FuzzReadInputAndResponseBound$' \
  -fuzztime=5s -parallel=2 ./internal/objectstore/minio/v7
go run ./internal/objectstore/minio/v7/testdata/consumer
go mod tidy -diff
go vet ./...
go test -race -count=1 -timeout=8m ./...
go build ./...
```

Local peers use bounded synthetic data and loopback HTTP/TLS, not a deployed S3
service or a new container. The integration path performs real configuration
preparation, assembly, binding, invocation admission, native SDK requests,
independent peer read-back and evidence reception with failed diagnostics.

| Tests | Observable coverage |
| --- | --- |
| Configuration/conformance | Defaults, invalid/unknown modes with no I/O, frozen overlays/maps, duplicate preflight, independent sources, credential separation, borrowing, private formatting |
| Upload/wire | Single/serial multipart and empty/unknown-size controls; actual final conditions; independent hashes under concurrent calls; bounded exact input; no streaming signer or native retry amplification |
| Read/metadata | Full checksum corruption, expected digest, ranges and exact versions, ignored-range/version refusal, metadata copies, successful empty versus missing, partial sinks |
| List/remove/copy | Output and aggregate response/exchange limits; explicit terminal flags and XML roots; versions/delete markers, per-target partial removals, bounded upload/part inspection, unsafe upload-cursor refusal, opaque copy-version encoding and source conditions |
| Lifecycle/failure | Blocked caller reader/body close/dial holds admission; late dial callbacks are fenced; request-body closure precedes buffer reuse; abort and late close failures survive; dropped completion still has independently observed effects |
| Consumer/fuzz | Actual consuming executable selects MinIO v7.3.0; bounded input/response byte arithmetic and malformed/EOF-bearing terminal outcomes |

`TestNativeCopyOptionsRequireExplicitEncoding` intentionally observes a native
limitation. Its PASS is not qualification; the adjacent integration test must
preserve the requested opaque version and condition on the actual request.
Historical native probes/race failures remain in the issue reference workspace,
not rewritten into passing integration claims.

## Real-service authorization and fixture

The real test is disabled by default. Before enabling it, confirm:
- the exact service/version and active account policy through read-only checks;
- exclusive ownership of a fresh random `fathomry-gh38-<96-bit-token>/` prefix
  in the already existing configured bucket;
- authority for at most 32 test objects and at most 128 MiB cumulative fixture
  writes, including staged parts and copies;
- no concurrent administrative/versioning changes during this isolated test;
- permission to inspect and clean only this run's exact objects and upload IDs.

The fixture never creates a bucket, changes account permissions, alters server/
bucket configuration, enables versioning/retention, or deletes arbitrary prefix
contents. It refuses to write unless a read-only preflight observes that bucket
versioning has never been enabled. No version/tag/encryption service claim is
made by this unversioned basic-object profile.

Supply a private, regular YAML file (at most 64 KiB, no group/other permission).
Only the following MinIO fields are extracted; other services are ignored:

```yaml
minio:
  api_endpoint: "http://<authorized-literal-IP>:9000"
  root:
    username: "<read-only versioning preflight account>"
    password: "<private>"
  fathomry:
    access_key: "<dedicated object-test account>"
    secret_key: "<private>"
    bucket: "<existing authorized bucket>"
    region: "<explicit region>"
```

The historical `root` field name is a fixture input convention, not permission
for administrative writes. Those credentials are used **only** for
GetBucketVersioning. All object activity and independent read-back/cleanup use
the dedicated account. The configured HTTP profile is explicit and not a claim
of credential confidentiality. Production TLS requires separate qualification.

Only after the owner authorizes the described scope:

```sh
FATHOMRY_MINIO_TEST_CONFIG="$PRIVATE_MINIO_CONFIG" \
FATHOMRY_MINIO_TEST_WRITES=1 \
go test -race -v -count=1 -timeout=3m \
  -run '^TestMinIOServiceIntegration$' ./internal/objectstore/minio/v7
```

The write flag records authorization; do not set it on the owner's behalf
merely because credentials exist. A path without the flag fails rather than
silently writing. The test checks namespace absence, registers cleanup before
mutations, tags every owned object with a per-run metadata marker, records exact
upload IDs, tests conditional multipart rejection and abort, reads content
independently and inspects missing/deleted results.

Cleanup checks the object marker and absence of an unexpected historical
version, deletes only the exact recorded keys, aborts only recorded upload IDs,
then independently checks object and upload absence. Unconfirmed ownership or
residual resources fail the test rather than authorizing recursive deletion.
A process crash can prevent Go test cleanup; retain the private run record for
owner-authorized recovery, never adopt preexisting resources.

`TestServiceFixtureAgainstProtocolPeer` executes the same fixture against the
local peer to test fixture logic. It is explicitly **not** real-service evidence.

Owner-authorized real-service runs exercise the existing HTTP, static dedicated
account and never-versioned bucket profile. They cover single/unknown-empty and
multipart uploads, final-condition rejection, expected-digest reads/downloads,
exact ranges, copy, listing, exact deletion, failed-reader abort, independent
evidence and independently confirmed cleanup. The local issue record contains
the server-reported release, exact worktree/toolchains and run logs; that report
is not binary/deployment attestation or qualification of other distributions.

The owner-requested agent review added rejecting regressions after the first
service happy-path pass. Final acceptance must run the corrected worktree, not
reuse that earlier service log. Agent review is not independent human approval.

## Evidence not supplied

No AWS/AIStor distribution, server restart/durability/failover, conditional-write
linearizability under concurrent external actors, deployed TLS/KMS/retention,
production throughput/RSS, maximum multipart capacity or RDMA qualification.
Local forced transport faults do not certify real-service failure transitions.
No Temporal command or durable format changes: replay is not applicable.

The issue's local verification record owns exact commands, toolchain versions,
dirty-worktree/source hashes, original failures and fixes. SDK v7.3.0, options
format 1, framework source revision and reported service version remain separate
facts. Build metadata and a clean vulnerability scan are not compatibility or
security certificates.
