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

# Kafka options, bounds and versions

[Documentation](../../../../../README.md) / [Kafka interface](interface.md)

**Audience:** composition and integration maintainers.
**Status:** implemented `OptionsV1`; no application configuration loader or reload.
**Source:** [options.go](../../../../../../internal/broker/franz/v1/options.go).

## Preparation and authorized inputs

`OptionsV1.Version` zero or one selects configuration format 1. Unknown versions
reject before construction. `Name` is the source identity, not an overlay setting.

Preparation reuses resource's existing strict precedence:
defaults < base < environment < local < variables. Objects merge recursively,
lists replace, explicit empty lists/strings override, and unknown/duplicate fields
reject even if a later layer would overwrite them. Layers do not read environment
variables. Bootstrap zero defaults are applied only before overlays; explicit
layer zero does not reapply defaults.

The preparation copies input; do not mutate options/layer bytes concurrently with
Select. Reused selections yield independent native clients and settings in each
assembly. There are no native options, mutable TLS/SASL objects, callbacks, custom
dialers, process environment reads, globals or credential refresh hooks.

| Go option / layer key | Default and accepted meaning |
| --- | --- |
| `Brokers / brokers` | Required 1–16 unique canonical literal `IP:port` pairs; nonzero ports, no unspecified/zone addresses. This is both bootstrap and the complete permitted endpoint set. Discovered broker endpoints must equal it; no DNS or automatic authority expansion. |
| `ClusterID / cluster_id` | Required nonempty UTF-8, no NUL, at most 128 bytes; checked on control and producer metadata. Not cryptographic authentication. |
| `Topics / topics` | Required 1–32 unique Kafka names, 1–249 bytes, letters/digits/`._-`, excluding `.` and `..`. At most 4096 discovered partitions in total. |
| `Plaintext / plaintext` | False; true explicitly permits unencrypted transport to the supplied endpoints. No authentication is implied. |
| `RootCAPEM / root_ca_pem` | Required 1–64 KiB explicit trust bundle when plaintext=false. TLS >=1.2, peer IP verification; no system-root fallback. Must be absent with plaintext=true. |
| `TransactionalID / transactional_id` | Empty disables transactional producer. Otherwise 1–128 UTF-8 bytes, no NUL; composition must grant exclusive ownership across processes. |
| `OffsetGroup / offset_group` | Empty disables checkpoints. Otherwise 1–128 UTF-8 bytes, no NUL; exclusively owned standalone group, not active membership/rebalancing. |
| `MaxActive / max_active` | 4; 1–16 concurrent root calls/cursors. Shared source admission, not deployment-wide quota. |
| `QueuedCalls / queued_calls` | 0; 0–64. Zero rejects overload; positive values use bounded shared admission waiting. |
| `MaxRecords / max_records` | 256; 1–4096 per produce/exact-set call or returned page. |
| `MaxRecordBytes / max_record_bytes` | 1 MiB; 1 KiB–4 MiB. Logical per-record charge includes topic/key/value, 128 bytes overhead, and each header's key/value plus 32 bytes. |
| `MaxBatchBytes / max_batch_bytes` | 4 MiB; MaxRecordBytes–16 MiB per call/page's retained logical content. |
| `MaxWireBytes / max_wire_bytes` | 8 MiB; MaxBatchBytes+1024–32 MiB. Native protocol read/write envelope; not decoded bytes. |
| `MaxDecodedBatchBytes / max_decoded_batch_bytes` | 4 MiB; MaxRecordBytes+512–16 MiB, cumulative record bytes across a prepared fetch prefix. |
| `MaxDecodedRecords / max_decoded_records` | 4096; MaxActive*MaxRecords–65536. Counts all decoded records, including aborted/control records, independently of output page size. |
| `Timeout / timeout_ns` | 10 s; 1 s–1 min, whole milliseconds. Separate admission/operation budgets, native dial/request/retry timeout and broker transaction timeout. Not a total hard return deadline. |
| `CleanupTimeout / cleanup_timeout_ns` | 5 s; 1 ms–30 s. Late-delivery grace and authorized native abort cleanup budget. |
| `Retries / retries` | 0; 0–10 nominal native record/request retry limit. Uncertain idempotent delivery and PID/coordinator recovery may exceed the nominal count until fencing. |
| `Linger / linger_ns` | 0; 0–1 s. Explicitly overrides native 10 ms default. |
| `Compression / compression` | `none`; `none` or `gzip`. Reading accepts these codecs only. |

Headers are limited to 64 per record and 1024 bytes per key. Keys and values are
opaque bytes; nil versus present-empty is preserved. Timestamps are zero (native
current time) or Unix epoch through the supported int64-nanosecond range, at
millisecond precision. Kafka topic policy can reject or replace client timestamps.

SASL, mTLS/client certificates, DNS discovery, KIP-714 client metrics, arbitrary
native hooks, automatic topic creation and dynamic reconfiguration are not selected.
Verified TLS transport is implemented but no TLS service profile is qualified by
the plaintext VM acceptance.

## Resource accounting

Producer count/byte buffering is explicitly configured from MaxActive and batch
limits. Shared admission reserves a whole working envelope before copies/SDK work.
Consumers reuse the control client with one explicit partition fetch per operation,
without background prefetch or record pools.

Before native record/header slab allocation, the provider checks complete batch
framing, record/header cardinality, offsets and cumulative decoded bytes. None/gzip
expansion is prepared once per response. A complete safe prefix can be returned
when a later batch would exceed a count/byte budget; an oversized first batch
refuses. These limits do not promise one arbitrary record can be selected out of
an otherwise unsupported batch.

The reservation accounts simultaneous wire/decode copies, native record/header
slabs and retained output metadata. Evidence has a separate reservation, retained
until explicit Inbox release. Error graphs, caller-retained result copies, native
connection/metadata baselines, allocator/GC behavior, and broker memory are not an
RSS cap. Do not equate quantity, encoded bytes, decompressed bytes or resource
reservation with physical process memory.

`LimitsV1` is a composition convenience. With overlays, attach the corresponding
resolved policy, not blindly the bootstrap policy. Bind rejects allowances that
exceed provider concurrency/queue ceilings or cannot cover a working envelope.
A cursor holds one root allowance and uses a single nested lease for each page/
checkpoint. `MaxLeases >=2` is required.

## Independent version axes

- Provider identity / import major: `broker.franz.v1`,
  `internal/broker/franz/v1`.
- Bootstrap/configuration format: OptionsV1, format 1.
- Effective source revision: opaque resource-preparation revision; not a content
  hash, schema version or workflow version.
- Selected SDK modules: franz-go **1.21.6**, kmsg **1.13.1**. Test-only kfake uses
  `v0.0.0-20260911174156-65d23a567563`.
- Native protocol: Metadata 10–12, Fetch 13; standalone checkpoints require
  OffsetCommit/OffsetFetch 10. Other native requests negotiate within the pinned
  SDK's stable versions, including transactional feature behavior.
- Service release/profile: observed separately; a successful API request cannot
  establish an exact broker release, replication/failover or retention SLA.
- Durable data/reference and workflow/mode schemas: not defined here.

`Profile` uses fresh, non-secret fields from actual prepared settings.
`compatibility.Inspect` reads the consuming binary; the executable build test
checks real selected franz-go/kmsg versions. `Assess` with no service evidence
does not declare tested support. Module checksums are not deployment attestation.

Pinned native contracts:
[configuration](https://github.com/twmb/franz-go/blob/b8814509d953ce8aa241a05a9c8c8fea4d259118/pkg/kgo/config.go),
[transaction lifetime](https://github.com/twmb/franz-go/blob/b8814509d953ce8aa241a05a9c8c8fea4d259118/pkg/kgo/txn.go),
[batch timestamp timeout](https://github.com/twmb/franz-go/blob/b8814509d953ce8aa241a05a9c8c8fea4d259118/pkg/kgo/sink.go).
