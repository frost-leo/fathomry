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
| `ConsumerGroup / consumer_group` | Empty disables managed groups. Otherwise 1–128 UTF-8 bytes, no NUL; must differ from OffsetGroup. Subscribes to the declared Topics. |
| `InitialOffset / initial_offset`, `ResetOffset / reset_offset` | Both explicitly required with ConsumerGroup: error, earliest or latest. Must be empty without ConsumerGroup. |
| `MaxGroupSessions / max_group_sessions` | 1; 1–MaxActive concurrent membership clients. Sessions also retain a root work allowance. |
| `MaxAssignments / max_assignments` | 256; 1–4096 per session. Exceeding it terminates the session, not truncates ownership silently. |
| `Routing / routing` | manual; manual or keyed. Keyed requires Message.Partition=-1 and retains actual ACK coordinates. |
| `SASL / sasl` | none; none, PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512. Credential-bearing profiles require verified TLS. |
| `User / user`, `Password / password` | Explicit nonempty UTF-8/no-NUL, at most 256/4096 bytes for SASL; both empty for none. |
| `MaxActive / max_active` | 4; 1–16 concurrent root calls/cursors. Shared source admission, not deployment-wide quota. |
| `QueuedCalls / queued_calls` | 0; 0–64. Zero rejects overload; positive values use bounded shared admission waiting. |
| `MaxRecords / max_records` | 256; 1–4096 per produce/exact-set call or returned page. |
| `MaxRecordBytes / max_record_bytes` | 1 MiB; 1 KiB–4 MiB. Logical per-record charge includes topic/key/value, 128 bytes overhead, and each header's key/value plus 32 bytes. |
| `MaxBatchBytes / max_batch_bytes` | 4 MiB; MaxRecordBytes–16 MiB per call/page's retained logical content. |
| `MaxWireBytes / max_wire_bytes` | 8 MiB; MaxBatchBytes+1024–32 MiB. Native protocol read/write envelope; not decoded bytes. |
| `MaxDecodedBatchBytes / max_decoded_batch_bytes` | 4 MiB; MaxRecordBytes+512–16 MiB, cumulative record bytes across a prepared fetch prefix. |
| `MaxDecodedRecords / max_decoded_records` | 4096; MaxActive*MaxRecords–65536. Bounds decoded records (including aborted/control records) and Fetch aborted-transaction metadata independently of output page size. |
| `Timeout / timeout_ns` | 10 s; 1 s–1 min, whole milliseconds. Separate admission/operation budgets, native dial/request/retry timeout and broker transaction timeout. Not a total hard return deadline. |
| `CleanupTimeout / cleanup_timeout_ns` | 5 s; 1 ms–30 s. Late-delivery grace and authorized native abort cleanup budget. |
| `Retries / retries` | 0; 0–10 nominal native record/request retry limit. Uncertain idempotent delivery and PID/coordinator recovery may exceed the nominal count until fencing. |
| `Linger / linger_ns` | 0; 0–1 s. Explicitly overrides native 10 ms default. |
| `Compression / compression` | none; none/gzip/snappy/lz4/zstd. All five are supported by the common bounded reader. |

Headers are limited to 64 per record and 1024 bytes per key. Keys and values are
opaque bytes; nil versus present-empty is preserved. Timestamps are zero (native
current time) or Unix epoch through the supported int64-nanosecond range, at
millisecond precision. Kafka topic policy can reject or replace client timestamps.

mTLS/client certificates, DNS discovery, KIP-714 client metrics, arbitrary
native hooks, automatic topic creation and dynamic reconfiguration are not selected.
Verified TLS and static SASL are implemented and tested against controlled peers,
not qualified as a deployed service profile by plaintext VM acceptance. The trust
bundle must contain only valid CA certificate PEM blocks and whitespace; junk,
non-CA certificates, unrelated PEM blocks and malformed prefixes reject.
SCRAM challenges are capped at 8 KiB and 4096–65536 PBKDF iterations before SDK
delegation; higher-cost deployments are outside this selected profile. The SDK
still verifies nonces, client proofs and server signatures. No native global
authentication setting or application callback is installed.

## Resource accounting

Producer count/byte buffering is explicitly configured from MaxActive and batch
limits. Shared admission reserves a whole working envelope before copies/SDK work.
Consumers reuse the control client with one explicit partition fetch per operation,
without background prefetch or record pools.

Before native record/header slab allocation, the provider checks complete batch
framing, record/header cardinality, offsets and cumulative decoded bytes. Codec
expansion is prepared once per response. Snappy advertised lengths/chunk totals
are checked before allocation; LZ4 streams through the output bound with a separate
bounded block workspace; zstd caps output and window/workspace explicitly.
LZ4 admits one modern independent-block frame (64 KiB–4 MiB blocks), including
empty uncompressed blocks and optional size/checksums. Legacy, dependent,
dictionary, skippable and concatenated frame profiles refuse before decoding.
The native library still checks frame/block checksums and performs decompression.
A complete safe prefix can be returned
when a later batch would exceed a count/byte budget; an oversized first batch
refuses. These limits do not promise one arbitrary record can be selected out of
an otherwise unsupported batch.

The reservation accounts simultaneous wire/decode copies, native record/header
slabs and retained output metadata. Evidence has a separate reservation, retained
until explicit Inbox release. Error graphs, caller-retained result copies, native
connection/metadata baselines, allocator/GC behavior, and broker memory are not an
RSS cap. Do not equate quantity, encoded bytes, decompressed bytes or resource
reservation with physical process memory.

Fetch v13 response envelopes are checked before the selected kmsg decoder allocates
slices: at most one topic/partition, MaxDecodedRecords abort entries, 64 total tags
including nested tags, and 16 broker hints. This bounds the later native abort map
as well as response arrays. Tag payloads remain wire-bounded and unknown tags are
preserved within this profile. Native timeout, throttle and value decoding remain
owned by the SDK. This is bounded preflight, not an allocation-free or RSS claim.

`LimitsV1` is a composition convenience. With overlays, attach the corresponding
resolved policy, not blindly the bootstrap policy. Bind rejects allowances that
exceed provider concurrency/queue ceilings or cannot cover a working envelope.
A direct/group cursor holds one root allowance and uses a single nested lease for each page/
checkpoint. `MaxLeases >=2` is required. Group reservations include assignment
metadata and membership protocol buffers; codec workspace is separate from output
bytes. `EvidenceBytesV1` reports the defaulted private retained envelope.
Observed group member IDs are bounded to 512 UTF-8 bytes; invalid identities
terminate the session. Revision exhaustion fences it rather than wrapping tokens.
Public composition must additionally reserve persistent native source buffers,
bridge custody and overlapping generations, as the public Adapter's Compose does.

## Independent version axes

- Provider identity / import major: `broker.franz.v1`,
  `internal/broker/franz/v1`.
- Bootstrap/configuration format: OptionsV1, format 1.
- Effective source revision: opaque resource-preparation revision; not a content
  hash, schema version or workflow version.
- Selected SDK modules: franz-go **1.21.6**, kmsg **1.13.1**. Test-only kfake uses
  `v0.0.0-20260911174156-65d23a567563`.
- Native protocol: Metadata 10–12, Fetch 13; standalone and group checkpoints require
  OffsetCommit/OffsetFetch 10. Other native requests negotiate within the pinned
  SDK's stable versions, including transactional feature behavior.
  Groups use classic JoinGroup/SyncGroup and cooperative-sticky; GroupProtocol
  remains the native consumer protocol type, not a classic/KIP-848 switch.
  Dynamic single-member LeaveGroup is exactly v2, because the selected SDK ignores
  v3+ member-level response errors. Incompatible peers refuse rather than downgrade.
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
