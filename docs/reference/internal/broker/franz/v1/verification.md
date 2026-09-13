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

# Kafka verification and service profile

[Documentation](../../../../../README.md) / [Kafka interface](interface.md)

**Audience:** maintainers running acceptance and evaluating support.
**Status:** executable unit/SDK-combination and opt-in service tests; broader
deployment guarantees remain unverified.

## Run from the product repository root

Local tests start their own in-process kfake brokers, not a Docker stack or an
existing deployment. They observe fixture listener closure directly; they perform
no port probing/scanning. Real-service tests are skipped unless explicitly enabled.

```sh
go test -race -count=1 -timeout=3m ./internal/broker/franz/v1/...
go test -race -count=5 -shuffle=on -timeout=3m ./internal/broker/franz/v1/...
GOMAXPROCS=4 go test -run '^$' -fuzz '^FuzzRecordPayloadRoundTrip$' \
  -fuzztime=5s -parallel=2 ./internal/broker/franz/v1
GOMAXPROCS=4 go test -run '^$' -bench '^BenchmarkProduceBatch$' \
  -benchtime=50x -benchmem ./internal/broker/franz/v1
```

The integration fixture exercises the actual Select → resource.Assemble → Bind →
invocation → native SDK → broker → independently observed data path. It includes
the reference record's own location, interleaved/late attempts, explicit empty
data/reference delivery, failed reference publication and independently retained
effects with saturated/unavailable diagnostics. Its reference DTO is test-owned;
passing it does not implement the framework protocol.

## Owner-authorized Kafka service

Start the intended test Kafka using its owner's existing deployment procedure.
The tests **do not** start/restart the VM, change server settings, or deploy
infrastructure. They wait up to 30 seconds using the Kafka Metadata API.

Supply a private regular YAML file (at most 64 KiB, no group/other permissions):

```yaml
kafka:
  bootstrap_servers: "<explicit-test-IP>:9092"
  security_protocol: PLAINTEXT
  auth: none
```

The private local connection-information file can include other service sections;
the test reads only Kafka settings and never prints endpoint/credential values.
Use an environment variable containing its path, not embedded credentials.

```sh
FATHOMRY_KAFKA_TEST_CONFIG="$PRIVATE_KAFKA_CONFIG" \
FATHOMRY_KAFKA_TEST_WRITES=1 \
go test -race -v -count=1 -timeout=3m \
  -run '^TestKafkaServiceIntegration$' ./internal/broker/franz/v1
```

The explicit write flag authorizes new random `fathomry-gh36-*` topics and a
matching standalone checkpoint group. The fixture:
- checks exact generated names are absent before creation;
- creates two-partition RF1 topics with CreateTime, delete cleanup and
  seven-day retention configuration;
- uses only these topics for data/reference, transactions, direct consumption,
  explicit checkpoint commits and prefix deletion;
- registers cleanup before creation to inspect lost creation responses;
- verifies incarnation IDs before deletion, deletes owned topics by ID, and
  observes their absence; the checkpoint group is deleted and absence checked;
- closes all test-owned native clients and managed assemblies.

The existing service remains running. No existing topic, ACL, business group,
offset or retention setting is modified. Kafka's internal log compaction and
inactive producer-ID/transaction metadata retention remain broker-owned; logical
resource deletion is not an assertion of physical byte erasure.

## What each layer demonstrates

| Evidence | Claims and limits |
| --- | --- |
| Options/errors/record tests | Strict versions/defaults/overlays, copies and null/empty/duplicate-header fidelity, original causes and restricted runtime presentation |
| Fetch/read tests | Exact offsets versus compaction holes, deleted-prefix/future-offset distinction, topic recreation, committed isolation, bounded prefixes and explicit refusal; local framing/codec controls |
| Producer/lifecycle/transaction tests | Partial ACKs, retry budgets, late completion, inbox/admission saturation, producer fencing while the fault remains active, no false commit, separate cleanup failure, unknown duplicate-sequence offsets |
| Consumer/checkpoint tests | Nested single-root cursor, final-page commit, cancellation between operations, no close-time autocommit, explicit checkpoint absence and prefix-versus-processing counterexample |
| Integration and optional bridge | Real shared mechanisms/native SDK/evidence path; explicit W3C context survives Kafka header round trip without native plugins |
| Existing VM service | Real Kafka data/reference reads, cross-partition transaction commit/abort, deleted-prefix refusal, direct consumer and standalone v10 checkpoints, consumer lifetime timeout/source reuse, compressed expansion refusal, resource cleanup |
| Benchmark/fuzz | Bounded valid payload round trips; a fixed local kfake batch workload with equal ACK/identity/evidence guarantees, allocations and latency samples—not production capacity |

The service acceptance uses the owner-selected **plaintext/no-auth, one advertised
broker, RF1 test-topic** profile. Its exact broker release is not available from
the tested Kafka APIs and is not inferred from protocol support. Prior literature's
Apache Kafka 4.3.1 single-node experiments are separate native evidence, not a
version attestation for this VM or a substitute for these product tests.

Real service tests do not inject server/network/coordinator failures or restart
the service. Such failure transitions are currently verified with controlled
kfake responses, so they are not real-service failover certification. Kfake is
not Apache Kafka: known timestamp/control-marker differences remain relevant.

Not qualified: TLS/SASL/mTLS deployment, multiple replicas/ISR loss, leader or
coordinator failover, broker restart/crash recovery, other broker products,
consumer-group rebalances, cross-service/group EOS, seven-day duration guarantees,
million-target/whole-process resource capacity, and complete workflow recovery.
No Temporal commands or durable schemas change here; replay is not applicable.

Use standard repository format/tidy/vet/build/full-race checks as well. The
per-issue reference workspace records exact commands, source manifests, review
findings, failures and toolchain results; it is not a runtime dependency.
