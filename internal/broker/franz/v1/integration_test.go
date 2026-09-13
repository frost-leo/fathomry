/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package franz

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.yaml.in/yaml/v3"
)

// These DTOs belong ONLY to this integration fixture. They demonstrate the
// technical data/reference path, not an accepted framework publication protocol.
type fixtureAddress struct {
	Cluster   string
	Topic     string
	TopicID   [16]byte
	Partition int32
	Offset    int64
}
type fixtureReference struct {
	Version   int
	Attempt   string
	Empty     bool
	Positions []fixtureAddress
}

func fixtureWire(position Position) fixtureAddress {
	return fixtureAddress{position.ClusterID, position.Topic, position.TopicID, position.Partition, position.Offset}
}
func (address fixtureAddress) position() Position {
	return Position{ClusterID: address.Cluster, Topic: address.Topic, TopicID: address.TopicID, Partition: address.Partition, Offset: address.Offset}
}

func runDataReferenceIntegration(t *testing.T, options OptionsV1) {
	t.Helper()
	fixture := bindFixture(t, options, 64)
	observer, err := invocation.NewObserver(1)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(fixture.assembly, fixture.selected, fixture.inbox, observer)
	if err != nil {
		t.Fatal(err)
	}
	dataTopic, referenceTopic := options.Topics[0], options.Topics[1]
	produce(t, client, "old-attempt", Message{Topic: dataTopic, Value: []byte("old-incomplete")})
	first := produce(t, client, "current-first", Message{Topic: dataTopic, Key: []byte("same-key"), Value: []byte("first")}).WritesCopy()[0]
	independent := nativeClient(t, options.Brokers)
	nativeProduce(t, independent, &kgo.Record{Topic: dataTopic, Value: []byte("foreign")})
	last := produce(t, client, "current-last", Message{Topic: dataTopic, Key: []byte("same-key"), Value: []byte("last")},
		Message{Topic: dataTopic, Partition: 1, Value: []byte("other-partition")}).WritesCopy()
	reference := fixtureReference{Version: 1, Attempt: "current", Positions: []fixtureAddress{
		fixtureWire(first.Position), fixtureWire(last[0].Position), fixtureWire(last[1].Position)}}
	encoded, err := json.Marshal(reference)
	if err != nil {
		t.Fatal(err)
	}
	published := produce(t, client, "publish-reference", Message{Topic: referenceTopic, Value: encoded}).WritesCopy()[0]
	produce(t, client, "late-old-attempt", Message{Topic: dataTopic, Value: []byte("late-old-incomplete")})

	// The downstream receives only the REFERENCE record's position, never data
	// bytes, an inferred continuous interval or a broker consumer-group offset.
	referenceRead := readResult(t, client, "downstream-reference", published.Position)
	if referenceRead.State != ReadFound {
		t.Fatal("reference not readable", referenceRead.Err)
	}
	var decoded fixtureReference
	if err := json.Unmarshal(referenceRead.Record.ValueCopy(), &decoded); err != nil || decoded.Version != 1 || decoded.Attempt != "current" || len(decoded.Positions) != 3 {
		t.Fatal("reference fixture validation failed")
	}
	for index, want := range []string{"first", "last", "other-partition"} {
		read := readResult(t, client, "downstream-"+string(rune('a'+index)), decoded.Positions[index].position())
		if read.State != ReadFound || string(read.Record.ValueCopy()) != want {
			t.Fatal("downstream output attribution/contents changed", read.Err)
		}
	}
	stored := observe(t, options.Brokers, dataTopic, 0, 0, 5)
	if string(stored[0].Value) != "old-incomplete" || string(stored[2].Value) != "foreign" || string(stored[4].Value) != "late-old-incomplete" {
		t.Fatal("independent interleaving control failed")
	}
	observedReference := observe(t, options.Brokers, referenceTopic, 0, published.Position.Offset, 1)[0]
	if string(observedReference.Value) != string(encoded) {
		t.Fatal("reference ACK differed from independently observed content")
	}

	empty := produce(t, client, "empty-data", Message{Topic: dataTopic, Value: []byte{}}).WritesCopy()[0]
	encoded, err = json.Marshal(fixtureReference{Version: 1, Attempt: "successful-empty", Empty: true, Positions: []fixtureAddress{fixtureWire(empty.Position)}})
	if err != nil {
		t.Fatal(err)
	}
	emptyReference := produce(t, client, "empty-reference", Message{Topic: referenceTopic, Value: encoded}).WritesCopy()[0]
	emptyRead := readResult(t, client, "empty-record", empty.Position)
	if emptyRead.State != ReadFound || emptyRead.Record.ValueCopy() == nil || len(emptyRead.Record.ValueCopy()) != 0 {
		t.Fatal("explicit empty data confused with tombstone/absence")
	}
	if readResult(t, client, "empty-reference-read", emptyReference.Position).State != ReadFound {
		t.Fatal("successful-empty reference missing")
	}

	orphan := produce(t, client, "orphan-data", Message{Topic: dataTopic, Value: []byte("orphan")}).WritesCopy()[0]
	receipt, err := client.Produce(deadline(t), correlation("failed-publication"), []Message{{Topic: referenceTopic, Partition: 99, Value: []byte("must-not-publish")}})
	failed := settle(t, receipt, err)
	if failed.Err() == nil || failed.Outcome.Value.WritesCopy()[0].State == WriteAcknowledged {
		t.Fatal("failed publication produced a usable reference")
	}
	if readResult(t, client, "orphan-observation", orphan.Position).State != ReadFound {
		t.Fatal("reference failure erased actual data effect")
	}
	if failed.Observation != invocation.ObservationDropped {
		t.Fatal("diagnostic saturation control not reached")
	}
	if err := observer.ExportOne(deadline(t), func(context.Context, invocation.Event) error { return errors.New("synthetic-diagnostic-unavailable") }); !errors.Is(err, invocation.ErrObservation) {
		t.Fatal("diagnostic failure control failed")
	}

	// Direct callers already handled the publication error. Independently receive
	// the original attributed facts; diagnostics are neither their owner nor proof.
	foundFailure, foundOrphan := false, false
	for fixture.inbox.Usage().Outstanding > 0 {
		delivery, err := fixture.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		evidence, err := delivery.Receipt().WaitReleased(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if evidence.Context.Correlation.Call == "failed-publication" {
			foundFailure = errors.Is(evidence.Err(), ErrProduce)
		}
		if evidence.Context.Correlation.Call == "orphan-data" {
			foundOrphan = evidence.Outcome.Value.WritesCopy()[0].State == WriteAcknowledged
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if !foundFailure || !foundOrphan {
		t.Fatal("independent partial publication facts lost")
	}

	access, err := resource.AccessFor(fixture.assembly, fixture.selected)
	if err != nil {
		t.Fatal(err)
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/twmb/franz-go", "github.com/twmb/franz-go/pkg/kmsg"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, access, client.Profile(),
		[]compatibility.Requirement{{Guarantee: "exact-reference-reading", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK, compatibility.Service}}}, nil)
	if err != nil || report.Require(compatibility.Policy{}) == nil {
		t.Fatal("build metadata manufactured tested compatibility")
	}
	conformance.Facade(t, client, "Metadata", "Produce", "ProduceTransaction", "ReadExact", "ReadPositions", "ReadRange", "Consume", "CommitOffsets", "FetchOffsets", "Profile", "EvidenceBytes",
		"String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
}
func TestDataReferenceIntegration(t *testing.T) {
	cluster := localCluster(t)
	runDataReferenceIntegration(t, clusterOptions(cluster))
}

type privateKafkaConfig struct {
	Kafka struct {
		BootstrapServers string `yaml:"bootstrap_servers"`
		SecurityProtocol string `yaml:"security_protocol"`
		Auth             string `yaml:"auth"`
	} `yaml:"kafka"`
}

func loadPrivateKafka(t *testing.T) []string {
	t.Helper()
	path := os.Getenv("FATHOMRY_KAFKA_TEST_CONFIG")
	if path == "" {
		t.Skip("explicit private Kafka fixture not supplied")
	}
	if os.Getenv("FATHOMRY_KAFKA_TEST_WRITES") != "1" {
		t.Fatal("explicit isolated-topic write authorization required")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("private Kafka configuration unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
		t.Fatal("private fixture permissions or size invalid")
	}
	var config privateKafkaConfig
	decoder := yaml.NewDecoder(io.LimitReader(file, 64<<10+1))
	if decoder.Decode(&config) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
		t.Fatal("private Kafka fixture invalid")
	}
	if config.Kafka.SecurityProtocol != "PLAINTEXT" || config.Kafka.Auth != "none" {
		t.Fatal("service fixture requires explicitly supported plaintext/no-auth profile")
	}
	addresses := strings.Split(config.Kafka.BootstrapServers, ",")
	if len(addresses) == 0 || len(addresses) > 16 {
		t.Fatal("invalid fixture endpoint count")
	}
	for index := range addresses {
		addresses[index] = strings.TrimSpace(addresses[index])
		if _, err := endpoint(addresses[index]); err != nil {
			t.Fatal("unsupported fixture endpoint form")
		}
	}
	return addresses
}

// newServiceTopics uses only new random test-owned topics. The cleanup is
// registered BEFORE creation, resolves lost responses, verifies incarnation IDs,
// deletes only those exact IDs and then observes absence.
func waitForServiceBroker(t *testing.T, admin *kgo.Client) *kmsg.MetadataResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		request := kmsg.NewPtrMetadataRequest()
		request.Topics = []kmsg.MetadataRequestTopic{}
		metadata, err := request.RequestWith(ctx, admin)
		if err == nil && metadata.ClusterID != nil && *metadata.ClusterID != "" && len(metadata.Brokers) > 0 && len(metadata.Brokers) <= 16 {
			return metadata
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal("test broker did not become ready through the Kafka metadata API")
		case <-timer.C:
		}
	}
}

func newServiceTopics(t *testing.T, addresses []string) (OptionsV1, *kgo.Client) {
	t.Helper()
	admin := nativeClient(t, addresses, kgo.RequestRetries(1), kgo.RetryTimeout(5*time.Second))
	metadata := waitForServiceBroker(t, admin)
	options := OptionsV1{Name: "integration", ClusterID: *metadata.ClusterID, Plaintext: true, Timeout: 10 * time.Second, MaxActive: 4}
	for _, broker := range metadata.Brokers {
		options.Brokers = append(options.Brokers, brokerAddress(broker.Host, broker.Port))
	}
	suffix := make([]byte, 12)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal("test identity generation failed")
	}
	prefix := "fathomry-gh36-" + hex.EncodeToString(suffix)
	options.Topics = []string{prefix + "-data", prefix + "-references"}
	inspect := func() (*kmsg.MetadataResponse, error) {
		query := kmsg.NewPtrMetadataRequest()
		query.AllowAutoTopicCreation = false
		for _, topic := range options.Topics {
			query.Topics = append(query.Topics, kmsg.MetadataRequestTopic{Topic: kmsg.StringPtr(topic)})
		}
		return query.RequestWith(deadline(t), admin)
	}
	before, err := inspect()
	if err != nil || len(before.Topics) != 2 {
		t.Fatal("test topic absence preflight failed")
	}
	for _, topic := range before.Topics {
		if topic.ErrorCode != kerr.UnknownTopicOrPartition.Code {
			t.Fatal("test topic name was not confirmed absent")
		}
	}
	owned := make(map[string][16]byte)
	t.Cleanup(func() {
		current, err := inspect()
		if err != nil {
			t.Error("test topic cleanup metadata failed")
			return
		}
		remove := kmsg.NewPtrDeleteTopicsRequest()
		remove.TimeoutMillis = 5000
		for _, topic := range current.Topics {
			if topic.ErrorCode == kerr.UnknownTopicOrPartition.Code {
				continue
			}
			if topic.ErrorCode != 0 || topic.Topic == nil || topic.TopicID == ([16]byte{}) {
				t.Error("test topic cleanup identity unavailable")
				return
			}
			expected, known := owned[*topic.Topic]
			if known && expected != topic.TopicID {
				t.Error("test topic incarnation changed; refusing deletion")
				return
			}
			remove.Topics = append(remove.Topics, kmsg.DeleteTopicsRequestTopic{TopicID: topic.TopicID})
		}
		if len(remove.Topics) > 0 {
			response, err := remove.RequestWith(deadline(t), admin)
			if err != nil {
				t.Error("test topic deletion unconfirmed")
				return
			}
			for _, topic := range response.Topics {
				if topic.ErrorCode != 0 && topic.ErrorCode != kerr.UnknownTopicID.Code {
					t.Error("test topic deletion failed")
					return
				}
			}
		}
		until := time.Now().Add(10 * time.Second)
		for {
			remaining, err := inspect()
			absent := err == nil
			if absent {
				for _, topic := range remaining.Topics {
					if topic.ErrorCode != kerr.UnknownTopicOrPartition.Code {
						absent = false
					}
				}
			}
			if absent {
				t.Log("test-owned topics deleted; absence independently observed")
				return
			}
			if time.Now().After(until) {
				t.Error("test topics remain after cleanup")
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
	create := kmsg.NewPtrCreateTopicsRequest()
	create.TimeoutMillis = 5000
	for _, name := range options.Topics {
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic = name
		topic.NumPartitions = 2
		topic.ReplicationFactor = 1
		topic.Configs = []kmsg.CreateTopicsRequestTopicConfig{
			{Name: "cleanup.policy", Value: kmsg.StringPtr("delete")}, {Name: "retention.ms", Value: kmsg.StringPtr("604800000")},
			{Name: "message.timestamp.type", Value: kmsg.StringPtr("CreateTime")}}
		create.Topics = append(create.Topics, topic)
	}
	created, err := create.RequestWith(deadline(t), admin)
	if err != nil {
		t.Fatal("test topic creation unconfirmed; cleanup will inspect")
	}
	for _, topic := range created.Topics {
		if topic.ErrorCode != 0 {
			t.Fatal("test topic creation failed")
		}
	}
	for until := time.Now().Add(10 * time.Second); ; {
		current, err := inspect()
		ready := err == nil && len(current.Topics) == 2
		if ready {
			for _, topic := range current.Topics {
				if topic.ErrorCode != 0 || topic.Topic == nil || topic.TopicID == ([16]byte{}) {
					ready = false
					break
				}
				for _, partition := range topic.Partitions {
					if partition.ErrorCode != 0 || partition.Leader < 0 {
						ready = false
					}
				}
				owned[*topic.Topic] = topic.TopicID
			}
		}
		if ready {
			break
		}
		if time.Now().After(until) {
			t.Fatal("created test topics did not become ready")
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("authorized existing Kafka: %d advertised broker(s); test-owned RF1 topics; plaintext/no-auth; server release not inferred from protocol", len(options.Brokers))
	return options, admin
}
func TestKafkaServiceIntegration(t *testing.T) {
	addresses := loadPrivateKafka(t)
	t.Run("data-reference-and-independent-evidence", func(t *testing.T) {
		options, _ := newServiceTopics(t, addresses)
		runDataReferenceIntegration(t, options)
	})
	t.Run("transactions-expiry-and-bounds", func(t *testing.T) {
		options, admin := newServiceTopics(t, addresses)
		runServiceEffects(t, options, admin)
	})
	t.Run("consumer-timeout-and-resource-reuse", func(t *testing.T) {
		options, _ := newServiceTopics(t, addresses)
		fixture := bindFixture(t, options, 8)
		receipt, err := fixture.client.Metadata(deadline(t), correlation("metadata"))
		metadata := settle(t, receipt, err)
		if metadata.Err() != nil {
			t.Fatal(metadata.Err())
		}
		topic := metadata.Outcome.Value.TopicsCopy()[0]
		position := Position{ClusterID: options.ClusterID, Topic: topic.Name, TopicID: topic.ID}
		lifetime, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		consumer, err := fixture.client.Consume(lifetime, correlation("timeout-consumer"), Range{Start: position, End: math.MaxInt64})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { settle(t, consumer.Close(), nil); <-consumer.done })
		receipt, err = consumer.Next(deadline(t), correlation("empty-read"))
		if err == nil {
			read := settle(t, receipt, nil)
			if read.Err() == nil || len(read.Outcome.Value.Page().RecordsCopy()) != 0 {
				t.Fatal("empty growing output became successful data")
			}
		} else if !errors.Is(err, ErrState) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		closed := settle(t, consumer.Receipt(), nil)
		if !errors.Is(closed.Err(), context.DeadlineExceeded) || closed.Outcome.Value.ConsumerProgress().Complete {
			t.Fatal("consumer deadline lost or output falsely completed")
		}
		after := produce(t, fixture.client, "after-timeout", Message{Topic: topic.Name, Value: []byte("still-usable")}).WritesCopy()[0]
		if after.Position.Offset != 0 || readResult(t, fixture.client, "after-timeout-read", after.Position).State != ReadFound {
			t.Fatal("consumer timeout destroyed the shared source or created unexpected data")
		}
	})
	t.Run("compressed-fetch-bound", func(t *testing.T) {
		options, _ := newServiceTopics(t, addresses)
		options.MaxActive, options.MaxRecordBytes, options.MaxBatchBytes = 1, 1024, 2048
		options.MaxWireBytes, options.MaxDecodedBatchBytes = 4096, 2048
		fixture := bindFixture(t, options, 4)
		native := nativeClient(t, options.Brokers, kgo.ProducerBatchCompression(kgo.GzipCompression()))
		record := &kgo.Record{Topic: options.Topics[0], Value: bytes.Repeat([]byte("x"), 64<<10)}
		nativeProduce(t, native, record)
		receipt, err := fixture.client.Metadata(deadline(t), correlation("metadata"))
		metadata := settle(t, receipt, err)
		if metadata.Err() != nil {
			t.Fatal(metadata.Err())
		}
		topic := metadata.Outcome.Value.TopicsCopy()[0]
		position := Position{ClusterID: options.ClusterID, Topic: topic.Name, TopicID: topic.ID, Offset: record.Offset}
		read := readResult(t, fixture.client, "bounded-read", position)
		if !errors.Is(read.Err, ErrLimit) || read.State == ReadFound {
			t.Fatal("compressed expansion escaped the decoded bound")
		}
		observed := observe(t, options.Brokers, topic.Name, 0, record.Offset, 1)
		if len(observed[0].Value) != 64<<10 {
			t.Fatal("independent decoded-size control failed")
		}
	})
}
func runServiceEffects(t *testing.T, options OptionsV1, admin *kgo.Client) {
	options.TransactionalID = options.Topics[0] + "-txn"
	options.OffsetGroup = options.Topics[0] + "-offsets"
	fixture := bindFixture(t, options, 32)
	t.Cleanup(func() {
		request := kmsg.NewPtrDeleteGroupsRequest()
		request.Groups = []string{options.OffsetGroup}
		response, err := request.RequestWith(deadline(t), admin)
		if err != nil || len(response.Groups) != 1 || response.Groups[0].ErrorCode != 0 && response.Groups[0].ErrorCode != kerr.GroupIDNotFound.Code {
			t.Error("test-owned checkpoint group cleanup failed")
			return
		}
		query := kmsg.NewPtrDescribeGroupsRequest()
		query.Groups = []string{options.OffsetGroup}
		observed, err := query.RequestWith(deadline(t), admin)
		if err != nil || len(observed.Groups) != 1 || observed.Groups[0].ErrorCode != kerr.GroupIDNotFound.Code && observed.Groups[0].State != "Dead" {
			t.Error("checkpoint group absence not confirmed")
			return
		}
		t.Log("test-owned checkpoint group deleted; absence observed")
	})
	client := fixture.client
	receipt, err := client.ProduceTransaction(deadline(t), correlation("atomic"), []Message{{Topic: options.Topics[0], Value: []byte("p0")}, {Topic: options.Topics[0], Partition: 1, Value: []byte("p1")}})
	result := settle(t, receipt, err)
	if result.Err() != nil || result.Outcome.Value.Transaction() != TransactionCommitted {
		t.Fatal("real Kafka transaction commit failed", result.Err())
	}
	for _, write := range result.Outcome.Value.WritesCopy() {
		if readResult(t, client, "committed-"+string(rune('a'+write.Position.Partition)), write.Position).State != ReadFound {
			t.Fatal("committed data not visible")
		}
	}
	receipt, err = client.ProduceTransaction(deadline(t), correlation("abort"), []Message{{Topic: options.Topics[0], Value: []byte("abort")}, {Topic: options.Topics[0], Partition: 99}})
	result = settle(t, receipt, err)
	if result.Err() == nil || result.Outcome.Value.Transaction() != TransactionAborted {
		t.Fatal("real Kafka partial batch did not abort")
	}
	aborted := result.Outcome.Value.WritesCopy()[0].Position
	if readResult(t, client, "aborted", aborted).State != ReadMissing {
		t.Fatal("aborted data became valid output")
	}

	live := produce(t, client, "retained", Message{Topic: options.Topics[1], Value: []byte("prefix")}, Message{Topic: options.Topics[1], Value: []byte("live")}).WritesCopy()
	request := kmsg.NewPtrDeleteRecordsRequest()
	request.TimeoutMillis = 5000
	request.Topics = []kmsg.DeleteRecordsRequestTopic{{Topic: options.Topics[1], Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: 0, Offset: live[1].Position.Offset}}}}
	deleted, err := request.RequestWith(deadline(t), admin)
	if err != nil || deleted.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatal("test-owned prefix deletion failed")
	}
	if readResult(t, client, "expired", live[0].Position).State != ReadExpired {
		t.Fatal("real expired prefix was not rejected")
	}
	if readResult(t, client, "still-live", live[1].Position).State != ReadFound {
		t.Fatal("live suffix was lost")
	}
	checkpoint := checkpointFor(live[1].Position, live[1].Position.Offset+1)
	receipt, err = client.CommitOffsets(deadline(t), correlation("checkpoint"), []Checkpoint{checkpoint})
	committed := settle(t, receipt, err)
	if committed.Err() != nil || committed.Outcome.Value.CheckpointsCopy()[0].State != CheckpointCommitted {
		logFixtureCauses(t, committed.Err())
		t.Fatal("real Kafka checkpoint commit failed")
	}
	receipt, err = client.FetchOffsets(deadline(t), correlation("checkpoint-readback"), []Checkpoint{checkpoint})
	readback := settle(t, receipt, err)
	if readback.Err() != nil || readback.Outcome.Value.CheckpointsCopy()[0].Checkpoint.Next != checkpoint.Next {
		t.Fatal("real Kafka checkpoint readback failed", readback.Err())
	}
	consumer, err := client.Consume(deadline(t), correlation("service-consumer"), Range{Start: live[1].Position, End: checkpoint.Next})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { settle(t, consumer.Close(), nil); <-consumer.done })
	receipt, err = consumer.Next(deadline(t), correlation("service-consumer-page"))
	page := settle(t, receipt, err)
	if page.Err() != nil || !page.Nested || len(page.Outcome.Value.Page().RecordsCopy()) != 1 {
		t.Fatal("real direct consumer page failed", page.Err())
	}
	receipt, err = consumer.Commit(deadline(t), correlation("service-consumer-commit"))
	if result := settle(t, receipt, err); result.Err() != nil {
		t.Fatal("real consumer checkpoint failed", result.Err())
	}
	closed := settle(t, consumer.Close(), nil)
	if !closed.Outcome.Value.ConsumerProgress().Complete {
		t.Fatal("consumer close lost completion evidence")
	}
}
