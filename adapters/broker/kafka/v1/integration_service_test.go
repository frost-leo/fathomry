//go:build kafka_service

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

package kafka

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.yaml.in/yaml/v3"
)

func serviceFixture(t *testing.T) (Settings, *kgo.Client) {
	t.Helper()
	path := os.Getenv("FATHOMRY_KAFKA_TEST_CONFIG")
	if path == "" {
		t.Skip("explicit private Kafka fixture not supplied")
	}
	if os.Getenv("FATHOMRY_KAFKA_TEST_WRITES") != "1" {
		t.Fatal("isolated gh105 topic/group writes and cleanup require authorization")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("private fixture unavailable")
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Mode().Perm()&0077 != 0 || stat.Size() > 64<<10 {
		t.Fatal("private fixture permissions/size invalid")
	}
	var config struct {
		Kafka struct {
			BootstrapServers string `yaml:"bootstrap_servers"`
			SecurityProtocol string `yaml:"security_protocol"`
			Auth             string `yaml:"auth"`
		} `yaml:"kafka"`
	}
	decoder := yaml.NewDecoder(io.LimitReader(file, 64<<10+1))
	if decoder.Decode(&config) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
		t.Fatal("private fixture invalid")
	}
	if config.Kafka.SecurityProtocol != "PLAINTEXT" || config.Kafka.Auth != "none" {
		t.Fatal("this opt-in fixture only qualifies plaintext/no-auth")
	}
	addresses := strings.Split(config.Kafka.BootstrapServers, ",")
	if len(addresses) < 1 || len(addresses) > 16 {
		t.Fatal("fixture broker count invalid")
	}
	for index := range addresses {
		addresses[index] = strings.TrimSpace(addresses[index])
	}
	admin, err := kgo.NewClient(kgo.SeedBrokers(addresses...), kgo.DisableClientMetrics(), kgo.RequestRetries(1), kgo.RetryTimeout(5*time.Second))
	if err != nil {
		t.Fatal("service client construction failed")
	}
	t.Cleanup(admin.Close)
	query := kmsg.NewPtrMetadataRequest()
	query.Topics = []kmsg.MetadataRequestTopic{}
	metadata, err := query.RequestWith(testContext(t), admin)
	if err != nil || metadata.ClusterID == nil || len(metadata.Brokers) == 0 {
		t.Fatal("service metadata unavailable")
	}
	suffix := make([]byte, 12)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal("fixture identity generation failed")
	}
	prefix := "gh105-" + hex.EncodeToString(suffix)
	t.Logf("owned synthetic fixture prefix: %s", prefix)
	value := Settings{Name: "live", ClusterID: *metadata.ClusterID, Topics: []string{prefix + "-records"}, Plaintext: true,
		ConsumerGroup: prefix + "-group", OffsetGroup: prefix + "-checkpoint", InitialOffset: "earliest", ResetOffset: "error", MaxGroupSessions: 2, MaxRecords: 1}
	for _, broker := range metadata.Brokers {
		value.Brokers = append(value.Brokers, net.JoinHostPort(broker.Host, fmt.Sprint(broker.Port)))
	}
	inspect := func() (*kmsg.MetadataResponse, error) {
		request := kmsg.NewPtrMetadataRequest()
		request.AllowAutoTopicCreation = false
		request.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(value.Topics[0])}}
		return request.RequestWith(testContext(t), admin)
	}
	before, err := inspect()
	if err != nil || len(before.Topics) != 1 || before.Topics[0].ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Fatal("unique topic absence not confirmed")
	}
	var owned [16]byte
	created := false
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		removeGroups := kmsg.NewPtrDeleteGroupsRequest()
		removeGroups.Groups = []string{value.ConsumerGroup, value.OffsetGroup}
		response, err := removeGroups.RequestWith(ctx, admin)
		clean := true
		if err != nil || len(response.Groups) != 2 {
			t.Error("test group cleanup unconfirmed")
			clean = false
		} else {
			for _, group := range response.Groups {
				if group.ErrorCode != 0 && group.ErrorCode != kerr.GroupIDNotFound.Code {
					t.Error("test group deletion failed")
					clean = false
				}
			}
		}
		if !created {
			return
		}
		current, err := inspect()
		if err != nil || len(current.Topics) != 1 {
			t.Error("test topic cleanup identity unavailable")
			return
		}
		topic := current.Topics[0]
		if topic.ErrorCode == kerr.UnknownTopicOrPartition.Code {
			return
		}
		if topic.TopicID == ([16]byte{}) || topic.ErrorCode != 0 || owned != ([16]byte{}) && topic.TopicID != owned {
			t.Error("test topic incarnation changed; deletion refused")
			return
		}
		remove := kmsg.NewPtrDeleteTopicsRequest()
		remove.TimeoutMillis = 5000
		remove.Topics = []kmsg.DeleteTopicsRequestTopic{{TopicID: topic.TopicID}}
		deleted, err := remove.RequestWith(ctx, admin)
		if err != nil || len(deleted.Topics) != 1 || deleted.Topics[0].ErrorCode != 0 {
			t.Error("test topic deletion unconfirmed")
			return
		}
		for {
			current, err := inspect()
			if err == nil && len(current.Topics) == 1 && current.Topics[0].ErrorCode == kerr.UnknownTopicOrPartition.Code {
				break
			}
			select {
			case <-ctx.Done():
				t.Error("test topic absence not observed")
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
		describe := kmsg.NewPtrDescribeGroupsRequest()
		describe.Groups = removeGroups.Groups
		groups, err := describe.RequestWith(ctx, admin)
		if err != nil || len(groups.Groups) != 2 {
			t.Error("test group absence observation unavailable")
			return
		}
		for _, group := range groups.Groups {
			if group.ErrorCode != kerr.GroupIDNotFound.Code && !(group.ErrorCode == 0 && group.State == "Dead") {
				t.Error("deleted test group still present")
				clean = false
			}
		}
		if clean {
			t.Log("unique gh105 topic/group cleanup independently observed")
		}
	})
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = 5000
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = value.Topics[0]
	topic.NumPartitions = 2
	topic.ReplicationFactor = 1
	topic.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "cleanup.policy", Value: kmsg.StringPtr("delete")}, {Name: "message.timestamp.type", Value: kmsg.StringPtr("CreateTime")}, {Name: "compression.type", Value: kmsg.StringPtr("producer")}}
	request.Topics = []kmsg.CreateTopicsRequestTopic{topic}
	created = true
	result, err := request.RequestWith(testContext(t), admin)
	if err != nil {
		t.Fatal("test topic creation unconfirmed; cleanup retained")
	}
	if len(result.Topics) != 1 || result.Topics[0].ErrorCode != 0 {
		created = false
		t.Fatal("test topic creation refused")
	}
	until := time.Now().Add(15 * time.Second)
	for {
		current, err := inspect()
		if err == nil && len(current.Topics) == 1 && current.Topics[0].ErrorCode == 0 && current.Topics[0].TopicID != ([16]byte{}) {
			ready := len(current.Topics[0].Partitions) == 2
			for _, partition := range current.Topics[0].Partitions {
				if partition.ErrorCode != 0 || partition.Leader < 0 {
					ready = false
				}
			}
			if ready {
				owned = current.Topics[0].TopicID
				break
			}
		}
		if time.Now().After(until) {
			t.Fatal("test topic readiness unconfirmed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return value, admin
}
func serviceOffset(t *testing.T, admin *kgo.Client, group string, position Position) int64 {
	t.Helper()
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Groups = []kmsg.OffsetFetchRequestGroup{{Group: group, Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: position.Topic, TopicID: position.TopicID, Partitions: []int32{position.Partition}}}}}
	response, err := request.RequestWith(testContext(t), admin)
	if err != nil || len(response.Groups) != 1 || response.Groups[0].ErrorCode != 0 || len(response.Groups[0].Topics) != 1 || len(response.Groups[0].Topics[0].Partitions) != 1 {
		t.Fatal("independent checkpoint observation failed")
	}
	partition := response.Groups[0].Topics[0].Partitions[0]
	if partition.ErrorCode != 0 {
		t.Fatal("independent checkpoint partition failed")
	}
	return partition.Offset
}
func TestKafkaAuthorizedServiceCore(t *testing.T) {
	options, admin := serviceFixture(t)
	deps, inbox := testDependencies(t, options)
	var first Position
	for codecID, codec := range []string{"none", "gzip", "snappy", "lz4", "zstd"} {
		selected := options
		selected.Compression = codec
		owner := testOpen(t, selected, deps)
		payload := bytes.Repeat([]byte(codec+"-gh105-synthetic-"), 64)
		value := send(t, owner.Client(), Message{Topic: selected.Topics[0], Value: payload})
		position := value.WritesCopy()[0].Position
		ack(t, inbox)
		if codec == "none" {
			first = position
		}
		reader, err := kgo.NewClient(kgo.SeedBrokers(selected.Brokers...), kgo.DisableClientMetrics(), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			position.Topic: {position.Partition: kgo.NewOffset().At(position.Offset)}}))
		if err != nil {
			t.Fatal("independent service reader construction failed")
		}
		fetched := reader.PollRecords(testContext(t), 1)
		reader.Close()
		if fetched.Err() != nil || len(fetched.Records()) != 1 || !bytes.Equal(fetched.Records()[0].Value, payload) {
			t.Fatal("real-service codec effect read-back failed", codec)
		}
		if int(fetched.Records()[0].Attrs.CompressionType()) != codecID {
			t.Fatal("service did not retain the selected codec", codec)
		}
		read, err := owner.Client().ReadExact(testContext(t), position)
		if err != nil || read.ReadsCopy()[0].State != ReadFound || !bytes.Equal(read.ReadsCopy()[0].Record.ValueCopy(), payload) {
			t.Fatal("real bounded codec read failed", codec)
		}
		ack(t, inbox)
		if err := owner.Close(testContext(t)); err != nil {
			t.Fatal("codec source close failed")
		}
		ack(t, inbox)
	}
	owner := testOpen(t, options, deps)
	group, err := owner.Client().ConsumeGroup(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	waitAssignment(t, group, inbox, 2)
	get := func(group *Group) GroupBatch {
		for range 4 {
			value, err := group.Next(testContext(t))
			ack(t, inbox)
			if errors.Is(err, ErrUnavailable) {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			return value.GroupBatch()
		}
		t.Fatal("service group had no page")
		return GroupBatch{}
	}
	initial := get(group)
	if initial.Start().Offset != first.Offset {
		t.Fatal("initial policy did not start at explicit earliest")
	}
	committed, err := group.CommitBatch(testContext(t), initial)
	if err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	confirmed := committed.CheckpointsCopy()[0].Checkpoint.Next
	if serviceOffset(t, admin, options.ConsumerGroup, first) != confirmed {
		t.Fatal("member commit not independently observed")
	}
	pending := get(group)
	second, err := owner.Client().ConsumeGroup(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	waitAssignment(t, second, inbox, 1)
	waitAssignment(t, group, inbox, 1)
	if _, err := group.CommitBatch(testContext(t), pending); !errors.Is(err, ErrState) {
		t.Fatal("real stale assignment commit accepted", err)
	}
	ack(t, inbox)
	if _, err := group.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	if _, err := second.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	if serviceOffset(t, admin, options.ConsumerGroup, first) != confirmed {
		t.Fatal("close automatically advanced a checkpoint")
	}
	rejoined, err := owner.Client().ConsumeGroup(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	waitAssignment(t, rejoined, inbox, 2)
	resumed := get(rejoined)
	if resumed.Start().Offset != confirmed {
		t.Fatal("rejoin skipped unprocessed work")
	}
	if _, err := rejoined.CommitBatch(testContext(t), resumed); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	if _, err := rejoined.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	t.Log("real Kafka: five codecs with independent effect read-back; two classic members, redistribution, stale refusal, explicit commit, no close commit and rejoin verified")
	t.Log("profile: plaintext/no-auth RF1 isolated fixture; real TLS/SASL and failover remain unqualified")
}
