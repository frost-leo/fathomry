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
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestConsumerCursorCommitAndOwnership(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.MaxRecords = 2
	options.MaxActive = 1
	options.OffsetGroup = "cursor"
	fixture := bindFixture(t, options, 12)
	first := produce(t, fixture.client, "first", Message{Topic: "records", Value: []byte("a")}, Message{Topic: "records", Value: []byte("b")}).WritesCopy()[0].Position
	produce(t, fixture.client, "last", Message{Topic: "records", Value: []byte("c")})
	consumer, err := fixture.client.Consume(deadline(t), correlation("cursor"), Range{Start: first, End: 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { settle(t, consumer.Close(), nil); <-consumer.done })
	for pageIndex := range 2 {
		receipt, err := consumer.Next(deadline(t), correlation("page-"+string(rune('a'+pageIndex))))
		result := settle(t, receipt, err)
		if result.Err() != nil || !result.Nested || result.Context.Correlation.Parent != "cursor" || len(result.Outcome.Value.Page().RecordsCopy()) == 0 {
			t.Fatal("consumer page failed", result.Err())
		}
	}
	// Same root reservation: MaxActive=1 must not self-deadlock on commit.
	receipt, err := consumer.Commit(deadline(t), correlation("checkpoint"))
	committed := settle(t, receipt, err)
	if committed.Err() != nil || committed.Outcome.Value.CheckpointsCopy()[0].Checkpoint.Next != 3 {
		t.Fatal("consumer commit failed", committed.Err())
	}
	root := settle(t, consumer.Close(), nil)
	if root.Outcome.Value.ConsumerProgress().Received != 3 || !root.Outcome.Value.ConsumerProgress().Complete {
		t.Fatal("consumer summary lost cursor")
	}
	if _, err := consumer.Next(deadline(t), correlation("after-close")); !errors.Is(err, ErrState) {
		t.Fatal("closed consumer accepted read")
	}
	if err := fixture.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}
func TestConsumerCloseDoesNotCommitAndWaitsForNativeRead(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.OffsetGroup = "uncommitted-cursor"
	fixture := bindFixture(t, options, 8)
	position := produce(t, fixture.client, "record", Message{Topic: "records", Value: []byte("data")}).WritesCopy()[0].Position
	consumer, err := fixture.client.Consume(deadline(t), correlation("cursor"), Range{Start: position, End: 1})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		close(entered)
		cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		receipt, err := consumer.Next(deadline(t), correlation("page"))
		result := settle(t, receipt, err)
		if result.Err() != nil {
			t.Error(result.Err())
		}
	}()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("read did not start")
	}
	closed := consumer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if _, err := closed.WaitReleased(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close released in-flight native use")
	}
	cancel()
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("assembly released active consumer")
	}
	unblock()
	<-done
	settle(t, closed, nil)
	<-consumer.done
	// Independent checkpoint lookup: Close never created/committed the group.
	native := nativeClient(t, cluster.ListenAddrs())
	request := kmsg.NewPtrOffsetFetchRequest()
	group := kmsg.NewOffsetFetchRequestGroup()
	group.Group = options.OffsetGroup
	group.Topics = []kmsg.OffsetFetchRequestGroupTopic{{TopicID: position.TopicID, Partitions: []int32{0}}}
	request.Groups = []kmsg.OffsetFetchRequestGroup{group}
	response, err := request.RequestWith(deadline(t), native)
	if err != nil {
		t.Fatal("checkpoint absence lookup failed")
	}
	if len(response.Groups) != 1 || response.Groups[0].ErrorCode == 0 && response.Groups[0].Topics[0].Partitions[0].Offset != -1 {
		t.Fatal("Close silently committed")
	}
}
func TestConsumerLifetimeCancellationKeepsEvidence(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 4)
	position := produce(t, fixture.client, "record", Message{Topic: "records"}).WritesCopy()[0].Position
	ctx, cancel := context.WithCancelCause(context.Background())
	consumer, err := fixture.client.Consume(ctx, correlation("lifetime"), Range{Start: position, End: math.MaxInt64})
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("synthetic-lifetime-cause")
	cancel(cause)
	result := settle(t, consumer.Receipt(), nil)
	if !errors.Is(result.Err(), cause) || result.Outcome.Value.ConsumerProgress().Complete {
		t.Fatal("lifetime cancellation fabricated completion")
	}
	<-consumer.done
	if _, err := consumer.Next(deadline(t), correlation("late")); !errors.Is(err, ErrState) {
		t.Fatal("canceled consumer admitted work")
	}
}
func TestConsumerEvidenceSaturationClosesWithoutExtraSlot(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 1)
	metadataReceipt, err := fixture.client.Metadata(deadline(t), correlation("metadata"))
	metadata := settle(t, metadataReceipt, err)
	topic := metadata.Outcome.Value.TopicsCopy()[0]
	delivery, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	consumer, err := fixture.client.Consume(deadline(t), correlation("root"), Range{Start: Position{ClusterID: "fathomry-kafka-test", Topic: topic.Name, TopicID: topic.ID}, End: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Next(deadline(t), correlation("full")); !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("saturated inbox admitted consumer page", err)
	}
	settle(t, consumer.Close(), nil)
	<-consumer.done
}
