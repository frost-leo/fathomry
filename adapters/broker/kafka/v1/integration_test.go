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
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestIndependentPublicConsumer(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile("testdata/consumer/main.go")
	if err != nil {
		t.Fatal(err)
	}
	syntax, err := parser.ParseFile(token.NewFileSet(), "main.go", fixture, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, imported := range syntax.Imports {
		name, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(name, "/internal/") || strings.Contains(name, "franz-go") || strings.Contains(name, "/framework/") {
			t.Fatal("independent consumer bypassed public boundary")
		}
	}
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string][]byte{
		"main.go": fixture, "go.sum": sums, "go.mod": []byte(fmt.Sprintf("module example.org/kafka-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))),
	} {
		if err := os.WriteFile(filepath.Join(directory, name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(offline bool, args ...string) []byte {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "TMPDIR="+filepath.Join(directory, "tmp"))
		if offline {
			command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
		}
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("independent consumer %v: %v\n%s", args, err, output)
		}
		return output
	}
	run(false, "mod", "tidy")
	run(true, "mod", "tidy", "-diff")
	binary := filepath.Join(directory, "consumer")
	run(true, "build", "-mod=readonly", "-race", "-o", binary, ".")
	dependencies := run(true, "list", "-mod=readonly", "-deps", ".")
	for _, name := range strings.Fields(string(dependencies)) {
		if strings.Contains(name, "/fathomry/framework/") || strings.Contains(name, "/fathomry/internal/objectstore/") || strings.Contains(name, "/fathomry/adapters/database/") {
			t.Fatal("provider acquired unrelated dependency", name)
		}
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	pinned := false
	for _, module := range info.Deps {
		if module.Path == "github.com/twmb/franz-go" {
			pinned = module.Version == "v1.21.6" && module.Replace == nil && module.Sum != ""
		}
	}
	if !pinned {
		t.Fatal("consumer native SDK pin changed")
	}
	peer := publicPeer(t)
	input, err := json.Marshal(peerSettings(peer))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(testContext(t), binary)
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil || string(output) != "kafka public consumer passed\n" {
		t.Fatalf("independent native path: %v\n%s", err, output)
	}
	if string(observeNative(t, peer, Position{Topic: "records", Partition: 0, Offset: 0}).Value) != "independent-public" {
		t.Fatal("independent executable produced no actual record")
	}
}

func TestFrameworkFixedFollowRetainsNativeGeneration(t *testing.T) {
	firstPeer, secondPeer := publicPeer(t), publicPeer(t)
	first, second := peerSettings(firstPeer), peerSettings(secondPeer)
	first.Name, second.Name = "first", "second"
	policy, err := Compose(first, first, second, second)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := framework.New(context.Background(), framework.Options{Operations: policy.Runtime})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	deps := Dependencies{Runtime: runtime.Operations(), Evidence: inbox, Transactions: &TransactionIDs{}}
	var mutex sync.Mutex
	owners := map[string][]*Owner{}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		mutex.Lock()
		defer mutex.Unlock()
		for _, list := range owners {
			for _, owner := range list {
				if !owner.ShutdownComplete() {
					t.Error("Framework abandoned a native owner")
				}
			}
		}
		drain(t, ctx, inbox)
	})
	clone := func(value Settings) Settings {
		value.Brokers = append([]string{}, value.Brokers...)
		value.Topics = append([]string{}, value.Topics...)
		return value
	}
	refusal := errors.New("synthetic candidate refusal")
	bind := func(name string, mode resource.Policy) resource.Ref[Handle] {
		ref, err := resource.Bind(runtime.Resources(), resource.Binding[Settings, Handle]{
			Name: name, Policy: mode, Clone: clone, Equal: func(left, right Settings) bool { return reflect.DeepEqual(left, right) },
			Select: func(view settings.View) (Settings, error) {
				snapshot, err := settings.As[Settings](view)
				if err != nil {
					return Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Build: func(ctx context.Context, value Settings) (*resource.Instance[Handle], error) {
				owner, err := Open(ctx, value, deps)
				if owner == nil {
					return nil, err
				}
				mutex.Lock()
				owners[name] = append(owners[name], owner)
				mutex.Unlock()
				instance := &resource.Instance[Handle]{Value: owner.Handle(), Release: owner.Release}
				if value.Name == "refused" {
					return instance, errors.Join(err, refusal)
				}
				return instance, err
			}})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	fixedRef, followRef := bind("fixed", resource.Fixed), bind("follow", resource.Follow)
	apply := func(value Settings) error {
		snapshot, err := settings.New(value, clone)
		if err != nil {
			return err
		}
		update, err := runtime.Resources().Apply(testContext(t), snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(testContext(t))
	}
	if err := apply(first); err != nil {
		t.Fatal(err)
	}
	fixed, err := Using(testContext(t), fixedRef, policy.Budget, deps)
	if err != nil {
		t.Fatal(err)
	}
	follow, err := Using(testContext(t), followRef, policy.Budget, deps)
	if err != nil {
		t.Fatal(err)
	}
	oldStatus, _ := followRef.Inspect()
	fixedStatus, _ := fixedRef.Inspect()
	write := send(t, follow, Message{Topic: "records", Value: []byte("old")}).WritesCopy()[0]
	ack(t, inbox)
	cursor, err := follow.Consume(testContext(t), Range{Start: write.Position, End: 1})
	if err != nil {
		t.Fatal(err)
	}
	group, err := follow.ConsumeGroup(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	waitAssignment(t, group, inbox, 2)
	if err := apply(second); err != nil {
		t.Fatal(err)
	}
	newStatus, _ := followRef.Inspect()
	if newStatus.Generation == oldStatus.Generation || newStatus.Retiring != 1 {
		t.Fatal("Follow did not retain borrowed old generation")
	}
	current := send(t, follow, Message{Topic: "records", Value: []byte("new")})
	ack(t, inbox)
	if current.Source().Name != "second" || current.Attribution().Source.Generation != newStatus.Generation {
		t.Fatal("Follow attribution drift")
	}
	if string(observeNative(t, secondPeer, current.WritesCopy()[0].Position).Value) != "new" {
		t.Fatal("Follow used old native cluster")
	}
	old, err := cursor.Next(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	if old.Source().Name != "first" || old.Attribution().Source.Generation != oldStatus.Generation {
		t.Fatal("direct cursor retargeted")
	}
	groupRead, err := group.Next(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	if groupRead.Source().Name != "first" || groupRead.Attribution().Source.Generation != oldStatus.Generation {
		t.Fatal("group retargeted")
	}
	fixedWrite := send(t, fixed, Message{Topic: "records", Value: []byte("fixed")})
	ack(t, inbox)
	if fixedWrite.Attribution().Source.Generation != fixedStatus.Generation || fixedWrite.Source().Name != "first" {
		t.Fatal("Fixed followed settings")
	}
	if _, err := cursor.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	if _, err := group.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	rejected := second
	rejected.Name = "refused"
	if err := apply(rejected); !errors.Is(err, refusal) {
		t.Fatal("partial candidate failure lost", err)
	}
	status, _ := followRef.Inspect()
	if status.Generation != newStatus.Generation {
		t.Fatal("failed replacement discarded last good source")
	}
	larger := second
	larger.Name, larger.MaxWireBytes = "larger", 16<<20
	if err := apply(larger); err != nil {
		t.Fatal(err)
	}
	var dispatched atomic.Int32
	secondPeer.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		secondPeer.KeepControl()
		dispatched.Add(1)
		return nil, nil, false
	})
	receipt, err := follow.Produce(testContext(t), []Message{{Topic: "records", Value: []byte("undercharged")}})
	if err != nil {
		t.Fatal(err)
	}
	rejectedResult, err := receipt.WaitReleased(testContext(t))
	if err != nil || !errors.Is(rejectedResult.Primary(), ErrLimit) || dispatched.Load() != 0 {
		t.Fatal("larger generation dispatched under an insufficient Using budget", err)
	}
}

func TestDirectNativeCapabilityAndOwnership(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	deps, inbox := testDependencies(t, options)
	owner := testOpen(t, options, deps)
	client, err := owner.Client().WithID("opaque/correlation")
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := client.Metadata(testContext(t))
	if err != nil || len(metadata.TopicsCopy()) != 1 {
		t.Fatal(err)
	}
	ack(t, inbox)
	if metadata.Source().Revision == "" || metadata.Attribution().ID != "opaque/correlation" {
		t.Fatal("attribution lost")
	}
	payload := []byte("native-value")
	receipt, err := client.Produce(testContext(t), []Message{{Topic: "records", Value: payload, Headers: []Header{{Key: "duplicate"}, {Key: "duplicate", Value: []byte{}}}},
		{Topic: "records", Value: nil}, {Topic: "records", Value: []byte{}}})
	payload[0] = 'X'
	written := await(t, receipt, err)
	ack(t, inbox)
	writes := written.WritesCopy()
	for _, write := range writes {
		if !write.IdentityChecked || !write.PositionKnown {
			t.Fatal("ACK identity missing")
		}
	}
	if !bytes.Equal(observeNative(t, peer, writes[0].Position).Value, []byte("native-value")) {
		t.Fatal("source mutation affected native input")
	}
	read, err := client.ReadPositions(testContext(t), []Position{writes[0].Position, writes[1].Position, writes[2].Position})
	if err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	reads := read.ReadsCopy()
	if reads[1].Record.ValueCopy() != nil || reads[2].Record.ValueCopy() == nil {
		t.Fatal("nil and empty collapsed")
	}
	headers := reads[0].Record.HeadersCopy()
	if len(headers) != 2 || headers[0].Value != nil || headers[1].Value == nil {
		t.Fatal("headers collapsed")
	}
	cursor, err := client.Consume(testContext(t), Range{Start: writes[0].Position, End: 3})
	if err != nil {
		t.Fatal(err)
	}
	page, err := cursor.Next(testContext(t))
	if err != nil || len(page.Page().RecordsCopy()) != 3 {
		t.Fatal(err)
	}
	ack(t, inbox)
	committed, err := cursor.Commit(testContext(t))
	if err != nil || committed.CheckpointsCopy()[0].Checkpoint.Next != 3 {
		t.Fatal(err)
	}
	ack(t, inbox)
	if _, err := cursor.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	checkpoint := committed.CheckpointsCopy()[0].Checkpoint
	fetched, err := client.FetchOffsets(testContext(t), []Checkpoint{checkpoint})
	if err != nil || fetched.CheckpointsCopy()[0].Checkpoint.Next != 3 {
		t.Fatal(err)
	}
	ack(t, inbox)
	if _, err := client.CommitOffsets(testContext(t), []Checkpoint{checkpoint}); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	rangeResult, err := client.ReadRange(testContext(t), Range{Start: writes[0].Position, End: 3})
	if err != nil || !rangeResult.Page().Complete() {
		t.Fatal(err)
	}
	ack(t, inbox)
}

func TestLatePromiseAndEvidenceRetryNeverResends(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	deps, inbox := testDependencies(t, options)
	owner := testOpen(t, options, deps)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var requests atomic.Int32
	peer.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		peer.KeepControl()
		if requests.Add(1) == 1 {
			close(entered)
			peer.SleepControl(func() { <-release })
		}
		return nil, nil, false
	})
	receipt, err := owner.Client().Produce(testContext(t), []Message{{Topic: "records", Value: []byte("late")}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("missing native production")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := receipt.WaitReleased(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("wait cancellation lost")
	}
	if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("early native release")
	}
	unblock()
	value := await(t, receipt, nil)
	refusal := errors.New("synthetic receiver failure")
	failed := make(chan struct{}, 1)
	receiver, err := framework.StartReceiver(testContext(t), inbox, framework.ReceiverOptions{RetryDelay: time.Minute}, func(context.Context, adapters.Snapshot[Result]) error { failed <- struct{}{}; return refusal })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-failed:
	case <-testContext(t).Done():
		t.Fatal("receiver did not receive finite evidence")
	}
	if err := receiver.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if status, err := receiver.Status(); err != nil || !errors.Is(status.LastError, refusal) {
		t.Fatal("receiver failure lost", err)
	}
	delivery, err := inbox.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	copied, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	repeated := await(t, copied, nil)
	if repeated.WritesCopy()[0].Position.Offset != value.WritesCopy()[0].Position.Offset || requests.Load() != 1 {
		t.Fatal("evidence Retry repeated mutation")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	observeNative(t, peer, value.WritesCopy()[0].Position)
}
