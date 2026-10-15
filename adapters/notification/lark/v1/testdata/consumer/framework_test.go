/*
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
*/

package consumer

import (
	"context"
	"errors"
	lark "github.com/frost-leo/fathomry/adapters/notification/lark/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"reflect"
	"sync"
	"testing"
	"time"
)

func cloneSettings(value lark.Settings) lark.Settings {
	if value.WebSocket != nil {
		socket := *value.WebSocket
		socket.AllowedHosts = append([]string(nil), socket.AllowedHosts...)
		value.WebSocket = &socket
	}
	return value
}
func TestFrameworkFixedFollowKeepsSourceAndConnectionGenerations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	first, second := newWSPeer(t), newWSPeer(t)
	configA, configB := first.settings(), second.settings()
	configA.Name, configB.Name = "first", "second"
	configA.WebSocket.AckTimeout, configB.WebSocket.AckTimeout = 2*time.Second, 2*time.Second
	preparedA, err := lark.Prepare(configA, lark.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	preparedB, err := lark.Prepare(configB, lark.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := lark.Compose(preparedA, preparedA, preparedB, preparedB)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[lark.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := lark.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	var mutex sync.Mutex
	seen := map[uint64]int{}
	receiver, err := framework.StartReceiver(ctx, inbox, framework.ReceiverOptions{RetryDelay: time.Millisecond},
		func(_ context.Context, snapshot adapters.Snapshot[lark.Result]) error {
			mutex.Lock()
			defer mutex.Unlock()
			seen[snapshot.Info().Sequence]++
			if snapshot.Info().Operation == "notification.lark.send" && seen[snapshot.Info().Sequence] == 1 {
				return errors.New("synthetic recording failure")
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close(context.Background())
	refused := errors.New("partial construction")
	bind := func(name string, mode resource.Policy) resource.Ref[lark.Handle] {
		ref, err := resource.Bind(runtime.Resources(), resource.Binding[lark.Settings, lark.Handle]{Name: name, Policy: mode,
			Select: func(view settings.View) (lark.Settings, error) {
				snapshot, err := settings.As[lark.Settings](view)
				if err != nil {
					return lark.Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Clone: cloneSettings, Equal: func(a, b lark.Settings) bool { return reflect.DeepEqual(a, b) },
			Build: func(lifetime context.Context, value lark.Settings) (*resource.Instance[lark.Handle], error) {
				owner, err := lark.Open(lifetime, value, dependencies)
				if owner == nil {
					return nil, err
				}
				instance := &resource.Instance[lark.Handle]{Value: owner.Handle(), Release: owner.Release}
				if value.Name == "partial" {
					return instance, errors.Join(err, refused)
				}
				return instance, err
			}})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	fixed, follow := bind("fixed", resource.Fixed), bind("follow", resource.Follow)
	store := settings.NewStore[lark.Settings]()
	publish := func(value lark.Settings) {
		snapshot, err := settings.New(value, cloneSettings)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.Publish(snapshot); err != nil {
			t.Fatal(err)
		}
	}
	apply := func() error {
		view, err := store.Reader().Capture()
		if err != nil {
			return err
		}
		update, err := runtime.Resources().Apply(ctx, view)
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	publish(configA)
	if err := apply(); err != nil {
		t.Fatal(err)
	}
	stable, err := lark.Using(ctx, fixed, policy, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	moving, err := lark.Using(ctx, follow, policy, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	eventsA, eventsB := publicEvents(t, 8), publicEvents(t, 8)
	stopA, doneA := startListen(moving, eventsA)
	defer stopA()
	socketA := nextConnection(t, first)
	sendEvent(t, socketA, "old-source")
	old, delivery := nextIncoming(t, eventsA)
	_ = delivery.Ack()
	publish(configB)
	before, err := moving.Send(ctx, lark.Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t))
	if err != nil || before.Source().Name != "first" {
		t.Fatal("settings publication was mistaken for adoption", err)
	}
	if err := apply(); err != nil {
		t.Fatal(err)
	}
	stopB, doneB := startListen(moving, eventsB)
	defer stopB()
	socketB := nextConnection(t, second)
	sendEvent(t, socketB, "new-source")
	newer, delivery := nextIncoming(t, eventsB)
	_ = delivery.Ack()
	if old.Attribution().Source.Generation == newer.Attribution().Source.Generation || old.ConnectionGeneration() != 1 || newer.ConnectionGeneration() != 1 {
		t.Fatal("dual generation identity lost")
	}
	if _, err := old.Acknowledge(ctx, lark.JSON{}); err != nil {
		t.Fatal("old source lost pending ACK", err)
	}
	oldWire, code := peerACK(t, first)
	if code != 200 || oldWire.headers["message_id"] != "old-source" {
		t.Fatal("old ACK retargeted")
	}
	noPositiveACK(t, second)
	if _, err := newer.Acknowledge(ctx, lark.JSON{}); err != nil {
		t.Fatal(err)
	}
	newWire, code := peerACK(t, second)
	if code != 200 || newWire.headers["message_id"] != "new-source" {
		t.Fatal("new ACK identity")
	}
	current, err := moving.Send(ctx, lark.Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t))
	if err != nil || current.Source().Name != "second" {
		t.Fatal("Follow did not adopt", err)
	}
	fixedResult, err := stable.Send(ctx, lark.Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t))
	if err != nil || fixedResult.Source().Name != "first" {
		t.Fatal("Fixed migrated", err)
	}
	finishListen(t, stopA, doneA)
	finishListen(t, stopB, doneB)
	partial := cloneSettings(configB)
	partial.Name = "partial"
	publish(partial)
	if err := apply(); !errors.Is(err, refused) {
		t.Fatal("partial construction cause lost", err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Finish(ctx); err != nil {
		t.Fatal(err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	for _, result := range []lark.Result{before, current, fixedResult} {
		if seen[result.Attribution().Sequence] != 2 {
			t.Fatal("independent evidence not redelivered")
		}
	}
}
