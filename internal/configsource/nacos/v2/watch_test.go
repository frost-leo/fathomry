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

package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	"sync"
	"testing"
	"time"
)

func nextChange(t testing.TB, subscription *Subscription) Change {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	change, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal("invalidation not observed", err)
	}
	return change
}
func pushPayload(kind, id string, fields map[string]any) *wire.Payload {
	if fields == nil {
		fields = make(map[string]any)
	}
	fields["requestId"] = id
	raw, _ := json.Marshal(fields)
	return &wire.Payload{Metadata: &wire.Metadata{Type: kind}, Body: payloadBytes(raw)}
}
func waitUntil(t testing.TB, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("fixture condition did not become true")
}
func TestWatchPushAckResetAndReconciliation(t *testing.T) {
	fixture := newFixture(t, false)
	client := openClient(t, fixture.options())
	subscription, err := client.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if change := nextChange(t, subscription); !change.Resync() || change.Err() != nil {
		t.Fatal("initial readiness/reconciliation evidence missing", change.Err())
	}
	fixture.push(t, pushPayload("ClientDetectionRequest", "detect", nil))
	waitUntil(t, func() bool { return fixture.acknowledgements.Load() >= 1 })
	fixture.mu.Lock()
	fixture.values[key{"DEFAULT_GROUP", "settings.yaml"}] = "host: changed"
	fixture.mu.Unlock()
	fixture.push(t, pushPayload("ConfigChangeNotifyRequest", "change", map[string]any{"tenant": "", "group": "DEFAULT_GROUP", "dataId": "settings.yaml"}))
	change := nextChange(t, subscription)
	if change.Err() != nil || change.Key().DataID != "settings.yaml" {
		t.Fatal("native invalidation changed", change.Err())
	}
	waitUntil(t, func() bool { return fixture.acknowledgements.Load() >= 2 })
	fixture.push(t, pushPayload("ConnectResetRequest", "reset", map[string]any{"serverIp": "unauthorized-canary.invalid", "serverPort": "1"}))
	for {
		change = nextChange(t, subscription)
		if change.Err() != nil {
			break
		}
	}
	if !change.Resync() {
		t.Fatal("reset hid observation gap")
	}
	for {
		change = nextChange(t, subscription)
		if change.Err() == nil && change.Resync() {
			break
		}
	}
	if fixture.setups.Load() < 2 || fixture.listens.Load() < 2 {
		t.Fatal("TCP reconnect was mistaken for Nacos registration")
	}
	if err := subscription.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := subscription.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal("closed subscription produced data")
	}
}
func TestBoundedQueueAndConcurrentWaiters(t *testing.T) {
	subscription := &Subscription{cancel: func() {}, done: make(chan struct{}), changed: make(chan struct{}), capacity: 2}
	subscription.publish(Change{selected: key{"g", "one"}})
	subscription.publish(Change{selected: key{"g", "two"}})
	subscription.publish(Change{selected: key{"g", "three"}})
	if change := nextChange(t, subscription); !change.Resync() || change.Key().DataID != "two" {
		t.Fatal("overflow silently discarded history")
	}
	_ = nextChange(t, subscription)
	var workers sync.WaitGroup
	values := make(chan string, 2)
	for range 2 {
		workers.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			change, err := subscription.Next(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			values <- change.Key().DataID
		})
	}
	subscription.publish(Change{selected: key{"g", "four"}})
	subscription.publish(Change{selected: key{"g", "five"}})
	workers.Wait()
	if len(values) != 2 {
		t.Fatal("queued invalidation stranded a waiting consumer")
	}
}
func TestWatchSaturationCancelAndClientIsolation(t *testing.T) {
	fixture := newFixture(t, false)
	input := fixture.options()
	input.QueueCapacity = 2
	client := openClient(t, input)
	subscription, err := client.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = nextChange(t, subscription)
	if extra, err := client.Watch(context.Background()); extra != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("subscription limit bypassed", err)
	}
	for index := range 32 {
		fixture.push(t, pushPayload("ConfigChangeNotifyRequest", fmt.Sprint(index), map[string]any{"tenant": "", "group": "DEFAULT_GROUP", "dataId": "settings.yaml"}))
	}
	waitUntil(t, func() bool { return fixture.acknowledgements.Load() == 32 })
	if change := nextChange(t, subscription); !change.Resync() {
		t.Fatal("saturated observation gap not signaled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatal("saturated close did not join", err)
	}
	select {
	case <-subscription.done:
	default:
		t.Fatal("client returned ahead of watch")
	}
}
func TestWatchRejectsCrossNamespacePush(t *testing.T) {
	fixture := newFixture(t, false)
	client := openClient(t, fixture.options())
	subscription, err := client.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = nextChange(t, subscription)
	fixture.push(t, pushPayload("ConfigChangeNotifyRequest", "cross", map[string]any{"tenant": "other-namespace", "group": "DEFAULT_GROUP", "dataId": "settings.yaml"}))
	change := nextChange(t, subscription)
	if !change.Resync() || !errors.Is(change.Err(), ErrDecode) {
		t.Fatal("cross-namespace push was admitted", change.Err())
	}
}

func TestReconciliationDetectsUnpushedUpdateAndDeletion(t *testing.T) {
	fixture := newFixture(t, false)
	client := openClient(t, fixture.options())
	subscription, err := client.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = nextChange(t, subscription)
	fixture.mu.Lock()
	fixture.values[key{"DEFAULT_GROUP", "settings.yaml"}] = "host: changed-without-push"
	fixture.mu.Unlock()
	if change := nextChange(t, subscription); change.Err() != nil || change.Key().DataID != "settings.yaml" {
		t.Fatal("periodic reconciliation hid an unpushed update", change.Err())
	}
	fixture.mu.Lock()
	delete(fixture.values, key{"DEFAULT_GROUP", "settings.yaml"})
	fixture.mu.Unlock()
	if change := nextChange(t, subscription); change.Err() != nil || change.Key().DataID != "settings.yaml" {
		t.Fatal("periodic reconciliation hid deletion", change.Err())
	}
}

func TestDefaultNamespacePushAcceptsNativeNullOrAbsentTenant(t *testing.T) {
	for _, include := range []bool{false, true} {
		fixture := newFixture(t, false)
		client := openClient(t, fixture.options())
		subscription, err := client.Watch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_ = nextChange(t, subscription)
		fields := map[string]any{"group": "DEFAULT_GROUP", "dataId": "settings.yaml"}
		if include {
			fields["tenant"] = nil
		}
		fixture.push(t, pushPayload("ConfigChangeNotifyRequest", "default-tenant", fields))
		change := nextChange(t, subscription)
		if change.Err() != nil || change.Key().DataID != "settings.yaml" {
			t.Fatal("native default-namespace push was rejected", change.Err())
		}
		waitUntil(t, func() bool { return fixture.acknowledgements.Load() == 1 })
		if err := client.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNamedNamespaceRefusesNullTenant(t *testing.T) {
	fixture := newFixture(t, false)
	input := fixture.options()
	input.Namespace = "private-namespace"
	client := openClient(t, input)
	subscription, err := client.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = nextChange(t, subscription)
	fixture.push(t, pushPayload("ConfigChangeNotifyRequest", "invalid-tenant", map[string]any{"tenant": nil, "group": "DEFAULT_GROUP", "dataId": "settings.yaml"}))
	if change := nextChange(t, subscription); !errors.Is(change.Err(), ErrDecode) || !change.Resync() {
		t.Fatal("default push crossed into named namespace", change.Err())
	}
}
