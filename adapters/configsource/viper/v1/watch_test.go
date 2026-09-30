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

package viper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestWatch(t *testing.T) {
	t.Run("initial change wait cancellation and retained evidence", func(t *testing.T) {
		client, runtime, inbox := testClient(t, 0)
		path := filepath.Join(t.TempDir(), "config.yaml")
		subscription, err := client.Watch(context.Background(), WatchSettings{Paths: []string{path}, Interval: 10 * time.Millisecond, QueueCapacity: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer subscription.Close(context.Background())
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		change, err := subscription.Next(ctx)
		if err != nil || !change.Resync() || change.Index() != -1 {
			t.Fatal("initial resync missing", err)
		}
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := delivery.Receipt()
		if err != nil {
			t.Fatal(err)
		}
		if snapshot, resolved := receipt.Snapshot(); resolved || snapshot.Info().Released {
			t.Fatal("released live watcher")
		}
		stopped, stop := context.WithCancel(context.Background())
		stop()
		if _, err := subscription.Next(stopped); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("value: next"), 0600); err != nil {
			t.Fatal(err)
		}
		change, err = subscription.Next(ctx)
		if err != nil || change.Index() != 0 || change.Err() != nil {
			t.Fatal("canceled wait stopped source", err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		change, err = subscription.Next(ctx)
		if err != nil || change.Index() != 0 {
			t.Fatal(err)
		}
		if err := subscription.Close(stopped); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := subscription.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := subscription.Next(ctx); !errors.Is(err, ErrClosed) {
			t.Fatal("delivery after completed close", err)
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil || !snapshot.Info().Released || snapshot.Err() != nil {
			t.Fatal("lifecycle evidence incomplete", err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
		stats, _ := runtime.Inspect()
		if stats.Active != 0 {
			t.Fatal("retained completed watcher", stats)
		}
	})
	t.Run("read error resync and concurrent close", func(t *testing.T) {
		client, _, _ := testClient(t, 0)
		subscription, err := client.Watch(context.Background(), WatchSettings{Paths: []string{t.TempDir()}, Interval: 10 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		change, err := subscription.Next(ctx)
		if err != nil || !change.Resync() || change.Err() == nil {
			t.Fatal("read refusal disappeared", err)
		}
		var group sync.WaitGroup
		for range 8 {
			group.Go(func() {
				if err := subscription.Close(ctx); err != nil {
					t.Error(err)
				}
			})
		}
		group.Wait()
	})
	t.Run("runtime cancellation joins subscription", func(t *testing.T) {
		client, runtime, _ := testClient(t, 0)
		subscription, err := client.Watch(context.Background(), WatchSettings{Paths: []string{filepath.Join(t.TempDir(), "absent")}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if err := subscription.Close(ctx); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("admission and zero handles", func(t *testing.T) {
		client, _, inbox := testClient(t, 1)
		path := filepath.Join(t.TempDir(), "absent")
		if _, err := client.Watch(context.Background(), WatchSettings{Paths: []string{path}, Interval: time.Nanosecond}); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
		if _, err := client.Watch(context.Background(), WatchSettings{Paths: []string{path}}); !errors.Is(err, adapters.ErrEvidence) {
			t.Fatal(err)
		}
		if err := inbox.DeliverOne(context.Background(), func(context.Context, adapters.Snapshot[Evidence]) error { return nil }); err != nil {
			t.Fatal(err)
		}
		var zero Subscription
		if _, err := zero.Next(context.Background()); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
		if err := zero.Close(context.Background()); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
	})
}
func TestDiagnostics(t *testing.T) {
	const secret = "private-configuration-canary"
	client, _, _ := testClient(t, 0)
	documents, err := client.Load(context.Background(), []Input{{Settings: Settings{Encoding: "yaml"}, Reader: strings.NewReader("text: " + secret)}})
	if err != nil {
		t.Fatal(err)
	}
	captured, err := documents[0].Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{client, documents[0], *documents[0], captured, Input{File: secret}, Change{err: errors.New(secret)}, (*Document)(nil), (*Subscription)(nil), Settings{EnvPrefix: secret}, Scalar{Text: secret}} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
			if strings.Contains(fmt.Sprintf(format, value), secret) {
				t.Fatal("format leak")
			}
		}
		var output strings.Builder
		slog.New(slog.NewJSONHandler(&output, nil)).Info("safe", "value", value)
		if strings.Contains(output.String(), secret) {
			t.Fatal("log leak")
		}
	}
	for _, value := range []any{client, documents[0], captured, Input{File: secret}} {
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime serialized")
		}
	}
	data, err := json.Marshal(Settings{EnvPrefix: secret})
	if err != nil || !strings.Contains(string(data), secret) {
		t.Fatal("settings are not loadable data")
	}
	var settings Settings
	if err := json.Unmarshal(data, &settings); err != nil || settings.EnvPrefix != secret {
		t.Fatal("settings failed roundtrip", err)
	}
}
