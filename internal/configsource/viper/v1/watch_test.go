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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
)

func TestFileWatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.toml")
	write := func(value string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("value=1")
	subscription, err := Watch(context.Background(), WatchOptionsV1{Paths: []string{path}, Interval: 10 * time.Millisecond, QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := subscription.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	next := func() Change {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		change, err := subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return change
	}
	if first := next(); !first.Resync() {
		t.Fatal("missing initial resync")
	}
	write("value=2")
	if change := next(); change.Index() != 0 {
		t.Fatal("file write not observed")
	}
	replacement := path + ".next"
	if err := os.WriteFile(replacement, []byte("value=3"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if change := next(); change.Index() != 0 {
		t.Fatal("atomic replacement not observed")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if change := next(); change.Index() != 0 {
		t.Fatal("deletion not observed")
	}
	subscription.publish(Change{index: 0})
	subscription.publish(Change{index: 0})
	if change := next(); !change.Resync() {
		t.Fatal("overflow evidence lost")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := subscription.Next(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("wait cancellation lost")
	}
	if err := subscription.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := subscription.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := subscription.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal("closed subscription read succeeded")
	}
}

func TestFileWatchBoundsAndWaiters(t *testing.T) {
	t.Run("aggregate_byte_budget", func(t *testing.T) {
		directory := t.TempDir()
		paths := make([]string, 5)
		for index := range paths {
			paths[index] = filepath.Join(directory, fmt.Sprintf("%d.yaml", index))
			if err := os.WriteFile(paths[index], []byte(strings.Repeat("x", MaxDocumentBytes)), 0600); err != nil {
				t.Fatal(err)
			}
		}
		subscription, err := Watch(context.Background(), WatchOptionsV1{Paths: paths})
		if err != nil {
			t.Fatal(err)
		}
		defer subscription.Close(context.Background())
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		change, err := subscription.Next(ctx)
		if err != nil || change.Index() != 4 || !change.Resync() || !errors.Is(change.Err(), ErrLimit) {
			t.Fatal("aggregate exhaustion lost limit evidence", err)
		}
	})
	t.Run("multiple_waiters_and_owned_shutdown", func(t *testing.T) {
		options := WatchOptionsV1{Paths: []string{filepath.Join(t.TempDir(), "absent")}, Interval: 5 * time.Minute}
		subscription, err := Watch(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		defer subscription.Close(context.Background())
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := subscription.Next(ctx); err != nil {
			t.Fatal(err)
		}
		delivered := make(chan error, 2)
		for range 2 {
			go func() { _, err := subscription.Next(ctx); delivered <- err }()
		}
		subscription.publish(Change{index: 0})
		subscription.publish(Change{index: 0})
		for range 2 {
			if err := <-delivered; err != nil {
				t.Fatal("waiter stranded queued invalidation", err)
			}
		}
		go func() { _, err := subscription.Next(ctx); delivered <- err }()
		if err := subscription.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-delivered; !errors.Is(err, ErrClosed) {
			t.Fatal("close did not release waiting consumer")
		}
		conformance.Runtime(t, options, new(WatchOptionsV1), options.Paths[0])
		conformance.Runtime(t, subscription, new(Subscription), options.Paths[0])
		conformance.Runtime(t, Change{err: errors.New("watch-canary")}, new(Change), "watch-canary")
	})
	t.Run("timed_out_close_retains_owner", func(t *testing.T) {
		work, cancel := context.WithCancel(context.Background())
		subscription := &Subscription{cancel: cancel, done: make(chan struct{})}
		expired, stop := context.WithCancel(context.Background())
		stop()
		if err := subscription.Close(expired); !errors.Is(err, ErrState) || work.Err() == nil {
			t.Fatal("close timeout lost retained shutdown responsibility")
		}
		close(subscription.done)
		if err := subscription.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
