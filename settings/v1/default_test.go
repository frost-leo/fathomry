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

package settings_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/settings/v1"
)

func TestDefault(t *testing.T) {
	const helper = "FATHOMRY_SETTINGS_DEFAULT_TEST"
	if os.Getenv(helper) != "child" {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDefault$", "-test.timeout=30s", "-test.v")
		command.Env = append(os.Environ(), helper+"=child", "GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
		output, err := command.CombinedOutput()
		if ctx.Err() != nil || err != nil || !strings.Contains(string(output), "default contract completed") {
			t.Fatalf("isolated default contract failed: %v\n%s", err, output)
		}
		return
	}

	type config struct {
		Installation int    `json:"installation"`
		Revision     int    `json:"revision"`
		Pair         [2]int `json:"pair"`
	}
	read := func(view settings.View) config {
		t.Helper()
		value, found, err := settings.Read(view, "", func(value config) config { return value })
		if err != nil || !found {
			t.Fatal("default did not retain its original root type")
		}
		return value
	}
	current := func() settings.View {
		t.Helper()
		view, err := settings.Default()
		if err != nil {
			t.Fatal(err)
		}
		return view
	}
	if settings.Configured() {
		t.Fatal("default configured without installation")
	}
	if _, err := settings.Default(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("early default did not reject")
	}
	empty := settings.NewStore[config]()
	for _, reader := range []settings.Reader{{}, empty.Reader()} {
		if err := settings.Configure(reader); !errors.Is(err, settings.ErrUnconfigured) {
			t.Fatal("unpublished reader installed")
		}
	}
	if settings.Configured() {
		t.Fatal("rejected installation claimed the default")
	}

	var copies atomic.Int32
	clone := func(value config) config { copies.Add(1); return value }
	publish := func(store settings.Store[config], value config) error {
		snapshot, err := settings.New(value, clone)
		if err != nil {
			return err
		}
		return store.Publish(snapshot)
	}
	const contenders = 24
	stores := make([]settings.Store[config], contenders)
	for index := range stores {
		stores[index] = settings.NewStore[config]()
		if err := publish(stores[index], config{Installation: index}); err != nil {
			t.Fatal(err)
		}
	}
	if settings.Configured() {
		t.Fatal("ordinary publication installed a process default")
	}
	type outcome struct {
		index int
		err   error
	}
	outcomes := make(chan outcome, contenders)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range stores {
		group.Go(func() {
			<-start
			outcomes <- outcome{index, settings.Configure(stores[index].Reader())}
		})
	}
	close(start)
	group.Wait()
	close(outcomes)
	winner, wins := -1, 0
	for result := range outcomes {
		if result.err == nil {
			winner = result.index
			wins++
		} else if !errors.Is(result.err, settings.ErrConfigured) {
			t.Fatal("unexpected competing installation error")
		}
	}
	if wins != 1 || !settings.Configured() {
		t.Fatal("default installation did not have exactly one winner")
	}
	initial := current()
	if got := read(initial); got.Installation != winner || got.Revision != 0 {
		t.Fatal("wrong installation selected")
	}
	if copies.Load() != contenders {
		t.Fatal("installation or capture called the root cloner")
	}

	for _, reader := range []settings.Reader{{}, empty.Reader()} {
		if err := settings.Configure(reader); !errors.Is(err, settings.ErrUnconfigured) {
			t.Fatal("invalid-reader admission priority changed")
		}
	}
	for _, store := range stores {
		if err := settings.Configure(store.Reader()); !errors.Is(err, settings.ErrConfigured) {
			t.Fatal("default rebound")
		}
	}
	if err := stores[winner].Publish(settings.Snapshot[config]{}); !errors.Is(err, settings.ErrSnapshot) {
		t.Fatal("invalid update accepted")
	}
	if read(current()) != read(initial) {
		t.Fatal("invalid update changed the default")
	}
	if err := publish(stores[winner], config{Installation: winner, Revision: 1, Pair: [2]int{1, 1}}); err != nil {
		t.Fatal(err)
	}
	if read(current()).Revision != 1 || read(initial).Revision != 0 {
		t.Fatal("publication did not preserve captured views")
	}

	business := settings.NewStore[string]()
	businessSnapshot, err := settings.New("business", func(value string) string { return value })
	if err != nil {
		t.Fatal(err)
	}
	if err := business.Publish(businessSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := settings.Configure(business.Reader()); !errors.Is(err, settings.ErrConfigured) {
		t.Fatal("business settings replaced application")
	}
	if read(current()).Revision != 1 {
		t.Fatal("independent domain changed application settings")
	}

	for worker := 0; worker < 4; worker++ {
		group.Go(func() {
			for revision := 1; revision <= 500; revision++ {
				if err := publish(stores[winner], config{Installation: winner, Revision: revision, Pair: [2]int{revision, revision}}); err != nil {
					t.Error(err)
					return
				}
			}
		})
		group.Go(func() {
			for count := 0; count < 1500; count++ {
				view, err := settings.Default()
				if err != nil {
					t.Error(err)
					return
				}
				value, found, err := settings.Read(view, "", func(value config) config { return value })
				if err != nil || !found || value.Installation != winner || value.Pair[0] != value.Revision || value.Pair[1] != value.Revision {
					t.Error("concurrent default reader observed torn fields")
					return
				}
			}
		})
	}
	group.Wait()
	if err := publish(stores[winner], config{Installation: winner, Revision: 9999, Pair: [2]int{9999, 9999}}); err != nil {
		t.Fatal(err)
	}
	if read(current()).Revision != 9999 || read(initial).Revision != 0 {
		t.Fatal("final/default view semantics changed")
	}
	if copies.Load() != contenders+1+4*500+1 {
		t.Fatal("a publication/capture ran the root copy policy")
	}
	t.Log("default contract completed")
}
