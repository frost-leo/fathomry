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

package httpcloak

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	nativehttp "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/fingerprint"
)

type countedBody struct{ reads, closes atomic.Int64 }

func (body *countedBody) Read([]byte) (int, error) { body.reads.Add(1); return 0, io.EOF }
func (body *countedBody) Close() error             { body.closes.Add(1); return nil }

func TestAliasesQueueAndFrozenInputs(t *testing.T) {
	address, options := peer(t, HTTP2, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/held" {
			w.Header().Set("Content-Length", "8")
			_, _ = io.WriteString(w, "part")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, r.Header.Get("X-Frozen"))
	})
	options.MaxActive = 1
	options.QueuedCalls = 1
	options.MaxBindings = 1
	options.MaxConnections = 1
	fixture := bindFixture(t, options, 3)
	selected := resource.Borrow("alias", fixture.assembly, fixture.selected)
	alias, err := resource.Assemble(testContext(t), testContext(t), "borrower", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := alias.Close(testContext(t)); err != nil {
			t.Error(err)
		}
	})
	client, err := Bind(alias, selected, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, first, err := fixture.client.Open(testContext(t), fault.Correlation{Call: "held"}, request(t, "GET", address+"/held", nil))
	if err != nil {
		t.Fatal(err)
	}
	body := &countedBody{}
	input := request(t, "POST", address, body)
	ctx, cancel := context.WithCancel(testContext(t))
	done := make(chan error, 1)
	go func() {
		receipt, err := client.Do(ctx, testContext(t), fault.Correlation{Call: "canceled-queue"}, input)
		if receipt != nil {
			t.Error("canceled admission returned accepted receipt")
		}
		done <- err
	}()
	waitQueued(t, fixture)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("queued cancellation cause missing", err)
	}
	if body.reads.Load() != 0 || body.closes.Load() != 0 {
		t.Fatal("queued body ownership was taken before admission")
	}
	queued := request(t, "GET", address, nil)
	queued.Header.Set("X-Frozen", "before")
	finished := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, err := client.Do(testContext(t), testContext(t), fault.Correlation{Call: "queued"}, queued)
		if err != nil {
			t.Error(err)
		}
		finished <- receipt
	}()
	waitQueued(t, fixture)
	queued.Header.Set("X-Frozen", "after")
	_ = stream.Close(testContext(t))
	settle(t, fixture, first)
	result := settle(t, fixture, <-finished)
	if string(result.Outcome.Value.DataCopy()) != "before" || result.Source.Configuration.Identity.Name != "source" || result.Source.Scope != "fixture" {
		t.Fatal("alias changed source identity or queued input")
	}
}
func waitQueued(t *testing.T, fixture *fixture) {
	t.Helper()
	ctx := testContext(t)
	for fixture.assembly.Snapshot().Sources[0].Usage.Queued != 1 {
		select {
		case <-ctx.Done():
			t.Fatal("queue boundary not reached")
		case <-time.After(time.Millisecond):
		}
	}
}
func TestIndependentSameNamedNativePresets(t *testing.T) {
	address, options := peer(t, HTTP2, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, r.Header.Get("X-Instance")) })
	const name = "fathomry-independent-native"
	if fingerprint.LookupCustom(name) != nil {
		t.Fatal("test registry name already exists")
	}
	preset := fingerprint.GetStrict("chrome-148")
	preset.Name = name
	preset.HeaderOrder = append(preset.HeaderOrder, fingerprint.HeaderPair{Key: "X-Instance", Value: "first"})
	options.PresetName = ""
	options.Native.Preset = preset
	options.Name = "first"
	first := bindFixture(t, options, 1)
	preset.HeaderOrder[len(preset.HeaderOrder)-1].Value = "second"
	options.Name = "second"
	second := bindFixture(t, options, 1)
	preset.HeaderOrder[len(preset.HeaderOrder)-1].Value = "mutated"
	for index, fixture := range []*fixture{first, second} {
		receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "instance"}, request(t, "GET", address, nil))
		if err != nil {
			t.Fatal(err)
		}
		result := settle(t, fixture, receipt)
		if string(result.Outcome.Value.DataCopy()) != []string{"first", "second"}[index] {
			t.Fatal("native instance snapshots share mutable data")
		}
	}
	if fingerprint.LookupCustom(name) != nil {
		t.Fatal("provider registered a process-global preset")
	}
}
func TestPreCanceledCallAndOptionsValidation(t *testing.T) {
	address, options := peer(t, HTTP1, func(http.ResponseWriter, *http.Request) { t.Error("pre-canceled operation reached SDK peer") })
	fixture := bindFixture(t, options, 1)
	body := &countedBody{}
	ctx, cancel := context.WithCancel(testContext(t))
	cancel()
	receipt, err := fixture.client.Do(ctx, testContext(t), fault.Correlation{Call: "pre-canceled"}, request(t, "POST", address, body))
	if receipt != nil || !errors.Is(err, context.Canceled) || body.reads.Load() != 0 || body.closes.Load() != 0 {
		t.Fatal("pre-canceled call acquired native responsibility")
	}
	for _, options := range []OptionsV1{{Name: "empty"}, {Name: "unknown", PresetName: "not-a-preset"}, {Name: "version", Version: 2, PresetName: "chrome-148"}} {
		if _, err := Select(options); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	input := request(t, "GET", address, nil)
	input.Header = nativehttp.Header{"proxy-authorization": {"private"}}
	if _, _, err := fixture.client.Open(testContext(t), fault.Correlation{Call: "headers"}, input); err == nil {
		t.Fatal("case variation bypassed proxy-credential boundary")
	}
}
