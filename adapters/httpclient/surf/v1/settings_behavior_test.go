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

package surf_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enetx/g"
	nativehttp "github.com/enetx/http"
	"github.com/enetx/surf/profiles"
	"github.com/enetx/surf/profiles/chrome"
	p "github.com/frost-leo/fathomry/adapters/httpclient/surf/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

type reviewSettingsConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (conn *reviewSettingsConn) Close() error {
	err := conn.Conn.Close()
	conn.once.Do(func() { close(conn.closed) })
	return err
}

func reviewSettingsDial(t *testing.T) (func(context.Context, string, string) (net.Conn, error), *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	var mu sync.Mutex
	var connections []*reviewSettingsConn
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range connections {
			select {
			case <-conn.closed:
			case <-time.After(2 * time.Second):
				t.Error("native TCP socket survived source close")
			}
		}
	})
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		calls.Add(1)
		raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		conn := &reviewSettingsConn{Conn: raw, closed: make(chan struct{})}
		mu.Lock()
		connections = append(connections, conn)
		mu.Unlock()
		return conn, nil
	}, &calls
}

func reviewSettingsReleased(t *testing.T, owner *p.Owner, deps p.Dependencies, receipt *adapters.Receipt[p.Result]) {
	t.Helper()
	direct, err := receipt.WaitReleased(reviewContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(reviewContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("source release", err)
	}
	matched := false
	for {
		usage, err := deps.Evidence.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		if usage.Outstanding == 0 {
			break
		}
		record, err := deps.Evidence.NextReleased(reviewContext(t))
		if err != nil {
			t.Fatal(err)
		}
		independentReceipt, err := record.Receipt()
		if err != nil {
			t.Fatal(err)
		}
		independent, err := independentReceipt.WaitReleased(reviewContext(t))
		if err != nil {
			t.Fatal(err)
		}
		if independent.Info() == direct.Info() {
			matched = true
			if independent.Primary() != direct.Primary() || independent.Cleanup() != direct.Cleanup() {
				t.Fatal("independent evidence changed direct primary/cleanup")
			}
		}
		if err := record.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	status, err := deps.Runtime.Inspect()
	if err != nil || status.Active != 0 || status.Queued != 0 || status.WorkBytes != 0 || !matched {
		t.Fatal("work or independent evidence remained after close", err)
	}
}

func TestPublicNativeRetryDelayAndCancellation(t *testing.T) {
	for _, scenario := range []string{"zero-delay", "nonzero-delay", "canceled-during-delay"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int64
			seen := make(chan time.Time, 8)
			peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				attempt := calls.Add(1)
				select {
				case seen <- time.Now():
				default:
					t.Error("retry fixture received unbounded attempts")
				}
				if attempt == 1 {
					writer.Header().Set("Content-Length", "0")
					writer.WriteHeader(http.StatusServiceUnavailable)
					writer.(http.Flusher).Flush()
					return
				}
				_, _ = io.WriteString(writer, "ok")
			}))
			t.Cleanup(peer.Close)
			delay := 240 * time.Millisecond
			if scenario == "zero-delay" {
				delay = 0
			}
			mode, retries := p.HTTP1Only, 1
			dial, dials := reviewSettingsDial(t)
			prepared, err := p.Prepare(p.Settings{Name: "retry-delay", Mode: &mode, NativeRetries: &retries,
				RetryCodes: []int{http.StatusServiceUnavailable}, RetryDelay: &delay}, p.NativeOptions{DialContext: dial})
			if err != nil {
				t.Fatal(err)
			}
			deps := reviewMechanisms(t, prepared)
			owner, err := prepared.Open(reviewContext(t), deps)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.Close(reviewContext(t)) })
			input, err := nativehttp.NewRequest("GET", peer.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancelCause(reviewContext(t))
			type completed struct {
				receipt *adapters.Receipt[p.Result]
				err     error
			}
			done := make(chan completed, 1)
			go func() {
				receipt, err := owner.Client().Do(ctx, ctx, input)
				done <- completed{receipt, err}
			}()
			joined := false
			t.Cleanup(func() {
				cancel(nil)
				if !joined {
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Error("retry caller survived cancellation")
					}
				}
			})
			var first time.Time
			select {
			case first = <-seen:
			case <-time.After(2 * time.Second):
				t.Fatal("first native attempt absent")
			}
			canceled := errors.New("synthetic method cancellation during native retry delay")
			if scenario == "canceled-during-delay" {
				select {
				case <-seen:
					t.Fatal("configured nonzero delay allowed an immediate second send")
				case <-time.After(delay / 4):
				}
				cancel(canceled)
			}
			var got completed
			select {
			case got = <-done:
				joined = true
			case <-time.After(2 * time.Second):
				t.Fatal("native retry did not finish or honor method cancellation")
			}
			if got.receipt == nil {
				t.Fatal("admitted retry lost its public receipt", got.err)
			}
			snapshot, err := got.receipt.WaitReleased(reviewContext(t))
			if err != nil {
				t.Fatal(err)
			}
			value, present := snapshot.ValueCopy()
			if !present || snapshot.Cleanup() != nil || dials.Load() != 1 {
				t.Fatal("retry cleanup or native socket ownership changed")
			}
			if scenario == "canceled-during-delay" {
				if !errors.Is(got.err, context.Canceled) || !errors.Is(snapshot.Primary(), canceled) ||
					!errors.Is(snapshot.Primary(), context.Canceled) || calls.Load() != 1 || value.RoundTrips() != 1 ||
					value.Metadata().StatusCode() != http.StatusServiceUnavailable || value.Complete() {
					t.Fatal("method cancellation lost first-response evidence or dispatched a second attempt", got.err)
				}
			} else {
				var second time.Time
				select {
				case second = <-seen:
				default:
					t.Fatal("normal retry did not reach the peer")
				}
				if got.err != nil || snapshot.Err() != nil || calls.Load() != 2 || value.RoundTrips() != 2 || !value.Complete() ||
					string(value.DataCopy()) != "ok" || second.Sub(first) < delay {
					t.Fatalf("native configured delay/normal retry changed: interval=%s configured=%s error=%v", second.Sub(first), delay, got.err)
				}
			}
			reviewSettingsReleased(t, owner, deps, got.receipt)
		})
	}
}

func TestPublicNameRefusalPrecedesLazyProfileAndDial(t *testing.T) {
	var profilesCalled, peerCalls atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		peerCalls.Add(1)
		if request.Header.Get("X-Name-Control") != "valid" {
			t.Error("valid lazy profile was not applied")
		}
		_, _ = io.WriteString(writer, "ok")
	}))
	t.Cleanup(peer.Close)
	dial, dials := reviewSettingsDial(t)
	profile := chrome.Desktop
	profile.BuildHeaders = func(profiles.OSKey) *g.MapOrd[g.String, g.String] {
		profilesCalled.Add(1)
		headers := g.NewMapOrd[g.String, g.String]()
		headers.Insert("X-Name-Control", "valid")
		return &headers
	}
	native := p.NativeOptions{Profile: &profile, DialContext: dial}
	mode := p.HTTP1Only
	settings := p.Settings{Name: "name-valid_1.2", Mode: &mode}
	prepared, err := p.Prepare(settings, native)
	if err != nil {
		t.Fatal(err)
	}
	deps := reviewMechanisms(t, prepared)
	deps.Native = native
	for _, name := range []string{"", "Uppercase", "path/name", "white space", strings.Repeat("n", 65), "invalid-\xff"} {
		invalid := settings
		invalid.Name = name
		if _, err := p.Prepare(invalid, native); !errors.Is(err, p.ErrInput) {
			t.Fatal("invalid public name passed preparation", err)
		}
		if owner, err := p.Open(reviewContext(t), invalid, deps); owner != nil || !errors.Is(err, p.ErrInput) {
			t.Fatal("invalid public name acquired a source", err)
		}
	}
	status, err := deps.Runtime.Inspect()
	if err != nil || status.Accepted != 0 || profilesCalled.Load() != 0 || dials.Load() != 0 || peerCalls.Load() != 0 {
		t.Fatal("invalid names entered runtime/profile/native work", err)
	}
	usage, err := deps.Evidence.Inspect()
	if err != nil || usage.Outstanding != 0 {
		t.Fatal("invalid name reserved evidence", err)
	}
	owner, err := p.Open(reviewContext(t), settings, deps)
	if err != nil {
		t.Fatal("unchanged valid-name control", err)
	}
	t.Cleanup(func() { _ = owner.Close(reviewContext(t)) })
	if profilesCalled.Load() != 0 || dials.Load() != 0 {
		t.Fatal("valid source construction eagerly executed native callbacks")
	}
	input, err := nativehttp.NewRequest("GET", peer.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := owner.Client().Do(reviewContext(t), reviewContext(t), input)
	if err != nil || receipt == nil {
		t.Fatal("valid-name request", err)
	}
	snapshot, err := receipt.WaitReleased(reviewContext(t))
	if err != nil {
		t.Fatal(err)
	}
	value, present := snapshot.ValueCopy()
	if !present || snapshot.Err() != nil || !value.Complete() || string(value.DataCopy()) != "ok" ||
		value.Source().Name != settings.Name || profilesCalled.Load() != 1 || dials.Load() != 1 || peerCalls.Load() != 1 {
		t.Fatal("valid name did not preserve source identity and exactly one real lazy profile/dial")
	}
	reviewSettingsReleased(t, owner, deps, receipt)
}
