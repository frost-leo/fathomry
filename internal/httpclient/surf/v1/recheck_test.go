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
package surf

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	sdk "github.com/enetx/surf"
	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type routeWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *routeWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestRecheckHealthyWaiterDoesNotInheritCanceledConstruction(t *testing.T) {
	entered := make(chan struct{})
	var listens atomic.Int32
	fix := newFixture(t, OptionsV1{Name: "waiting-route", Mode: PreferHTTP3, MaxRoutes: 1,
		Native: NativeOptionsV1{ListenPacket: func(ctx context.Context, network, address string) (net.PacketConn, error) {
			if listens.Add(1) == 1 {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return (&net.ListenConfig{}).ListenPacket(ctx, network, address)
		}},
	}, 1)
	firstContext, cancel := context.WithCancel(testContext(t))
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := fix.client.owner.binding(firstContext, routeChoice{}); first <- err }()
	<-entered
	secondContext := &routeWaitContext{Context: testContext(t), waiting: make(chan struct{})}
	second := make(chan error, 1)
	go func() { _, err := fix.client.owner.binding(secondContext, routeChoice{}); second <- err }()
	<-secondContext.waiting
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal("first construction was not canceled", err)
	}
	if err := <-second; err != nil || secondContext.Err() != nil || listens.Load() != 2 {
		t.Fatal("healthy concurrent waiter inherited unrelated cancellation", err)
	}
}

type cleanupErrorConn struct {
	net.Conn
	cause error
}

func (conn cleanupErrorConn) Close() error {
	_ = conn.Conn.Close()
	return conn.cause
}
func TestRecheckCleanupFailureStopsUnboundedNativeReuse(t *testing.T) {
	cause := errors.New("native connection closed with diagnostic")
	var dials atomic.Int32
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }))
	defer peer.Close()
	fix := newFixture(t, OptionsV1{Name: "cleanup-quarantine", Mode: HTTP1Only, MaxTCPConnections: 1,
		Native: NativeOptionsV1{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return cleanupErrorConn{conn, cause}, nil
		}},
	}, 3)
	fix.cleanupCause = cause
	for _, id := range []string{"first", "second", "third"} {
		input := request(t, "GET", peer.URL, "")
		input.Close = true
		receipt, _ := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: id}, input)
		settle(t, fix, receipt)
	}
	if dials.Load() != 1 {
		t.Error("cleanup failure allowed continuing acquisitions and unbounded error history", dials.Load())
	}
	if err := fix.assembly.Close(testContext(t)); !errors.Is(err, cause) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("native cleanup cause absent or release never finished", err)
	}
}

func TestRecheckRetryProfilesPassActualAssessment(t *testing.T) {
	build, err := Build()
	if err != nil {
		t.Fatal(err)
	}
	maximum := make([]int, 64)
	for index := range maximum {
		maximum[index] = 100 + index
	}
	var normalized []compatibility.Option
	cases := []struct {
		name    string
		retries int
		codes   []int
	}{
		{"disabled", 0, nil}, {"default", 1, nil}, {"multiple", 1, []int{503, 429}},
		{"duplicate", 1, []int{429, 503, 429}}, {"maximum", 1, maximum},
	}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			fix := newFixture(t, OptionsV1{Name: "assessment", NativeRetries: fixture.retries, RetryCodes: fixture.codes}, 1)
			access, err := resource.AccessFor(fix.assembly, fix.selected)
			if err != nil {
				t.Fatal(err)
			}
			report, err := compatibility.Assess(build, access, fix.client.Profile(),
				[]compatibility.Requirement{{Guarantee: "native-http", Layers: []compatibility.Layer{compatibility.SDK, compatibility.Capability}}}, nil)
			if err != nil {
				t.Fatal("effective Profile is unusable by actual internal assessment", err)
			}
			if len(report.Decisions) != 1 || report.Decisions[0].Status == compatibility.Tested {
				t.Fatal("no evidence became certification")
			}
			if fixture.name == "multiple" {
				normalized = report.Actual.Profile.Options
			}
			if fixture.name == "duplicate" && !reflect.DeepEqual(normalized, report.Actual.Profile.Options) {
				t.Fatal("equivalent effective native policy has different compatibility projection")
			}
		})
	}
}

func TestRecheckLateNotificationFailureIsRetainedAfterCallRelease(t *testing.T) {
	cause := errors.New("late TLS cache notification")
	cache := &blockingTLSCache{entered: make(chan struct{}), release: make(chan struct{}), failure: cause}
	var once sync.Once
	unblock := func() { once.Do(func() { close(cache.release) }) }
	defer unblock()
	peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }, false)
	fix := newFixture(t, OptionsV1{Name: "late-cache", Mode: HTTP1Only,
		Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots, ClientSessionCache: cache}}}, 1)
	fix.cleanupCause = cause
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan *invocation.Receipt[Result], 1)
	go func() {
		_, receipt, _ := fix.client.Open(ctx, fault.Correlation{Call: "late"}, request(t, "GET", peer.tcp.URL, ""))
		done <- receipt
	}()
	select {
	case <-cache.entered:
	case <-ctx.Done():
		t.Fatal("post-handshake callback did not start")
	}
	cancel()
	value := settle(t, fix, <-done)
	if errors.Is(value.Err(), cause) {
		t.Fatal("late callback failure was reported before it occurred")
	}
	unblock()
	if err := fix.assembly.Close(testContext(t)); !errors.Is(err, cause) || !errors.Is(err, sdk.ErrFathomryCallback) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("late notification failure lost from resource cleanup", err)
	}
}

func TestRecheckCanceledStreamReadsKeepBoundedEvidence(t *testing.T) {
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }))
	defer peer.Close()
	fix := newFixture(t, OptionsV1{Name: "repeated-read", Mode: HTTP1Only}, 1)
	ctx, cancel := context.WithCancelCause(testContext(t))
	defer cancel(nil)
	stream, receipt, err := fix.client.Open(ctx, fault.Correlation{Call: "read"}, request(t, "GET", peer.URL, ""))
	if err != nil {
		t.Fatal(err)
	}
	stream.gate <- struct{}{}
	cause := errors.New("canceled stream")
	cancel(cause)
	_, _ = stream.Read(make([]byte, 1))
	stream.op.mu.Lock()
	first := stream.op.primary
	stream.op.mu.Unlock()
	for range 100 {
		if _, err := stream.Read(make([]byte, 1)); !errors.Is(err, cause) {
			t.Fatal("sticky cancellation lost cause")
		}
	}
	stream.op.mu.Lock()
	unchanged := first == stream.op.primary
	stream.op.mu.Unlock()
	<-stream.gate
	_ = stream.Close(testContext(t))
	settle(t, fix, receipt)
	if !unchanged {
		t.Fatal("repeated reads appended an unbounded duplicate error history")
	}
}
