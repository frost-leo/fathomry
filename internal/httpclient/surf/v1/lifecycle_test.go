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
	"crypto/x509"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	utls "github.com/refraction-networking/utls"
)

func TestCancellationPreservesUnknownRemoteEffectsAndIndependentCause(t *testing.T) {
	reached := make(chan struct{})
	serverDone := make(chan struct{})
	var effects atomic.Int32
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		effects.Add(1)
		close(reached)
		<-r.Context().Done()
		close(serverDone)
	}))
	defer peer.Close()
	fix := newFixture(t, OptionsV1{Name: "cancel", Mode: HTTP1Only}, 1)
	ctx, cancel := context.WithCancelCause(testContext(t))
	cause := errors.New("caller cancellation")
	type response struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan response, 1)
	go func() {
		receipt, err := fix.client.Do(ctx, testContext(t), fault.Correlation{Call: "cancel"}, request(t, "POST", peer.URL, "effect"))
		done <- response{receipt, err}
	}()
	<-reached
	cancel(cause)
	var got response
	select {
	case got = <-done:
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not end waiting")
	}
	if got.err == nil || !errors.Is(got.err, context.Canceled) {
		t.Fatal("cancellation cause absent")
	}
	result := settle(t, fix, got.receipt)
	if !errors.Is(result.Err(), cause) || !errors.Is(result.Err(), context.Canceled) || result.Outcome.Value.Complete() || effects.Load() != 1 {
		t.Fatal("cancellation invented non-effect or dropped causality", result.Err())
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("test peer did not observe terminated request")
	}
}

type heldInput struct {
	readEntered, readRelease, closeEntered, closeRelease chan struct{}
	readOnce, closeOnce                                  sync.Once
	closes                                               atomic.Int32
}

func (input *heldInput) Read([]byte) (int, error) {
	input.readOnce.Do(func() { close(input.readEntered) })
	<-input.readRelease
	return 0, io.EOF
}
func (input *heldInput) Close() error {
	input.closes.Add(1)
	input.closeOnce.Do(func() { close(input.closeEntered) })
	<-input.closeRelease
	return nil
}
func TestUncooperativeInputDoesNotReleaseOnCallerReturn(t *testing.T) {
	fix := newFixture(t, OptionsV1{Name: "held", Mode: HTTP1Only, MaxActive: 1}, 1)
	body := &heldInput{readEntered: make(chan struct{}), readRelease: make(chan struct{}), closeEntered: make(chan struct{}), closeRelease: make(chan struct{})}
	input := request(t, "POST", "http://127.0.0.1:1", "")
	input.Body = body
	input.ContentLength = -1
	input.GetBody = nil
	ctx, cancel := context.WithCancel(testContext(t))
	type answer struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan answer, 1)
	go func() {
		_, receipt, err := fix.client.Open(ctx, fault.Correlation{Call: "held"}, input)
		done <- answer{receipt, err}
	}()
	<-body.readEntered
	cancel()
	got := <-done
	if got.receipt == nil || got.err == nil {
		t.Fatal("waiting return lost receipt")
	}
	<-body.closeEntered
	if value, ok := got.receipt.Result(); ok && value.Released {
		t.Fatal("blocked reader/closer falsely released")
	}
	cleanup, cancelCleanup := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelCleanup()
	if err := fix.assembly.Close(cleanup); err == nil {
		t.Fatal("assembly released a still-borrowed native input")
	}
	close(body.readRelease)
	close(body.closeRelease)
	result := settle(t, fix, got.receipt)
	if result.Outcome.Value.Complete() || !result.Released || body.closes.Load() != 1 {
		t.Fatal("input termination evidence changed")
	}
}

func TestNativeFactoryOutlivesWaitButNotItsLease(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }, false)
	options := OptionsV1{Name: "factory", Mode: HTTP1Only, Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots},
		HelloSpecFactory: func(context.Context) (utls.ClientHelloSpec, error) {
			once.Do(func() { close(entered) })
			<-release
			return utls.UTLSIdToSpec(utls.HelloChrome_Auto)
		}}}
	fix := newFixture(t, options, 1)
	ctx, cancel := context.WithCancel(testContext(t))
	type answer struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan answer, 1)
	go func() {
		_, receipt, err := fix.client.Open(ctx, fault.Correlation{Call: "factory"}, request(t, "GET", peer.tcp.URL, ""))
		done <- answer{receipt, err}
	}()
	<-entered
	cancel()
	got := <-done
	if got.err == nil {
		t.Fatal("wait did not report cancellation")
	}
	if value, ok := got.receipt.Result(); ok && value.Released {
		t.Fatal("native callback still owns borrowed state")
	}
	close(release)
	value := settle(t, fix, got.receipt)
	if !errors.Is(value.Err(), context.Canceled) || value.Outcome.Value.Complete() {
		t.Fatal("native callback cancellation lost")
	}
}

func TestNativeMiddlewareViewsCannotEscapeOriginalClient(t *testing.T) {
	var calls, requests, responses atomic.Int32
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		calls.Add(1)
		if r.Header.Get("X-Native") != "view" {
			t.Error("native header extension missing")
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer peer.Close()
	options := OptionsV1{Name: "views", Mode: HTTP1Only, Native: NativeOptionsV1{
		RequestMiddleware: []func(*sdk.Request) error{func(view *sdk.Request) error {
			requests.Add(1)
			if got := view.Do(); got.IsOk() {
				t.Error("request view performed unmanaged I/O")
			}
			view.SetHeaders("X-Native", "view")
			return nil
		}},
		ResponseMiddleware: []func(*sdk.Response) error{func(view *sdk.Response) error {
			responses.Add(1)
			if view.Body != nil || view.GetResponse().Body != nil || view.GetResponse().Request.Body != nil || view.GetResponse().Request.GetBody != nil {
				t.Error("response view exported body ownership")
			}
			if got := view.Get("http://127.0.0.1:1").Do(); got.IsOk() {
				t.Error("response view exposed a live client")
			}
			_ = view.Close()
			return nil
		}},
	}}
	fix := newFixture(t, options, 2)
	for _, name := range []string{"one", "two"} {
		receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: name}, request(t, "GET", peer.URL, ""))
		if err != nil {
			t.Fatal(err)
		}
		if got := settle(t, fix, receipt); !got.Outcome.Value.Complete() {
			t.Fatal("view altered native response")
		}
	}
	if calls.Load() != 2 || requests.Load() != 2 || responses.Load() != 2 {
		t.Fatal("native bypass or callback disappearance")
	}
}

func TestNativeConstructionFailureCleansAcquiredPacket(t *testing.T) {
	cause := errors.New("packet constructor partial failure")
	var acquired []net.PacketConn
	var mu sync.Mutex
	options := OptionsV1{Name: "partial", Mode: PreferHTTP3, Native: NativeOptionsV1{
		ListenPacket: func(ctx context.Context, network, address string) (net.PacketConn, error) {
			packet, err := (&net.ListenConfig{}).ListenPacket(ctx, network, address)
			if err != nil {
				return nil, err
			}
			mu.Lock()
			acquired = append(acquired, packet)
			mu.Unlock()
			return packet, cause
		},
	}}
	fix := newFixture(t, options, 1)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "partial"}, request(t, "GET", "https://127.0.0.1:1", ""))
	if err == nil {
		t.Fatal("partial constructor success invented")
	}
	value := settle(t, fix, receipt)
	if !errors.Is(value.Err(), cause) || value.Outcome.Present {
		t.Fatal("construction cause or missing state lost", value.Err())
	}
	mu.Lock()
	packets := append([]net.PacketConn(nil), acquired...)
	mu.Unlock()
	if len(packets) == 0 {
		t.Fatal("failure oracle did not acquire a packet")
	}
	for _, packet := range packets {
		if err := packet.SetReadDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
			t.Fatal("acquired socket survived failure", err)
		}
	}
}

func TestBorrowedAliasRetainsOriginalAdmission(t *testing.T) {
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(200)
		w.(stdhttp.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer peer.Close()
	options := OptionsV1{Name: "original", Mode: HTTP1Only, MaxActive: 1}
	fix := newFixture(t, options, 2)
	borrowed := resource.Borrow("alias", fix.assembly, fix.selected)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "borrower", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(testContext(t))
	alias, err := Bind(assembly, borrowed, fix.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, receipt, err := fix.client.Open(testContext(t), fault.Correlation{Call: "held"}, request(t, "GET", peer.URL, ""))
	if err != nil {
		t.Fatal(err)
	}
	second, err := alias.Do(testContext(t), testContext(t), fault.Correlation{Call: "alias"}, request(t, "GET", peer.URL, ""))
	if second != nil || !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("alias enlarged original quota", err)
	}
	_ = stream.Close(testContext(t))
	value := settle(t, fix, receipt)
	if value.Context.Source != "original" || value.Outcome.Value.Complete() {
		t.Fatal("identity or unread completeness changed")
	}
}

func TestJAConfigPreservesNativeVerificationCause(t *testing.T) {
	cause := errors.New("native verification callback")
	peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		t.Error("verification failure reached HTTP handler")
	}, false)
	profile := chrome.Desktop
	options := OptionsV1{Name: "verification", Mode: HTTP1Only, Native: NativeOptionsV1{Profile: &profile,
		JAConfig: &utls.Config{RootCAs: peer.roots, OmitEmptyPsk: true, VerifyPeerCertificate: func([][]byte, [][]*x509.Certificate) error { return cause }},
	}}
	fix := newFixture(t, options, 1)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "verify"}, request(t, "GET", peer.tcp.URL, ""))
	if err == nil {
		t.Fatal("native verification failure ignored")
	}
	value := settle(t, fix, receipt)
	if !errors.Is(value.Err(), cause) {
		t.Fatal("native verification cause erased")
	}
}
