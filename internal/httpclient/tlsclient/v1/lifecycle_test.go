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

package tlsclient

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestProviderCanceledNativeDialRetainsOwnership(t *testing.T) {
	options := providerOptions()
	options.Mode = HTTP1Only
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	sentinel := errors.New("synthetic late dial failure")
	options.Native.DialContext = func(context.Context, string, string) (net.Conn, error) {
		close(entered)
		<-release
		return nil, sentinel
	}
	fixture := bindProvider(t, options, 1)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	returned := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, _ := fixture.client.Do(ctx, testContext(t), providerID("dial"), providerRequest(t, "GET", "https://unused.invalid", nil))
		returned <- receipt
	}()
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("dial not entered")
	}
	cancel()
	receipt := <-returned
	if receipt == nil {
		t.Fatal("accepted dial lost evidence")
	}
	wait, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err := receipt.WaitReleased(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("noncooperative native dial released early", err)
	}
	if result, ok := receipt.Result(); ok && result.Released {
		t.Fatal("noncooperative native dial released early")
	}
	if err := fixture.assembly.Close(testContext(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("closed a source with outstanding native dial", err)
	}
	once.Do(func() { close(release) })
	result := settleProvider(t, fixture, receipt)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), sentinel) {
		t.Fatal("late native failure lost", result.Err())
	}
}

type unconfirmedCloseConn struct {
	net.Conn
	allow *atomic.Bool
	cause error
}

func (conn unconfirmedCloseConn) Close() error {
	_ = conn.Conn.Close()
	if !conn.allow.Load() {
		return conn.cause
	}
	return nil
}

func TestProviderFailedNativeCloseRetainsQuotaAndHistory(t *testing.T) {
	endpoint, options := providerPeer(t, HTTP1Only, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "response") })
	var allow atomic.Bool
	sentinel := errors.New("synthetic unconfirmed close")
	options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return unconfirmedCloseConn{Conn: conn, allow: &allow, cause: sentinel}, nil
	}
	fixture := bindProvider(t, options, 1)
	fixture.cleanupCause = sentinel
	t.Cleanup(func() { allow.Store(true) })
	receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("native-close"), providerRequest(t, "GET", endpoint, nil))
	if err != nil {
		t.Fatal(err)
	}
	settleProvider(t, fixture, receipt)
	if err := fixture.assembly.Close(testContext(t)); !errors.Is(err, resource.ErrIncomplete) || !errors.Is(err, sentinel) {
		t.Fatal("unconfirmed native Close certified release", err)
	}
	owner := fixture.client.owner
	owner.mu.Lock()
	held := owner.tcp > 0 && owner.retiring > 0 && len(owner.held) > 0
	owner.mu.Unlock()
	if !held {
		t.Fatal("unconfirmed native resources lost quota")
	}
	allow.Store(true)
	if err := fixture.assembly.Close(testContext(t)); !errors.Is(err, sentinel) || errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("continued cleanup lost original evidence", err)
	}
	status := fixture.assembly.Snapshot().Sources[0]
	if !status.Quiescent || !status.Released {
		t.Fatal("continued confirmed release stayed unresolved")
	}
}
func TestProviderBorrowedAliasSharesSourceAndLimits(t *testing.T) {
	endpoint, options := providerPeer(t, Negotiated, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "shared") })
	options.MaxActive = 1
	fixture := bindProvider(t, options, 2)
	selected := resource.Borrow("alias", fixture.assembly, fixture.selected)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "borrower", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(testContext(t)); err != nil {
			t.Error(err)
		}
	})
	client, err := Bind(assembly, selected, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, receipt, err := client.Open(testContext(t), providerID("borrowed"), providerRequest(t, "GET", endpoint, nil))
	if err != nil {
		t.Fatal(err)
	}
	if rejected, err := fixture.client.Do(testContext(t), testContext(t), providerID("second"), providerRequest(t, "GET", endpoint, nil)); rejected != nil || !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("borrow multiplied quota", err)
	}
	if err := stream.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	result := settleProvider(t, fixture, receipt)
	if result.Context.Source != options.Name || result.Source.Scope != "local" {
		t.Fatal("alias relabeled source")
	}
	if err := assembly.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	receipt, err = fixture.client.Do(testContext(t), testContext(t), providerID("original"), providerRequest(t, "GET", endpoint, nil))
	if err != nil {
		t.Fatal("closing borrower closed original native client", err)
	}
	settleProvider(t, fixture, receipt)
}
func TestProviderBindingCeilingAndConfirmedRetirement(t *testing.T) {
	endpoint, options := providerPeer(t, HTTP1Only, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "bounded") })
	options.MaxBindings = 1
	fixture := bindProvider(t, options, 2)
	first := RequestOptionsV1{ConnectHeaders: map[string][]string{"X-Route": {"one"}}}
	second := RequestOptionsV1{ConnectHeaders: map[string][]string{"X-Route": {"two"}}}
	stream, receipt, err := fixture.client.Open(testContext(t), providerID("held"), providerRequest(t, "GET", endpoint, nil), first)
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := fixture.client.Do(testContext(t), testContext(t), providerID("refused"), providerRequest(t, "GET", endpoint, nil), second)
	if !errors.Is(err, ErrLimit) {
		t.Fatal("live binding ceiling bypassed", err)
	}
	// Both accepted calls have independent evidence; finish in delivery order.
	if err := stream.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		delivery, err := fixture.inbox.Next(testContext(t))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := delivery.Receipt().WaitReleased(testContext(t)); err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := receipt.WaitReleased(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := rejected.WaitReleased(testContext(t)); err != nil {
		t.Fatal(err)
	}
	next, err := fixture.client.Do(testContext(t), testContext(t), providerID("retired"), providerRequest(t, "GET", endpoint, nil), second)
	if err != nil {
		t.Fatal("confirmed retirement failed", err)
	}
	settleProvider(t, fixture, next)
	owner := fixture.client.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if len(owner.bindings) != 1 || owner.retiring != 0 || len(owner.held) != 0 {
		t.Fatal("binding retirement accounting differs")
	}
}
