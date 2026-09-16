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

package nuki

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	nativehttp "github.com/nukilabs/http"
)

type failingClose struct {
	mu     sync.Mutex
	allow  bool
	closed bool
	cause  error
}

func (closer *failingClose) Close() error {
	closer.mu.Lock()
	defer closer.mu.Unlock()
	if !closer.allow {
		return closer.cause
	}
	closer.closed = true
	return nil
}
func TestProviderFailedNativeReleaseStaysChargedAndRetainsCause(t *testing.T) {
	options := providerOptions()
	owner, err := newOwner(defaults(options), options.Native)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("synthetic-close-failure")
	native := &failingClose{cause: cause}
	owned := &socket{owner: owner, closer: native}
	owner.sockets[owned] = struct{}{}
	first := owner.release(testContext(t))
	if first.Released || first.Quiescent || first.Continue == nil || !errors.Is(first.Err, cause) {
		t.Fatal("failed Close fabricated release", first.Err)
	}
	owner.mu.Lock()
	retained := len(owner.sockets)
	owner.mu.Unlock()
	if retained != 1 {
		t.Fatal("failed handle lost its charge")
	}
	native.mu.Lock()
	native.allow = true
	native.mu.Unlock()
	second := first.Continue(testContext(t))
	if !second.Released || !second.Quiescent || !errors.Is(second.Err, cause) {
		t.Fatal("later release erased history", second.Err)
	}
	native.mu.Lock()
	closed := native.closed
	native.mu.Unlock()
	if !closed {
		t.Fatal("release lacked independent close evidence")
	}
}

func TestProviderCanceledBlockedHookRetainsOriginalUse(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	defer release()
	options := providerOptions()
	options.Native.Before = []func(context.Context, *nativehttp.Request) error{func(ctx context.Context, _ *nativehttp.Request) error {
		close(entered)
		<-unblock
		return ctx.Err()
	}}
	fixture := bindProvider(t, options)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	receipt, err := fixture.client.Do(ctx, fault.Correlation{Call: "blocked-hook"}, nativeRequest(t, "GET", "http://127.0.0.1:1", nil))
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	cancel()
	waiting, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err := receipt.WaitFinal(waiting); !errors.Is(err, invocation.ErrWait) {
		t.Fatal("callback was declared finished", err)
	}
	if err := fixture.assembly.Close(testContext(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("callback escaped ownership", err)
	}
	release()
	result := outcome(t, fixture, receipt)
	if !errors.Is(result.Err(), context.Canceled) || result.Attempts.Observed != 0 {
		t.Fatal("callback cancellation evidence changed", result.Err())
	}
}

func TestProviderResponseStatusIsDataAndEmptyIsPresent(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Length", "0")
		writer.WriteHeader(404)
	}))
	defer peer.Close()
	fixture := bindProvider(t, providerOptions())
	receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "empty"}, nativeRequest(t, "GET", peer.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fixture, receipt)
	if result.Err() != nil || !result.Outcome.Present || !result.Outcome.Value.Complete() || result.Outcome.Value.DataCopy() == nil || len(result.Outcome.Value.DataCopy()) != 0 || result.Outcome.Value.Metadata().StatusCode() != 404 {
		t.Fatal("empty/status semantics conflated", result.Err())
	}
}

var _ io.Closer = (*failingClose)(nil)
