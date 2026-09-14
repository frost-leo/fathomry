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

package minio

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type gatedClose struct {
	io.Reader
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
	cause   error
}

func (body *gatedClose) Close() error {
	body.once.Do(func() { close(body.entered) })
	<-body.gate
	return body.cause
}
func TestActualBodyCloseRetainsLeaseAndLateFailure(t *testing.T) {
	_, options := newPeer(t)
	options.MaxActive = 1
	fixture := bindFixture(t, options, 4)
	closeErr := errors.New("late-private-close-error")
	body := &gatedClose{Reader: strings.NewReader("abc"), entered: make(chan struct{}), gate: make(chan struct{}), cause: closeErr}
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(body.gate) }) })
	fixture.client.owner.wire.base = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Length": {"3"}, "Last-Modified": {time.Unix(1700000000, 0).UTC().Format(http.TimeFormat)}}, Body: body, Request: request}, nil
	})
	receipt, err := fixture.client.Read(deadline(t), correlation("late-close"), ReadRequest{Address: Address{Key: "owned/stream"}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-body.entered:
	case <-deadline(t).Done():
		t.Fatal("close was not reached")
	}
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := receipt.WaitReleased(short); err == nil {
		t.Fatal("receipt released while actual body close was blocked")
	}
	if fixture.assembly.Snapshot().Sources[0].Usage.Active != 1 {
		t.Fatal("native body lost its active lease")
	}
	unblock.Do(func() { close(body.gate) })
	result := settle(t, receipt, nil)
	if !errors.Is(result.Outcome.Cleanup, closeErr) || result.Outcome.Primary != nil || string(result.Outcome.Value.DataCopy()) != "abc" {
		t.Fatal("late close failure or data lost", result.Err())
	}
	if fixture.assembly.Snapshot().Sources[0].Usage.Active != 0 {
		t.Fatal("lease not released after actual close")
	}
}
func TestResponseBudgetCannotUnderflowAcrossBodies(t *testing.T) {
	value := defaults(OptionsV1{})
	value.MaxResponseBytes = 1
	state := newExchange(value, nil, false)
	first := &boundedBody{body: io.NopCloser(strings.NewReader("abc")), state: state}
	if _, err := io.ReadAll(first); !errors.Is(err, ErrLimit) {
		t.Fatal("response overflow accepted")
	}
	second := &boundedBody{body: io.NopCloser(strings.NewReader("x")), state: state}
	if _, err := io.ReadAll(second); !errors.Is(err, ErrLimit) {
		t.Fatal("subsequent response regained byte allowance")
	}
}

func TestNativeEOFRewriteRetainsTransportCause(t *testing.T) {
	_, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	fixture.client.owner.wire.base = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, io.EOF
	})
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("transport-eof"), WriteRequest{Key: "owned/eof", Size: 3}, strings.NewReader("abc"))
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), io.EOF) || result.Outcome.Value.Transfer().Effect != Unknown {
		t.Fatal("native URL-bearing EOF rewrite lost the original transport cause")
	}
}
func TestRequestBudgetAndSDKRetryAreIndependent(t *testing.T) {
	server, options := newPeer(t)
	options.MaxRequests = 1
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "PUT" {
			errorResponse(writer, 503, "SlowDown")
			return true
		}
		return false
	}
	server.mu.Unlock()
	before := server.count()
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("no-retry"), WriteRequest{Key: "owned/retry", Size: 3}, strings.NewReader("abc"))
	result := settle(t, receipt, err)
	if result.Err() == nil || server.count()-before != 1 || !result.Attempts.Exact || result.Attempts.Observed != 1 {
		t.Fatal("retry amplification/attempt evidence incorrect")
	}
	server.mu.Lock()
	server.hook = nil
	for _, key := range []string{"owned/a", "owned/b", "owned/c"} {
		server.store(key, []byte("x"), nil)
	}
	server.mu.Unlock()
	options.Name = "bounded-list"
	options.MaxEntries = 1
	second := bindFixture(t, options, 4)
	receipt, err = second.client.List(deadline(t), correlation("request-limit"), ListRequest{Prefix: "owned/"})
	listed := settle(t, receipt, err)
	if !errors.Is(listed.Err(), ErrLimit) || listed.Attempts.Observed != 1 || listed.Outcome.Value.Complete() {
		t.Fatal("listing request budget escaped", listed.Err())
	}
}
