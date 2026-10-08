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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/enetx/surf"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

type reviewMultipartInput struct {
	io.Reader
	closed   atomic.Int64
	closeErr error
}

func (input *reviewMultipartInput) Close() error { input.closed.Add(1); return input.closeErr }

type reviewForeignProducerMethod struct {
	io.ReadCloser
	called atomic.Int64
}

func (input *reviewForeignProducerMethod) FathomryProducerError() error {
	input.called.Add(1)
	panic("ordinary request body extra method is not callback authority")
}

func TestMultipartNoticeDoesNotInvokeForeignBodyMethods(t *testing.T) {
	fixture := newFixture(t, OptionsV1{Name: "ordinary-body-method"}, 1)
	op := &operation{client: fixture.client, ctx: testContext(t), work: newActivity()}
	foreign := &reviewForeignProducerMethod{ReadCloser: io.NopCloser(strings.NewReader("ordinary"))}
	input := op.input(foreign)
	var escaped any
	func() {
		defer func() { escaped = recover() }()
		_ = input.Close()
	}()
	if escaped != nil || foreign.called.Load() != 0 {
		t.Fatal("ordinary io.ReadCloser acquired an undocumented producer callback and escaped containment", foreign.called.Load())
	}
}

func TestMultipartEncodedLimitNotice(t *testing.T) {
	fixture := newFixture(t, OptionsV1{Name: "encoded-limit", MaxRequestBytes: 1}, 1)
	op := &operation{client: fixture.client, ctx: testContext(t), work: newActivity()}
	input := op.input(io.NopCloser(strings.NewReader("ab")))
	count, err := input.Read(make([]byte, 2))
	if count != 1 || !errors.Is(err, ErrLimit) {
		t.Fatal("encoded limit rejecting control failed", count, err)
	}
	if !errors.Is(input.readErr, ErrLimit) {
		t.Fatal("generated encoded limit error was omitted from the independent input notice")
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMultipartPartialReplayFactoryCleanup(t *testing.T) {
	var calls atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		calls.Add(1)
		writer.Header().Set("Location", "/replay")
		writer.WriteHeader(307)
		_, _ = io.WriteString(writer, "retained")
	}))
	defer peer.Close()
	fixture := newFixture(t, OptionsV1{Name: "partial-replay", Mode: HTTP1Only}, 1)
	factoryFailure := errors.New("review partial replay factory")
	cleanupFailure := errors.New("review partial replay cleanup")
	var firstCalls, secondCalls atomic.Int64
	var mu sync.Mutex
	var acquired []*reviewMultipartInput
	makeInput := func(cleanup error) *reviewMultipartInput {
		input := &reviewMultipartInput{Reader: strings.NewReader("part"), closeErr: cleanup}
		mu.Lock()
		acquired = append(acquired, input)
		mu.Unlock()
		return input
	}
	body := &Multipart{Parts: []Part{
		{Name: "first", FileName: "first", Open: func(context.Context) (io.ReadCloser, error) {
			if firstCalls.Add(1) == 2 {
				return makeInput(cleanupFailure), nil
			}
			return makeInput(nil), nil
		}},
		{Name: "second", FileName: "second", Open: func(context.Context) (io.ReadCloser, error) {
			if secondCalls.Add(1) == 2 {
				return makeInput(nil), factoryFailure
			}
			return makeInput(nil), nil
		}},
	}}
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "partial-replay"}, multipartRequest(t, peer.URL), RequestOptionsV1{Multipart: body})
	result := settle(t, fixture, receipt)
	if !errors.Is(err, factoryFailure) || !errors.Is(result.Outcome.Primary, factoryFailure) || !errors.Is(result.Outcome.Cleanup, cleanupFailure) ||
		calls.Load() != 1 || result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().StatusCode() != 307 || firstCalls.Load() != 2 || secondCalls.Load() != 2 {
		t.Fatal("partial factory failure dispatched replay or lost independent phases", err, result.Err(), calls.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(acquired) != 4 {
		t.Fatal("partial acquisition was not exercised", len(acquired))
	}
	for _, input := range acquired {
		if input.closed.Load() != 1 {
			t.Fatal("part acquired before/with failure did not close exactly once", input.closed.Load())
		}
	}
}

func TestMultipartLateFactoryReaderAndErrorStayOwned(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) { _, _ = io.Copy(io.Discard, request.Body) }))
	defer func() { peer.CloseClientConnections(); peer.Close() }()
	fixture := newFixture(t, OptionsV1{Name: "late-factory", Mode: HTTP1Only}, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	factoryFailure := errors.New("review late factory")
	cleanupFailure := errors.New("review late input close")
	input := &reviewMultipartInput{Reader: strings.NewReader("late"), closeErr: cleanupFailure}
	body := &Multipart{Parts: []Part{{Name: "late", FileName: "late", Open: func(ctx context.Context) (io.ReadCloser, error) {
		close(entered)
		<-release
		if ctx.Err() == nil {
			return input, errors.New("factory context was not canceled")
		}
		return input, factoryFailure
	}}}}
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, _ := fixture.client.Do(ctx, context.Background(), fault.Correlation{Call: "late-factory"}, multipartRequest(t, peer.URL), RequestOptionsV1{Multipart: body})
		done <- receipt
	}()
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("factory did not run")
	}
	cancel()
	var receipt *invocation.Receipt[Result]
	select {
	case receipt = <-done:
	case <-testContext(t).Done():
		t.Fatal("canceled caller did not get its retained receipt")
	}
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err := receipt.WaitReleased(short); !errors.Is(err, context.DeadlineExceeded) || input.closed.Load() != 0 {
		t.Fatal("late factory returned ownership before acquisition finished", err)
	}
	unblock()
	result := settle(t, fixture, receipt)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Outcome.Cleanup, cleanupFailure) || input.closed.Load() != 1 || result.Outcome.Value.Complete() {
		t.Fatal("late reader/error lost cancellation or cleanup ownership", result.Err())
	}
	found := false
	for _, notice := range result.Outcome.Value.InputErrorsCopy() {
		found = found || errors.Is(notice, factoryFailure)
	}
	if !found {
		t.Fatal("late factory error lost from input observations")
	}
}

type reviewBlockedMultipartClose struct {
	readEntered, readStopped, closeEntered, closeAllowed chan struct{}
	readOnce, closeOnce                                  sync.Once
	closes                                               atomic.Int64
}

func (input *reviewBlockedMultipartClose) Read([]byte) (int, error) {
	input.readOnce.Do(func() { close(input.readEntered) })
	<-input.readStopped
	return 0, context.Canceled
}
func (input *reviewBlockedMultipartClose) Close() error {
	input.closeOnce.Do(func() { input.closes.Add(1); close(input.readStopped); close(input.closeEntered) })
	<-input.closeAllowed
	return nil
}

func TestMultipartCleanupWaitDoesNotReleaseActualClose(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) { _, _ = io.Copy(io.Discard, request.Body) }))
	defer func() { peer.CloseClientConnections(); peer.Close() }()
	fixture := newFixture(t, OptionsV1{Name: "blocked-close", Mode: HTTP1Only}, 1)
	input := &reviewBlockedMultipartClose{readEntered: make(chan struct{}), readStopped: make(chan struct{}), closeEntered: make(chan struct{}), closeAllowed: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(input.closeAllowed) })
	defer unblock()
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, _ := fixture.client.Do(ctx, context.Background(), fault.Correlation{Call: "blocked-close"}, multipartRequest(t, peer.URL),
			RequestOptionsV1{Multipart: &Multipart{Parts: []Part{{Name: "blocked", FileName: "blocked", Input: input}}}})
		done <- receipt
	}()
	select {
	case <-input.readEntered:
	case <-testContext(t).Done():
		t.Fatal("input read was not entered")
	}
	cancel()
	var receipt *invocation.Receipt[Result]
	select {
	case receipt = <-done:
	case <-testContext(t).Done():
		t.Fatal("caller cancellation did not return a receipt")
	}
	select {
	case <-input.closeEntered:
	case <-testContext(t).Done():
		t.Fatal("cleanup did not enter the input Close")
	}
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err := receipt.WaitReleased(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked Close released work", err)
	}
	if err := fixture.assembly.Close(short); err == nil || fixture.assembly.Snapshot().Sources[0].Released {
		t.Fatal("source released while actual input Close remained live", err)
	}
	unblock()
	result := settle(t, fixture, receipt)
	if !errors.Is(result.Err(), context.Canceled) || input.closes.Load() != 1 || result.Outcome.Value.Complete() {
		t.Fatal("cleanup continuation changed facts", result.Err(), input.closes.Load())
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil || !fixture.assembly.Snapshot().Sources[0].Released {
		t.Fatal("continued source cleanup failed", err)
	}
}

func TestMultipartMiddlewareCannotChangeBoundary(t *testing.T) {
	var requests, factories atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer peer.Close()
	options := OptionsV1{Name: "boundary-header", Mode: HTTP1Only}
	options.Native.RequestMiddleware = []func(*sdk.Request) error{func(request *sdk.Request) error {
		request.GetRequest().Header.Set("Content-Type", "multipart/form-data; boundary=not-the-native-boundary")
		return nil
	}}
	fixture := newFixture(t, options, 1)
	body := &Multipart{Parts: []Part{{Name: "unused", FileName: "unused", Open: func(context.Context) (io.ReadCloser, error) {
		factories.Add(1)
		return io.NopCloser(strings.NewReader("unused")), nil
	}}}}
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "boundary-header"}, multipartRequest(t, peer.URL), RequestOptionsV1{Multipart: body})
	result := settle(t, fixture, receipt)
	if err == nil || result.Outcome.Value.Complete() || requests.Load() != 0 || factories.Load() != 0 {
		t.Fatal("changed native boundary reached input or peer", err, factories.Load(), requests.Load())
	}
}
