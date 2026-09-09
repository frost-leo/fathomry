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

package conformance_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/operation"
	"github.com/frost-leo/fathomry/source"
)

// These integrations are test doubles, not SDK or real-service support.
type transfer struct {
	Bytes   int
	Unknown int
	Digest  [32]byte
	Private string
}

type nativeFailure struct{}

func (*nativeFailure) Error() string { return "secret-canary-native-error" }

type counters struct {
	users     atomic.Int64
	bytes     atomic.Int64
	peakBytes atomic.Int64
	releases  atomic.Int64
}

func (counts *counters) enter(bytes int64) func() {
	counts.users.Add(1)
	current := counts.bytes.Add(bytes)
	for peak := counts.peakBytes.Load(); current > peak && !counts.peakBytes.CompareAndSwap(peak, current); peak = counts.peakBytes.Load() {
	}
	return func() { counts.bytes.Add(-bytes); counts.users.Add(-1) }
}

type fixture struct {
	owner    *source.Assembly
	access   *source.Access
	inbox    *operation.Inbox[transfer]
	counts   *counters
	limits   source.Limits
	capacity int
}

func newFixture(t testing.TB, active int, bytes int64, count int) fixture {
	t.Helper()
	type config struct {
		Secret string `json:"secret"`
		Limit  int64  `json:"limit"`
	}
	prepared, err := source.Prepare(source.Schema[config]{Format: 1, Defaults: config{Secret: "secret-canary-config", Limit: bytes}},
		source.Input{Identity: source.Identity{Provider: "fixture.transfer", Name: "data"}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	counts := new(counters)
	limits := source.Limits{Active: active, Queued: active, Bytes: bytes, QueuedBytes: bytes, MaxLeases: 4}
	selected := source.WithLimits(source.Select(prepared, func(_ context.Context, config config) (source.Resource[struct{}], error) {
		if config.Limit != bytes || config.Secret != "secret-canary-config" {
			t.Error("effective defaults changed")
		}
		return source.Resource[struct{}]{Acquired: true, Capability: struct{}{}, Release: func(context.Context) source.ReleaseResult {
			counts.releases.Add(1)
			if counts.users.Load() != 0 {
				t.Error("conformance: owner cleanup ran while native users remained")
			}
			return source.ReleaseResult{Quiescent: true, Released: true}
		}}, nil
	}), limits)
	owner, err := source.Assemble(context.Background(), context.Background(), "integration", selected)
	if err != nil {
		t.Fatal(err)
	}
	access, err := source.AccessFor(owner, selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := operation.NewInbox[transfer](count, int64(count)*128)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(context.Background()); err != nil {
			t.Error("fixture cleanup remained incomplete")
		}
		if counts.users.Load() != 0 || counts.bytes.Load() != 0 {
			t.Error("native fixture leaked use")
		}
		if inbox.Usage() != (operation.InboxUsage{}) {
			t.Error("fixture leaked evidence")
		}
	})
	return fixture{owner: owner, access: access, inbox: inbox, counts: counts, limits: limits, capacity: count}
}

func request(id string, shape operation.Shape, bytes int64) operation.Request {
	return operation.Request{Name: "transfer", Execution: failure.Execution{Call: id, Run: "run", Item: id, Owner: "account"},
		Shape: shape, Bytes: bytes, EvidenceBytes: 128, Admission: operation.Budget{Limit: time.Second},
		AttemptsKnown: true, MaxAttempts: 1}
}

func (fixture fixture) begin(t testing.TB, input operation.Request) *operation.Call[transfer] {
	t.Helper()
	call, err := operation.Begin(context.Background(), fixture.access, input, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	return call
}

func (fixture fixture) expected(input operation.Request, value transfer) conformance.Expected[transfer] {
	return conformance.Expected[transfer]{
		Attribution: failure.Attribution{Operation: "transfer", Provider: "fixture.transfer", Source: "data", Assembly: "integration", Execution: input.Execution},
		Source:      fixture.access.Info(), Limits: fixture.limits, Shape: input.Shape,
		Present: true, Final: true, Released: true, Attempts: operation.Attempts{Exact: true, Observed: 1},
		Value: func(t testing.TB, got transfer) {
			t.Helper()
			if got != value {
				t.Error("conformance: partial/unknown output differs from independent input oracle")
			}
		},
	}
}

func receive(t testing.TB, fixture fixture, expected ...conformance.Expected[transfer]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conformance.Receive(t, ctx, fixture.inbox, expected)
}

type pullFacade struct {
	read  func([]byte) (int, error)
	close func() error
}

func (stream *pullFacade) Read(buffer []byte) (int, error) { return stream.read(buffer) }
func (stream *pullFacade) Close() error                    { return stream.close() }

type owningReader struct{ *pullFacade }

func (*owningReader) Shutdown() {}

type exposedReader struct {
	*pullFacade
	Owner func()
}

type pointerOwner struct{ *pullFacade }

func (*pointerOwner) Shutdown() {}

// openPull is a small integration: data consumption and local stream close belong
// to the caller, while source ownership and independent evidence do not. This
// fixture's Read/Close methods are single-goroutine only, not concurrently safe.
func openPull(t testing.TB, fixture fixture, input operation.Request, payload []byte, fault string) (io.ReadCloser, *operation.Receipt[transfer]) {
	t.Helper()
	call := fixture.begin(t, input)
	done := fixture.counts.enter(int64(len(payload)))
	if _, err := call.Attempt(); err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(payload)
	var consumed int
	var resolved, closed bool
	outcome := func() operation.Outcome[transfer] {
		value := transfer{Bytes: consumed, Unknown: 1, Digest: sha256.Sum256(payload[:consumed]), Private: "secret-canary-payload"}
		if fault == "result" {
			value.Unknown = 0
		}
		return operation.Outcome[transfer]{Present: fault != "empty", Value: value, Primary: io.ErrUnexpectedEOF}
	}
	stream := &pullFacade{read: func(buffer []byte) (int, error) {
		if closed {
			return 0, io.ErrClosedPipe
		}
		count, err := reader.Read(buffer)
		consumed += count
		if err == io.EOF && !resolved {
			resolved = true
			call.Resolve(outcome())
			return count, io.ErrUnexpectedEOF
		}
		return count, err
	}, close: func() error {
		if !closed {
			closed = true
			if !resolved {
				resolved = true
				call.Resolve(outcome())
			}
			call.Finish(nil)
			done()
			call.Release()
		}
		return nil
	}}
	if fault == "ownership" {
		return &owningReader{stream}, call.Receipt()
	}
	if fault == "ownership-field" {
		return &exposedReader{stream, func() { _ = fixture.owner.Close(context.Background()) }}, call.Receipt()
	}
	if fault == "ownership-pointer" {
		return pointerOwner{stream}, call.Receipt()
	}
	return stream, call.Receipt()
}

func runPull(t *testing.T, fault string) {
	for _, size := range []int{0, 1, 4096, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			f := newFixture(t, 1, int64(max(size, 1)), 1)
			payload := bytes.Repeat([]byte("x"), size)
			input := request("pull", operation.Stream, int64(size))
			stream, receipt := openPull(t, f, input, payload, fault)
			conformance.Facade(t, stream, "Read", "Close")
			buffer := make([]byte, 31)
			read := 0
			for {
				count, err := stream.Read(buffer)
				read += count
				if err != nil {
					if !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Error("wrong partial read fault")
					}
					break
				}
			}
			if read != size {
				t.Error("pull data lost")
			}
			expected := f.expected(input, transfer{Bytes: size, Unknown: 1, Digest: sha256.Sum256(payload), Private: "secret-canary-payload"})
			expected.Final, expected.Released, expected.Primary = false, false, io.ErrUnexpectedEOF
			early, ready := receipt.Result()
			if !ready {
				t.Fatal("partial output missing")
			}
			conformance.Result(t, early, expected)
			conformance.Runtime(t, early, new(operation.Result[transfer]), "secret-canary")
			if !errors.Is(f.owner.Close(context.Background()), source.ErrIncomplete) || f.counts.releases.Load() != 0 {
				t.Error("conformance: pull result released its source before stream close")
			}
			if err := stream.Close(); err != nil {
				t.Error(err)
			}
			if err := stream.Close(); err != nil {
				t.Error(err)
			}
			expected.Final, expected.Released = true, true
			receive(t, f, expected)
		})
	}
}

func TestPullIntegration(t *testing.T) { runPull(t, "") }

func runAsync(t *testing.T, fault string) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, 1, 32, 1)
		input := request("async", operation.Async, 32)
		call := f.begin(t, input)
		if _, err := call.Attempt(); err != nil {
			t.Fatal(err)
		}
		native := new(nativeFailure)
		value := transfer{Bytes: 3, Unknown: 1, Private: "secret-canary-payload"}
		callbackRan, leaveSubmit, submitted := make(chan struct{}), make(chan struct{}), make(chan struct{})
		go func() {
			defer close(submitted)
			var guard *operation.Guard
			if fault != "completion" {
				var err error
				guard, err = call.Scope().Hold()
				if err != nil {
					t.Error(err)
					return
				}
			}
			done := f.counts.enter(32)
			outcome := operation.Outcome[transfer]{Present: true, Value: value, Primary: native}
			if fault == "identity" {
				outcome.Primary = errors.New("different-identity")
			}
			call.Complete(outcome)
			close(callbackRan)
			<-leaveSubmit
			done()
			guard.End()
		}()
		<-callbackRan
		expected := f.expected(input, value)
		expected.Primary, expected.Released = native, false
		result, ready := call.Receipt().Result()
		if !ready {
			t.Fatal("early native callback result missing")
		}
		conformance.Result(t, result, expected)
		conformance.Cause(t, result.Err(), func(got *nativeFailure) bool { return got == native })
		if f.counts.users.Load() != 1 {
			t.Fatal("native submit oracle is not active")
		}
		if !errors.Is(f.owner.Close(context.Background()), source.ErrIncomplete) {
			t.Error("conformance: callback return was mistaken for submission return")
		}
		var diagnostic any = result
		if fault == "privacy" {
			diagnostic = struct {
				Cause   error
				Payload string
			}{native, value.Private}
		}
		conformance.Private(t, diagnostic, "secret-canary")
		if call.Complete(operation.Outcome[transfer]{Present: true}) {
			t.Error("duplicate completion overwrote evidence")
		}
		close(leaveSubmit)
		<-submitted
		expected.Released = true
		receive(t, f, expected)
	})
}

func TestAsyncIntegration(t *testing.T) { runAsync(t, "") }

func TestBackgroundSessionRequiresStopAndJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, 1, 4096, 2)
		input := request("session", operation.Session, 4096)
		parent := f.begin(t, input)
		if _, err := parent.Attempt(); err != nil {
			t.Fatal(err)
		}
		delivery, err := f.inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		establish, finishEstablish, err := (operation.Budget{Limit: time.Second}).Context(context.Background(), operation.Establish)
		if err != nil || establish.Err() != nil {
			t.Fatal("establish budget")
		}
		owner, stopOwner := context.WithCancel(context.Background())
		defer stopOwner()
		lifetime, finishLifetime, err := (operation.Budget{}).Context(owner, operation.Lifetime)
		if err != nil {
			t.Fatal(err)
		}
		defer finishLifetime()
		message, proceed, stopped, joined := make(chan int, 1), make(chan struct{}), make(chan struct{}), make(chan struct{})
		go func() {
			defer close(joined)
			buffer := bytes.Repeat([]byte("x"), 4096)
			done := f.counts.enter(int64(len(buffer)))
			defer done()
			for count := 0; ; count++ {
				select {
				case <-lifetime.Done():
					close(stopped)
					<-proceed
					return
				case message <- int(buffer[count%len(buffer)]):
				}
			}
		}()
		finishEstablish()
		for count := range 32 {
			if <-message != int('x') {
				t.Fatal("background buffer content changed")
			}
			nested := request(fmt.Sprintf("message-%d", count), operation.Finite, 0)
			nested.Execution.Parent = "session"
			child, err := operation.BeginNested(lifetime, parent.Scope(), nested, f.inbox, nil)
			if err != nil {
				t.Fatal("nested work competed for its held permit", err)
			}
			value := transfer{Bytes: count, Unknown: 1}
			if err := child.Execute(lifetime, operation.Budget{Limit: time.Second}, func(context.Context, operation.Scope) operation.Outcome[transfer] {
				_, _ = child.Attempt()
				return operation.Outcome[transfer]{Present: true, Value: value}
			}); err != nil {
				t.Fatal(err)
			}
			conformance.Accounting(t, f.owner.Snapshot().Sources[0].Usage, f.limits, f.inbox.Usage(), 2, 256)
			expected := f.expected(nested, value)
			expected.Nested = true
			receive(t, f, expected)
		}
		wait, endWait := context.WithCancel(context.Background())
		endWait()
		if _, err := parent.Receipt().Wait(wait); !errors.Is(err, context.Canceled) {
			t.Fatal("caller wait did not end")
		}
		if lifetime.Err() != nil {
			t.Fatal("caller wait ended session lifetime")
		}
		stopOwner()
		<-stopped
		cleanup := errors.New("native-stop-did-not-yet-join")
		value := transfer{Unknown: 1}
		parent.Resolve(operation.Outcome[transfer]{Present: true, Value: value, Primary: context.Canceled})
		parent.Finish(cleanup)
		expected := f.expected(input, value)
		expected.Primary, expected.Cleanup, expected.Released = context.Canceled, cleanup, false
		result, _ := parent.Receipt().Result()
		conformance.Result(t, result, expected)
		if !errors.Is(delivery.Release(), operation.ErrPending) || f.counts.users.Load() != 1 {
			t.Fatal("stop request or final cleanup report released a live worker")
		}
		if !errors.Is(f.owner.Close(context.Background()), source.ErrIncomplete) {
			t.Fatal("live session did not pin owner")
		}
		close(proceed)
		<-joined
		if !parent.Release() {
			t.Fatal("join confirmation rejected")
		}
		expected.Released = true
		final, err := delivery.Receipt().WaitReleased(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		conformance.Result(t, final, expected)
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBoundedPayloadConcurrencyQueueAndOutstandingEvidence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, size := range []int{0, 1, 4096, 65536} {
			for _, concurrency := range []int{1, 4} {
				f := newFixture(t, concurrency, int64(max(size, 1)*concurrency), concurrency+1)
				var expected []conformance.Expected[transfer]
				unblock, entered := make(chan struct{}), make(chan struct{}, concurrency)
				var workers sync.WaitGroup
				for index := range concurrency {
					input := request(fmt.Sprintf("work-%d", index), operation.Finite, int64(size))
					call := f.begin(t, input)
					value := transfer{Bytes: size, Digest: sha256.Sum256(bytes.Repeat([]byte("x"), size))}
					expected = append(expected, f.expected(input, value))
					workers.Go(func() {
						if err := call.Execute(context.Background(), operation.Budget{Limit: 10 * time.Second}, func(context.Context, operation.Scope) operation.Outcome[transfer] {
							done := f.counts.enter(int64(size))
							defer done()
							payload := bytes.Repeat([]byte("x"), size)
							_, _ = call.Attempt()
							entered <- struct{}{}
							<-unblock
							return operation.Outcome[transfer]{Present: true, Value: transfer{Bytes: len(payload), Digest: sha256.Sum256(payload)}}
						}); err != nil {
							t.Error(err)
						}
					})
				}
				for range concurrency {
					<-entered
				}
				queued := make(chan error, 1)
				go func() {
					call, err := operation.Begin(context.Background(), f.access, request("expired", operation.Finite, int64(size)), f.inbox, nil)
					if call != nil {
						call.Complete(operation.Outcome[transfer]{})
						t.Error("conformance: expired queued call entered native work")
					}
					queued <- err
				}()
				synctest.Wait()
				conformance.Accounting(t, f.owner.Snapshot().Sources[0].Usage, f.limits, f.inbox.Usage(), concurrency+1, int64(concurrency+1)*128)
				time.Sleep(2 * time.Second)
				if err := <-queued; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("queued admission did not expire")
				}
				close(unblock)
				workers.Wait()
				if f.inbox.Usage().Outstanding != concurrency {
					t.Fatal("completed callbacks discarded pending evidence")
				}
				if f.counts.peakBytes.Load() != int64(size*concurrency) || f.counts.users.Load() != 0 {
					t.Fatal("native fixture allocation/use bounds differ from oracle")
				}
				receive(t, f, expected...)
				if f.owner.Snapshot().Sources[0].Usage != (source.Usage{}) || f.inbox.Usage() != (operation.InboxUsage{}) {
					t.Fatal("workload retained responsibility")
				}
			}
		}
	})
}

func TestBrokenIntegrationsFailRealContractTests(t *testing.T) {
	for _, fault := range []string{"ownership", "ownership-field", "ownership-pointer", "completion", "result", "privacy", "identity", "empty"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestContractMutationChild$", "-test.timeout=20s")
			command.Env = append(os.Environ(), "FATHOMRY_CONFORMANCE_FAULT="+fault)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || ctx.Err() != nil || !bytes.Contains(output, []byte("conformance:")) || !bytes.Contains(output, []byte("--- FAIL:")) {
				t.Fatalf("fault %s did not trigger the real testing contract", fault)
			}
			if bytes.Contains(output, []byte("secret-canary")) {
				t.Fatal("mutation test diagnostic leaked the injected secret")
			}
		})
	}
}

func TestContractMutationChild(t *testing.T) {
	fault := os.Getenv("FATHOMRY_CONFORMANCE_FAULT")
	switch fault {
	case "ownership", "ownership-field", "ownership-pointer", "result", "empty":
		runPull(t, fault)
	case "completion", "privacy", "identity":
		runAsync(t, fault)
	case "":
		return
	default:
		t.Fatal("unknown mutation")
	}
}
