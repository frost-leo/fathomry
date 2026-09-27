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

package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	remote "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	c "github.com/frost-leo/fathomry/framework/configuration/v1"
	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestVariableUnicodeFidelity(t *testing.T) {
	for _, text := range []string{"a\u0085b", "\u0085", "\u007f\u0080\u009f\uffff", "😀", "\ufffd", "a\r\nb\t", "\\ud800"} {
		for _, encoding := range []c.VariableEncoding{c.Text, c.JSON} {
			t.Run(fmt.Sprintf("%x/%d", []byte(text), encoding), func(t *testing.T) {
				input := text
				if encoding == c.JSON {
					data, err := json.Marshal(text)
					if err != nil {
						t.Fatal(err)
					}
					input = string(data)
				}
				t.Setenv("GH96_UNICODE", input)
				loaded, err := c.Load(context.Background(), schema(), c.Plan{Variables: []c.Variable{
					{Name: "GH96_UNICODE", Field: "/project/text", Encoding: encoding},
				}})
				if err != nil {
					t.Fatal("valid variable rejected", err)
				}
				value, err := loaded.ValueCopy()
				if err != nil || value.Project.Text != text {
					t.Fatalf("variable changed: got %q want %q (%v)", value.Project.Text, text, err)
				}
			})
		}
	}
	for _, input := range []string{`"\ud83d\ude00"`, `"\u0000\u0001"`, `"\ufffd"`} {
		t.Run(input, func(t *testing.T) {
			t.Setenv("GH96_UNICODE", input)
			var expected string
			if err := json.Unmarshal([]byte(input), &expected); err != nil {
				t.Fatal(err)
			}
			loaded, err := c.Load(context.Background(), schema(), c.Plan{Variables: []c.Variable{
				{Name: "GH96_UNICODE", Field: "/project/text", Encoding: c.JSON},
			}})
			if err != nil {
				t.Fatal("valid JSON escape rejected", err)
			}
			value, _ := loaded.ValueCopy()
			if value.Project.Text != expected {
				t.Fatal("JSON escape changed")
			}
		})
	}
	t.Setenv("GH96_UNICODE", "{\"a\u0085b\":\"c\u0085d\",\"\\ud83d\\ude00\":\"\\ud83d\\ude00\",\"\":\"empty-key\"}")
	loaded, err := c.Load(context.Background(), schema(), c.Plan{Variables: []c.Variable{
		{Name: "GH96_UNICODE", Field: "/project/labels", Encoding: c.JSON},
	}})
	if err != nil {
		t.Fatal("valid JSON map rejected", err)
	}
	value, _ := loaded.ValueCopy()
	if !reflect.DeepEqual(value.Project.Labels, map[string]string{"default": "kept", "a\u0085b": "c\u0085d", "😀": "😀", "": "empty-key"}) {
		t.Fatal("JSON names/values or common map merger changed")
	}
	for _, input := range []string{`{"a":"first","\u0061":"last"}`, `{"":"first","":"last"}`, `{"a":"\ud800"}`, `{"\udfff":"a"}`, `{"a":"\ud800\u0041"}`} {
		t.Setenv("GH96_UNICODE", input)
		if _, err := c.Load(context.Background(), schema(), c.Plan{Variables: []c.Variable{
			{Name: "GH96_UNICODE", Field: "/project/labels", Encoding: c.JSON},
		}}); !errors.Is(err, c.ErrPlan) {
			t.Fatalf("invalid JSON variable admitted: %s", input)
		}
	}
}

func TestRemoteUnicodeEscapeBoundary(t *testing.T) {
	fixture := newProtocolFixture(t, false)
	selected := fixture.selectSource(t)
	for _, test := range []struct {
		escaped string
		text    string
		valid   bool
	}{
		{`\ud800`, "", false}, {`\udfff`, "", false}, {`\ud800\u0041`, "", false},
		{`\ud83d\ude00`, "😀", true}, {`\ufffd`, "\ufffd", true}, {"\ufffd", "\ufffd", true},
		{`\\ud800`, `\ud800`, true},
	} {
		t.Run(test.escaped, func(t *testing.T) {
			fixture.mu.Lock()
			fixture.malformed = &wire.Payload{Metadata: &wire.Metadata{Type: "ConfigQueryResponse"},
				Body: &anypb.Any{Value: []byte(`{"resultCode":200,"success":true,"content":"format: 1\nproject: {text: '` + test.escaped + `'}"}`)}}
			fixture.mu.Unlock()
			batch, err := selected.Capture(context.Background())
			loaded, loadErr := c.Load(context.Background(), schema(), remotePlan(selected))
			if !test.valid {
				if batch != nil || !errors.Is(err, remote.ErrProtocol) || !errors.Is(loadErr, remote.ErrProtocol) {
					t.Fatal("lossy native content accepted")
				}
				return
			}
			if err != nil || loadErr != nil {
				t.Fatal("valid Unicode refused", err, loadErr)
			}
			raw, presence, err := batch.RawCopy("document")
			if err != nil || presence != source.Present || string(raw) != "format: 1\nproject: {text: '"+test.text+"'}" {
				t.Fatal("raw Unicode changed")
			}
			value, _ := loaded.ValueCopy()
			if value.Project.Text != test.text {
				t.Fatal("accepted text changed")
			}
		})
	}
}

func TestVariableUnicodeWatchCaptureIsFrozen(t *testing.T) {
	for _, remoteSource := range []bool{false, true} {
		t.Run(fmt.Sprint(remoteSource), func(t *testing.T) {
			var input c.Plan
			var update func()
			if remoteSource {
				fixture := newProtocolFixture(t, false)
				input = remotePlan(fixture.selectSource(t))
				update = func() { fixture.set("format: 1\nproject: {count: 42}") }
			} else {
				path := file(t, t.TempDir(), "input", "format: 1")
				input = plan(selected(t, "local", path, time.Second), false)
				update = func() { replace(t, path, "format: 1\nproject: {count: 42}") }
			}
			t.Setenv("GH96_UNICODE_WATCH", `"a\u0085b\ud83d\ude00"`)
			input.Variables = []c.Variable{{Name: "GH96_UNICODE_WATCH", Field: "/project/text", Encoding: c.JSON}}
			live, err := c.Watch(context.Background(), schema(), input)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeOwner(t, live.Close) })
			first := await(t, live, func(state c.State[project]) bool { return state.Status == c.Ready })
			value, err := first.Snapshot.ValueCopy()
			if err != nil || value.Project.Text != "a\u0085b😀" {
				t.Fatal("initial watch variable changed", err)
			}
			t.Setenv("GH96_UNICODE_WATCH", `"changed"`)
			update()
			latest := await(t, live, func(state c.State[project]) bool {
				value, err := state.Snapshot.ValueCopy()
				return err == nil && state.Status == c.Ready && value.Project.Count == 42
			})
			value, _ = latest.Snapshot.ValueCopy()
			if value.Project.Text != "a\u0085b😀" {
				t.Fatal("watch reread or normalized captured variable")
			}
		})
	}
}

func TestRemoteTypedDenialRefreshesUsedToken(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			fixture := newProtocolFixture(t, false)
			fixture.loginToken, fixture.denialCode = "first-token", code
			settings := fixture.settings()
			settings.Username, settings.Password = "reader", "credential-canary"
			selected, err := remote.Select(settings)
			if err != nil {
				t.Fatal(err)
			}
			observer, err := selected.Observe(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeOwner(t, observer.Close) })
			initial := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Available })
			fixture.mu.Lock()
			fixture.loginToken = "renewed-token"
			fixture.values["settings.yaml"] = "format: 1\nproject: {count: 99}"
			fixture.mu.Unlock()
			// No additional push is sent: the source's existing paced recovery owns
			// authentication and full resynchronization after this single revocation.
			recovered := awaitRaw(t, observer, func(state source.State) bool {
				if state.Status != source.Available || state.Generation == initial.Generation {
					return false
				}
				raw, _, _ := state.Batch.RawCopy("document")
				return strings.Contains(string(raw), "99")
			})
			if recovered.Failure != nil || fixture.denials.Load() < 1 || fixture.logins.Load() < 2 {
				t.Fatal("revoked cached token did not recover")
			}
		})
	}
}

func TestRemoteEveryAttemptPreservesRPCContextCause(t *testing.T) {
	for _, code := range []codes.Code{codes.Canceled, codes.DeadlineExceeded} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", code, reverse), func(t *testing.T) {
				first, second := newProtocolFixture(t, false), newProtocolFixture(t, false)
				first.failCode.Store(int32(codes.Unavailable))
				second.failCode.Store(int32(code))
				settings := first.settings()
				settings.Servers = append(settings.Servers, second.settings().Servers[0])
				if reverse {
					settings.Servers[0], settings.Servers[1] = settings.Servers[1], settings.Servers[0]
				}
				selected, err := remote.Select(settings)
				if err != nil {
					t.Fatal(err)
				}
				batch, err := selected.Capture(context.Background())
				expected := context.Canceled
				if code == codes.DeadlineExceeded {
					expected = context.DeadlineExceeded
				}
				if batch != nil || !errors.Is(err, remote.ErrRead) || !errors.Is(err, expected) ||
					first.queries.Load() != 1 || second.queries.Load() != 1 {
					t.Fatal("multi-attempt context evidence lost", err)
				}
				sourcePrivate(t, err)
			})
		}
	}
}

type borrowedCancellation struct {
	hooks atomic.Int32
}

func (cause *borrowedCancellation) Error() string { cause.hooks.Add(1); return "private-canary" }
func (cause *borrowedCancellation) Is(error) bool { cause.hooks.Add(1); return false }
func (cause *borrowedCancellation) As(any) bool   { cause.hooks.Add(1); return false }
func (cause *borrowedCancellation) Unwrap() error { cause.hooks.Add(1); return nil }
func (cause *borrowedCancellation) GRPCStatus() *status.Status {
	cause.hooks.Add(1)
	return status.New(codes.DeadlineExceeded, "private-canary")
}

func TestBorrowedCancellationIsNotAcquisitionEvidence(t *testing.T) {
	fixture := newProtocolFixture(t, false)
	path := file(t, t.TempDir(), "input", "format: 1")
	for name, selection := range map[string]source.Selection{
		"local": selected(t, "local", path, time.Second), "remote": fixture.selectSource(t),
	} {
		t.Run(name, func(t *testing.T) {
			cause := new(borrowedCancellation)
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(cause)
			batch, err := selection.Capture(ctx)
			if batch != nil || err == nil {
				t.Fatal("canceled capture succeeded")
			}
			sourcePrivate(t, err)
			if cause.hooks.Load() != 0 {
				t.Errorf("producer inspected borrowed cause %d times", cause.hooks.Load())
			}
			if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
				t.Fatal("deliberate cause matching lost")
			}
			observer, err := selection.Observe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			closeOwner(t, observer.Close)
			state, err := observer.Current()
			if err != nil || state.Status != source.Closed || cause.hooks.Load() != 0 {
				t.Fatal("canceled observer inspected borrowed cause")
			}
			if !errors.Is(state.Failure, cause) {
				t.Fatal("terminal state lost caller cause")
			}
		})
	}
	if fixture.queries.Load() != 0 || fixture.setups.Load() != 0 {
		t.Fatal("already canceled operation performed network acquisition")
	}
}

func TestInFlightCancellationIsNotNativeEvidence(t *testing.T) {
	for _, observe := range []bool{false, true} {
		t.Run(fmt.Sprint(observe), func(t *testing.T) {
			fixture := newProtocolFixture(t, false)
			fixture.queryBlock = make(chan struct{})
			selection := fixture.selectSource(t)
			cause := new(borrowedCancellation)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			var observer source.Observer
			result := make(chan error, 1)
			if observe {
				var err error
				observer, err = selection.Observe(ctx)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { closeOwner(t, observer.Close) })
			} else {
				go func() {
					batch, err := selection.Capture(ctx)
					if batch != nil {
						t.Error("canceled capture returned data")
					}
					result <- err
				}()
			}
			deadline := time.Now().Add(3 * time.Second)
			for fixture.queries.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if fixture.queries.Load() == 0 {
				t.Fatal("query never began")
			}
			cancel(cause)
			var err error
			if observe {
				closeOwner(t, observer.Close)
				state, problem := observer.Current()
				if problem != nil || state.Status != source.Closed {
					t.Fatal("owner not joined", problem)
				}
				err = state.Failure
			} else {
				select {
				case err = <-result:
				case <-time.After(3 * time.Second):
					t.Fatal("capture did not join")
				}
			}
			if cause.hooks.Load() != 0 {
				t.Errorf("in-flight cause inspected %d times", cause.hooks.Load())
			}
			if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
				t.Fatal("caller cause not retained")
			}
		})
	}
}
