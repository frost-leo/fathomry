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
	"mime"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/enetx/g"
	nativehttp "github.com/enetx/http"
	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	utls "github.com/refraction-networking/utls"
)

type multipartReader struct {
	io.Reader
	closed atomic.Int64
}

func (reader *multipartReader) Close() error { reader.closed.Add(1); return nil }

type gatedMultipartReader struct {
	prefix    *strings.Reader
	tail      *strings.Reader
	allow     chan struct{}
	stopped   chan struct{}
	once      sync.Once
	generated atomic.Bool
	closed    atomic.Int64
}

func (reader *gatedMultipartReader) Read(buffer []byte) (int, error) {
	if reader.prefix.Len() > 0 {
		return reader.prefix.Read(buffer)
	}
	select {
	case <-reader.allow:
		reader.generated.Store(true)
		return reader.tail.Read(buffer)
	case <-reader.stopped:
		return 0, context.Canceled
	}
}
func (reader *gatedMultipartReader) Close() error {
	reader.once.Do(func() { reader.closed.Add(1); close(reader.stopped) })
	return nil
}
func multipartRequest(t *testing.T, endpoint string) *nativehttp.Request {
	t.Helper()
	value, err := nativehttp.NewRequest("POST", endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func multipartOptions(name string, mode ProtocolMode, boundary *atomic.Int64) OptionsV1 {
	profile := chrome.Desktop
	profile.HelloSpec = nil
	profile.HelloID = utls.ClientHelloID{}
	profile.ShuffleExtensions = false
	profile.Boundary = func() g.String { boundary.Add(1); return "owned-fixture-boundary" }
	return OptionsV1{Name: name, Mode: mode, Native: NativeOptionsV1{Profile: &profile}}
}
func TestMultipartIncrementalNativeOrderAndContext(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1Only, H2C} {
		t.Run(string(mode), func(t *testing.T) {
			input := &gatedMultipartReader{prefix: strings.NewReader("prefix"), tail: strings.NewReader("tail"), allow: make(chan struct{}), stopped: make(chan struct{})}
			defer input.Close()
			allow := sync.OnceFunc(func() { close(input.allow) })
			defer allow()
			other := &multipartReader{Reader: strings.NewReader("second")}
			var boundary atomic.Int64
			seen := make(chan []string, 1)
			peer := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				kind, parameters, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
				if err != nil || kind != "multipart/form-data" || parameters["boundary"] != "owned-fixture-boundary" {
					t.Error("native boundary changed", err)
					return
				}
				reader, err := request.MultipartReader()
				if err != nil {
					t.Error(err)
					return
				}
				var parts []string
				for {
					part, err := reader.NextPart()
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Error(err)
						return
					}
					var data []byte
					if part.FormName() == "first-file" {
						prefix := make([]byte, 6)
						if _, err := io.ReadFull(part, prefix); err != nil || string(prefix) != "prefix" || input.generated.Load() {
							t.Error("whole input generated before peer prefix", err)
							return
						}
						allow()
						tail, err := io.ReadAll(part)
						if err != nil {
							t.Error(err)
							return
						}
						data = append(prefix, tail...)
					} else {
						data, err = io.ReadAll(part)
						if err != nil {
							t.Error(err)
							return
						}
					}
					parts = append(parts, part.FormName()+":"+part.FileName()+":"+string(data))
				}
				seen <- parts
				_, _ = io.WriteString(writer, request.Proto)
			}))
			if mode == H2C {
				protocols := new(http.Protocols)
				protocols.SetUnencryptedHTTP2(true)
				peer.Config.Protocols = protocols
			}
			peer.Start()
			defer peer.Close()
			fixture := newFixture(t, multipartOptions("incremental", mode, &boundary), 1)
			inputRequest := multipartRequest(t, peer.URL)
			old, cancelOld := context.WithCancel(context.Background())
			cancelOld()
			inputRequest = inputRequest.WithContext(old)
			inputRequest.Header.Set("Content-Type", "application/ignored-native-multipart-override")
			body := &Multipart{Fields: []Field{{Name: "a", Value: "one"}, {Name: "repeat", Value: "old"}, {Name: "b", Value: "two"}, {Name: "repeat", Value: "new"}},
				Parts: []Part{{Name: "first-file", FileName: "first.txt", Input: input}, {Name: "second-file", FileName: "second.bin", Input: other}}}
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "incremental"}, inputRequest, RequestOptionsV1{Multipart: body})
			if err != nil {
				t.Fatal(err)
			}
			result := settle(t, fixture, receipt)
			want := []string{"a::one", "repeat::new", "b::two", "first-file:first.txt:prefixtail", "second-file:second.bin:second"}
			select {
			case got := <-seen:
				if !reflect.DeepEqual(got, want) {
					t.Fatal("native order/bytes changed", got)
				}
			case <-testContext(t).Done():
				t.Fatal("peer receipt absent")
			}
			protocol := "HTTP/1.1"
			if mode == H2C {
				protocol = "HTTP/2.0"
			}
			if !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != protocol || boundary.Load() != 1 || input.closed.Load() != 1 || other.closed.Load() != 1 {
				t.Fatal("multipart ownership, boundary or protocol changed", result.Err())
			}
		})
	}
}
func TestMultipartReplayAndRetainedRedirect(t *testing.T) {
	for _, mode := range []string{"one-shot", "redirect", "status", "decline", "factory-error", "alias"} {
		t.Run(mode, func(t *testing.T) {
			var requests, factories, boundaries atomic.Int64
			var mu sync.Mutex
			var bodies []string
			var inputs []*multipartReader
			failure := errors.New("synthetic replay factory failure")
			peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				data, err := io.ReadAll(request.Body)
				if err != nil {
					t.Error(err)
				}
				mu.Lock()
				bodies = append(bodies, string(data))
				mu.Unlock()
				if requests.Add(1) == 1 {
					if mode == "status" {
						writer.WriteHeader(503)
						_, _ = io.WriteString(writer, "retry")
						return
					}
					writer.Header().Set("Location", "/next")
					writer.WriteHeader(307)
					_, _ = io.WriteString(writer, "redirect")
					return
				}
				_, _ = io.WriteString(writer, "complete")
			}))
			defer peer.Close()
			options := multipartOptions(mode, HTTP1Only, &boundaries)
			if mode == "status" {
				options.NativeRetries = 1
				options.RetryCodes = []int{503}
			}
			if mode == "decline" {
				options.Native.CheckRedirect = func(*nativehttp.Request, []*nativehttp.Request) error { return nativehttp.ErrUseLastResponse }
			}
			fixture := newFixture(t, options, 1)
			body := &Multipart{Fields: []Field{{Name: "field", Value: "static"}}, Parts: []Part{{Name: "file", FileName: "part.txt"}}}
			if mode == "one-shot" {
				input := &multipartReader{Reader: strings.NewReader("payload")}
				inputs = append(inputs, input)
				body.Parts[0].Input = input
			} else {
				body.Parts[0].Open = func(ctx context.Context) (io.ReadCloser, error) {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					call := factories.Add(1)
					if mode == "alias" && call == 2 {
						return inputs[0], nil
					}
					input := &multipartReader{Reader: strings.NewReader("payload")}
					inputs = append(inputs, input)
					if mode == "factory-error" && call == 2 {
						return input, failure
					}
					return input, nil
				}
			}
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: mode}, multipartRequest(t, peer.URL), RequestOptionsV1{Multipart: body})
			result := settle(t, fixture, receipt)
			value := result.Outcome.Value
			switch mode {
			case "factory-error":
				if !errors.Is(err, failure) || requests.Load() != 1 || value.Metadata().StatusCode() != 307 || value.Complete() {
					t.Fatal("unsafe failed replay sent or lost first response", err)
				}
			case "alias":
				if !errors.Is(err, ErrInput) || requests.Load() != 1 || value.Complete() {
					t.Fatal("aliased replay sent", err)
				}
			case "one-shot", "decline":
				if err != nil || requests.Load() != 1 || value.Metadata().StatusCode() != 307 || string(value.DataCopy()) != "redirect" || !value.Complete() {
					t.Fatal("retained redirect lost", err)
				}
			default:
				if err != nil || requests.Load() != 2 || !value.Complete() || value.Metadata().StatusCode() != 200 {
					t.Fatal("replay failed", err)
				}
				mu.Lock()
				same := len(bodies) == 2 && bodies[0] == bodies[1] && strings.Contains(bodies[0], "payload")
				mu.Unlock()
				if !same {
					t.Fatal("multipart replay bytes/boundary changed")
				}
			}
			if boundaries.Load() != 1 {
				t.Fatal("logical upload reselected boundary")
			}
			for _, input := range inputs {
				if input.closed.Load() != 1 {
					t.Fatal("input not closed exactly once", input.closed.Load())
				}
			}
			if mode == "decline" && factories.Load() != 2 {
				t.Fatal("unused native redirect replay was not exercised")
			}
		})
	}
}
func TestMultipartStaticRetryRefusalDoesNotBorrow(t *testing.T) {
	var requests atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer peer.Close()
	fixture := newFixture(t, OptionsV1{Name: "static", NativeRetries: 1}, 1)
	input := &multipartReader{Reader: strings.NewReader("payload")}
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "static"}, multipartRequest(t, peer.URL), RequestOptionsV1{Multipart: &Multipart{Parts: []Part{{Name: "file", FileName: "part", Input: input}}}})
	if receipt != nil || !errors.Is(err, ErrInput) || requests.Load() != 0 || input.closed.Load() != 0 {
		t.Fatal("static incompatible retry acquired input or network", err)
	}
}

type blockedMultipartReader struct {
	entered, closed     chan struct{}
	readOnce, closeOnce sync.Once
	closes              atomic.Int64
}

func (reader *blockedMultipartReader) Read([]byte) (int, error) {
	reader.readOnce.Do(func() { close(reader.entered) })
	<-reader.closed
	return 0, context.Canceled
}
func (reader *blockedMultipartReader) Close() error {
	reader.closeOnce.Do(func() { reader.closes.Add(1); close(reader.closed) })
	return nil
}
func TestMultipartCancellationAndEarlyResponseJoinProducer(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "early-response"}[early], func(t *testing.T) {
			stop := make(chan struct{})
			peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if early {
					_ = http.NewResponseController(writer).EnableFullDuplex()
					reader, err := request.MultipartReader()
					if err != nil {
						t.Error(err)
						return
					}
					if _, err := reader.NextPart(); err != nil {
						t.Error(err)
						return
					}
					writer.Header().Set("Content-Length", "4")
					writer.WriteHeader(413)
					_, _ = io.WriteString(writer, "stop")
					return
				}
				_, _ = io.Copy(io.Discard, request.Body)
				<-stop
			}))
			defer func() { close(stop); peer.Close() }()
			fixture := newFixture(t, OptionsV1{Name: "producer", Mode: HTTP1Only}, 1)
			input := &blockedMultipartReader{entered: make(chan struct{}), closed: make(chan struct{})}
			defer input.Close()
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			type completion struct {
				receipt *invocation.Receipt[Result]
				err     error
			}
			done := make(chan completion, 1)
			go func() {
				receipt, err := fixture.client.Do(ctx, testContext(t), fault.Correlation{Call: "producer"}, multipartRequest(t, peer.URL), RequestOptionsV1{Multipart: &Multipart{Parts: []Part{{Name: "file", FileName: "blocked", Input: input}}}})
				done <- completion{receipt, err}
			}()
			select {
			case <-input.entered:
			case <-testContext(t).Done():
				t.Fatal("producer did not enter input")
			}
			if !early {
				cancel()
			}
			var completed completion
			select {
			case completed = <-done:
			case <-testContext(t).Done():
				t.Fatal("managed pipe/input cancellation did not unblock native writer")
			}
			result := settle(t, fixture, completed.receipt)
			if input.closes.Load() != 1 {
				t.Fatal("input did not close")
			}
			if early {
				if completed.err != nil || result.Outcome.Value.Metadata().StatusCode() != 413 || string(result.Outcome.Value.DataCopy()) != "stop" {
					t.Fatal("early response lost", completed.err, result.Err())
				}
			} else if !errors.Is(completed.err, context.Canceled) || result.Outcome.Value.Complete() {
				t.Fatal("cancellation changed", completed.err)
			}
		})
	}
}
