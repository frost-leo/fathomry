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

package nethttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestNativeRedirectReplayAndHeaderExtension(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var requests atomic.Int64
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			requests.Add(1)
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Error(err)
			}
			if string(body) != "payload" || request.Method != "POST" {
				t.Error("native replay changed")
			}
			if request.URL.Path == "/start" {
				writer.Header().Set("Location", "/end")
				writer.WriteHeader(307)
				return
			}
			_, _ = io.WriteString(writer, request.Header.Get("X-Extension"))
		})
		options.Native.CheckRedirect = func(request *http.Request, previous []*http.Request) error {
			if request.Body != nil || request.GetBody != nil || len(previous) != 1 || previous[0].Body != nil {
				t.Error("native owning request body escaped")
			}
			request.Header.Set("X-Extension", "runtime")
			return nil
		}
		f := bindFixture(t, options, 1)
		original := newRequest(t, "POST", server.URL+"/start", strings.NewReader("payload"))
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("redirect"), original)
		if err != nil {
			t.Fatal(err)
		}
		result := settle(t, f, receipt)
		if requests.Load() != 2 || result.Outcome.Value.Exchanges() != 2 || result.Attempts.Exact || string(result.Outcome.Value.DataCopy()) != "runtime" || original.Header.Get("X-Extension") != "" {
			t.Fatal("redirect or snapshot semantics lost")
		}
	})
}

func TestNativeLastResponseAndRedirectRefusal(t *testing.T) {
	for _, last := range []bool{false, true} {
		t.Run(map[bool]string{false: "refusal", true: "last"}[last], func(t *testing.T) {
			var requests atomic.Int64
			server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				writer.Header().Set("Location", "/end")
				writer.WriteHeader(302)
				_, _ = io.WriteString(writer, "redirect")
			})
			cause := errors.New("synthetic policy refusal")
			options.Native.CheckRedirect = func(*http.Request, []*http.Request) error {
				if last {
					return http.ErrUseLastResponse
				}
				return cause
			}
			f := bindFixture(t, options, 1)
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation("policy"), newRequest(t, "GET", server.URL, nil))
			result := settle(t, f, receipt)
			if requests.Load() != 1 {
				t.Fatal("refused redirect reached the peer")
			}
			if last {
				if err != nil || string(result.Outcome.Value.DataCopy()) != "redirect" || !result.Outcome.Value.Complete() {
					t.Fatal("last response was not preserved", err)
				}
			} else {
				var native *url.Error
				if !errors.Is(err, cause) || !errors.As(result.Err(), &native) || !errors.Is(result.Err(), cause) || result.Outcome.Value.Complete() {
					t.Fatal("native refusal identity lost", err)
				}
			}
		})
	}
}

func TestNativeErrorHandoffPrecedesCleanupCancellation(t *testing.T) {
	cause := errors.New("synthetic native proxy refusal")
	options := OptionsV1{Name: "handoff", Native: NativeOptionsV1{Proxy: func(*http.Request) (*url.URL, error) { return nil, cause }}}
	f := bindFixture(t, options, 1)
	for range 100 {
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("handoff"), newRequest(t, "GET", "http://synthetic.invalid/", nil))
		if !errors.Is(err, cause) {
			t.Fatal("cleanup cancellation replaced the known native error", err)
		}
		result := settle(t, f, receipt)
		if !errors.Is(result.Err(), cause) {
			t.Fatal("native cause was lost from independent evidence")
		}
	}
}

func TestNativeJarAndCallerSuppliedCookie(t *testing.T) {
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(writer, request.Header.Get("Cookie"))
	})
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	address, _ := url.Parse(server.URL)
	jar.SetCookies(address, []*http.Cookie{{Name: "synthetic", Value: "jar"}})
	options.Native.Jar = jar
	f := bindFixture(t, options, 1)
	request := newRequest(t, "GET", server.URL, nil)
	request.Header.Set("Cookie", "caller=runtime")
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("cookies"), request)
	if err != nil {
		t.Fatal(err)
	}
	result := settle(t, f, receipt)
	got := string(result.Outcome.Value.DataCopy())
	if !strings.Contains(got, "caller=runtime") || !strings.Contains(got, "synthetic=jar") || request.Header.Get("Cookie") != "caller=runtime" {
		t.Fatal("native jar or caller header isolation lost")
	}
}

func TestUploadLimitRetainsEvidence(t *testing.T) {
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.Copy(io.Discard, request.Body) })
	options.MaxRequestBytes = 3
	f := bindFixture(t, options, 1)
	request := newRequest(t, "POST", server.URL, io.NopCloser(strings.NewReader("oversize")))
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("upload"), request)
	result := settle(t, f, receipt)
	if err == nil || !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.RequestBytesRead() > 4 || result.Outcome.Value.Complete() {
		t.Fatal("upload overrun accepted", err)
	}
}

func TestCanceledDialRetainsActualNativeUse(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	options := OptionsV1{Name: "dial", Timeout: 50 * time.Millisecond}
	options.Native.DialContext = func(context.Context, string, string) (net.Conn, error) {
		close(entered)
		<-release
		first, second := net.Pipe()
		_ = second.Close()
		return first, nil
	}
	f := bindFixture(t, options, 1)
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("dial"), newRequest(t, "GET", "http://synthetic.invalid/", nil))
	if err == nil {
		t.Fatal("canceled dial unexpectedly succeeded")
	}
	<-entered
	wait, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := receipt.WaitReleased(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("late native dial was released")
	}
	if err := f.assembly.Close(deadline(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("pending native dial lost ownership", err)
	}
	unblock()
	result := settle(t, f, receipt)
	if !errors.Is(result.Err(), context.DeadlineExceeded) {
		t.Fatal("native deadline lost", result.Err())
	}
}

type invalidReplayReader struct{ data []byte }

func (reader *invalidReplayReader) Read(data []byte) (int, error) {
	return copy(data, reader.data), io.EOF
}
func (*invalidReplayReader) Close() error { return nil }

func TestMalformedReplayReadersRetainClassifiedEvidence(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var requests atomic.Int64
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			requests.Add(1)
			_, _ = io.Copy(io.Discard, request.Body)
			http.Redirect(writer, request, "/end", http.StatusTemporaryRedirect)
		})
		f := bindFixture(t, options, 1)
		cause := errors.New("synthetic replay failure")
		for index := range 3 {
			request := newRequest(t, "POST", server.URL+"/start", strings.NewReader("payload"))
			request.GetBody = func() (io.ReadCloser, error) {
				if index == 0 {
					return nil, nil
				}
				var reader *invalidReplayReader
				if index == 2 {
					return reader, cause
				}
				return reader, nil
			}
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation("invalid-replay"), request)
			result := settle(t, f, receipt)
			if !errors.Is(err, ErrInput) || !errors.Is(result.Err(), ErrInput) || result.Outcome.Value.Complete() || requests.Load() != int64(index+1) {
				t.Fatal("malformed replay was submitted or lost classified evidence", err)
			}
			if index == 2 && (!errors.Is(err, cause) || !errors.Is(result.Err(), cause)) {
				t.Fatal("native replay failure identity was discarded")
			}
		}
	})
}

func TestRequestMetadataBoundsApplyBeforeNativeEntryAndAfterRedirect(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var requests atomic.Int64
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			requests.Add(1)
			http.Redirect(writer, request, "/end", http.StatusFound)
		})
		options.MaxHeaderBytes = 1024
		options.Native.CheckRedirect = func(request *http.Request, _ []*http.Request) error {
			request.Method = strings.Repeat("M", 512<<10)
			return nil
		}
		f := bindFixture(t, options, 1)
		for _, field := range []string{"method", "host", "transfer-encoding"} {
			request := newRequest(t, "GET", server.URL, nil)
			switch field {
			case "method":
				request.Method = strings.Repeat("M", 512<<10)
			case "host":
				request.Host = strings.Repeat("h", 2048)
			case "transfer-encoding":
				request.TransferEncoding = []string{strings.Repeat("x", 2048)}
			}
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation("metadata"), request)
			if receipt != nil || !errors.Is(err, ErrLimit) || requests.Load() != 0 || f.inbox.Usage().Outstanding != 0 {
				t.Fatal("oversized metadata reached native admission", field, err)
			}
		}
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("redirect-metadata"), newRequest(t, "GET", server.URL, nil))
		result := settle(t, f, receipt)
		if !errors.Is(err, ErrLimit) || !errors.Is(result.Err(), ErrLimit) || requests.Load() != 1 || result.Outcome.Value.Complete() {
			t.Fatal("redirect metadata bypassed native bounds", err)
		}
	})
}

func TestFixedOutboundTrailers(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		seen := make(chan string, 1)
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			_, _ = io.Copy(io.Discard, request.Body)
			seen <- request.Trailer.Get("X-Content-Digest")
			_, _ = io.WriteString(writer, "complete")
		})
		f := bindFixture(t, options, 1)
		request := newRequest(t, "POST", server.URL, io.NopCloser(strings.NewReader("body")))
		request.Trailer = http.Header{"X-Content-Digest": {"precomputed"}}
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("fixed-trailer"), request)
		if err != nil {
			t.Fatal(err)
		}
		settle(t, f, receipt)
		if <-seen != "precomputed" || request.Trailer.Get("X-Content-Digest") != "precomputed" {
			t.Fatal("fixed trailer was lost or mutated")
		}
	})
}

type blockingBody struct {
	*bytes.Reader
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (body *blockingBody) Close() error {
	body.once.Do(func() { close(body.entered) })
	<-body.release
	return nil
}

func TestCleanupWaitDoesNotReleaseUploadBody(t *testing.T) {
	body := &blockingBody{Reader: bytes.NewReader([]byte("body")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(body.release) }) }
	defer unblock()
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		_, _ = io.WriteString(writer, "done")
	})
	f := bindFixture(t, options, 1)
	cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	request := newRequest(t, "POST", server.URL, body)
	request.ContentLength = 4
	receipt, err := f.client.Do(deadline(t), cleanup, correlation("cleanup"), request)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocking close did not retain a pending wait", err)
	}
	<-body.entered
	if result, ok := receipt.Result(); ok && result.Released {
		t.Fatal("blocked native body was released")
	}
	if err := f.assembly.Close(deadline(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("cleanup responsibility disappeared", err)
	}
	unblock()
	result := settle(t, f, receipt)
	if result.Err() != nil || !result.Released {
		t.Fatal("eventual cleanup did not finish", result.Err())
	}
}
