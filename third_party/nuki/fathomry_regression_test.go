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

package tlsclient_test

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nhttp "github.com/nukilabs/http"
	nuki "github.com/nukilabs/tlsclient"
	"github.com/nukilabs/tlsclient/profiles"
)

func TestFathomryNoFollowPreservesFirstResponse(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/first" {
			http.Redirect(w, r, "/last", 302)
			return
		}
		w.WriteHeader(200)
	}))
	defer peer.Close()
	client := nuki.New(profiles.Safari17, nuki.WithNoFollowRedirects(), nuki.WithNoCookieJar())
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := nhttp.NewRequestWithContext(ctx, "GET", peer.URL+"/first", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 302 || calls.Load() != 1 {
		t.Fatalf("no-follow ignored: status=%d server_calls=%d", res.StatusCode, calls.Load())
	}
}

func TestFathomryConcurrentRequestsDoNotSkipHooks(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer peer.Close()
	client := nuki.New(profiles.Safari17, nuki.WithNoCookieJar())
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var hooks atomic.Int32
	client.SetPreHooks(func(_ *nuki.Client, req *nhttp.Request) (*nhttp.Request, error) {
		if hooks.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return req, nil
	})
	do := func() error {
		req, err := nhttp.NewRequestWithContext(ctx, "GET", peer.URL, nil)
		if err != nil {
			return err
		}
		res, err := client.Do(req)
		if err == nil {
			err = res.Body.Close()
		}
		return err
	}
	first := make(chan error, 1)
	go func() { first <- do() }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	secondErr := do()
	unblock()
	firstErr := <-first
	if firstErr != nil || secondErr != nil {
		t.Fatal(firstErr, secondErr)
	}
	if hooks.Load() != 2 {
		t.Fatalf("native hook skipped: requests=2 hook_calls=%d", hooks.Load())
	}
}

type responseTransport struct{ body *countedBody }

func (transport responseTransport) RoundTrip(req *nhttp.Request) (*nhttp.Response, error) {
	return &nhttp.Response{StatusCode: 200, Header: make(nhttp.Header), Body: transport.body, Request: req, ContentLength: 4}, nil
}

type countedBody struct {
	io.Reader
	closed atomic.Int32
}

func (body *countedBody) Close() error { body.closed.Add(1); return nil }

func TestFathomryPostHookFailureClosesResponse(t *testing.T) {
	body := &countedBody{Reader: bytes.NewBufferString("body")}
	client := nuki.New(profiles.Safari17, nuki.WithNoCookieJar())
	client.CloseIdleConnections()
	client.Transport = responseTransport{body: body}
	cause := errors.New("synthetic-post-hook-cause")
	client.SetPostHooks(func(_ *nuki.Client, _ *nhttp.Request, _ *nhttp.Response) (*nhttp.Response, error) { return nil, cause })
	req, err := nhttp.NewRequest("GET", "http://synthetic.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(req)
	if !errors.Is(err, cause) {
		t.Fatal("post-hook cause lost")
	}
	if body.closed.Load() != 1 {
		t.Fatalf("discarded response not closed: closes=%d", body.closed.Load())
	}
}

func TestFathomryDeflateAlwaysClosesUnderlyingBody(t *testing.T) {
	for _, read := range []bool{false, true} {
		name := "before-read"
		if read {
			name = "after-read"
		}
		t.Run(name, func(t *testing.T) {
			var encoded bytes.Buffer
			writer := zlib.NewWriter(&encoded)
			_, err := writer.Write([]byte("payload"))
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			body := &countedBody{Reader: bytes.NewReader(encoded.Bytes())}
			res := &nhttp.Response{Header: nhttp.Header{"Content-Encoding": {"deflate"}}, Body: body, ContentLength: int64(encoded.Len())}
			nuki.DecompressBody(res)
			defer func() {
				if recover() != nil {
					t.Error("native Close panicked")
				}
				if body.closed.Load() != 1 {
					t.Errorf("underlying body not closed: closes=%d", body.closed.Load())
				}
			}()
			if read {
				got, err := io.ReadAll(res.Body)
				if err != nil || string(got) != "payload" {
					t.Fatal("decoded response mismatch", err)
				}
			}
			if err := res.Body.Close(); err != nil {
				t.Error(err)
			}
		})
	}
}

type onceErrorBody struct {
	delivered bool
	cause     error
}

func (body *onceErrorBody) Read(data []byte) (int, error) {
	if body.delivered {
		return 0, io.EOF
	}
	body.delivered = true
	data[0] = 0x78
	return 1, body.cause
}
func (*onceErrorBody) Close() error { return nil }

func TestFathomryDeflatePreservesInitializationError(t *testing.T) {
	cause := errors.New("synthetic-encoded-read-cause")
	res := &nhttp.Response{Header: nhttp.Header{"Content-Encoding": {"deflate"}}, Body: &onceErrorBody{cause: cause}, ContentLength: 9}
	nuki.DecompressBody(res)
	_, err := io.ReadAll(res.Body)
	if !errors.Is(err, cause) {
		t.Fatal("native decoder discarded encoded read cause")
	}
}
