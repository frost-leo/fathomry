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
	"crypto/x509"
	"errors"
	"io"
	"log"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enetx/g"
	"github.com/enetx/http"
	utls "github.com/refraction-networking/utls"
)

func TestFathomryJACustomRoots(t *testing.T) {
	peer := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Write([]byte("exact"))
	}))
	peer.EnableHTTP2 = true
	peer.Config.ErrorLog = log.New(io.Discard, "", 0)
	peer.StartTLS()
	defer peer.Close()
	for _, trusted := range []bool{true, false} {
		t.Run(map[bool]string{true: "trusted", false: "untrusted"}[trusted], func(t *testing.T) {
			client := NewClient()
			defer client.Close()
			client.GetTLSConfig().RootCAs = x509.NewCertPool()
			if trusted {
				client.GetTLSConfig().RootCAs.AddCert(peer.Certificate())
			}
			built := client.Builder().SecureTLS().Proxy("").JA().SetHelloID(utls.HelloChrome_Auto).Build()
			if built.IsErr() {
				t.Fatal(built.Err())
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			got := client.Get(g.String(peer.URL)).WithContext(ctx).Do()
			if !trusted {
				if got.IsOk() {
					got.Ok().Body.Close()
					t.Fatal("untrusted root was accepted")
				}
				var unknown x509.UnknownAuthorityError
				if !errors.As(got.Err(), &unknown) {
					t.Fatalf("typed verification cause lost: %v", got.Err())
				}
				return
			}
			if got.IsErr() {
				t.Fatal(got.Err())
			}
			response := got.Ok()
			data := response.Body.Bytes()
			if data.IsErr() || string(data.Ok()) != "exact" || response.Proto != "HTTP/2.0" {
				t.Fatalf("wrong trusted response: %s %v", response.Proto, data.Err())
			}
		})
	}
}

type fathomryBody struct {
	io.Reader
	closes   atomic.Int32
	closeErr error
}

func (body *fathomryBody) Close() error {
	body.closes.Add(1)
	return body.closeErr
}

func TestFathomryBodyIntegrityAndCleanupCauses(t *testing.T) {
	t.Run("truncation", func(t *testing.T) {
		reader := &fathomryBody{Reader: strings.NewReader("abc")}
		body := &Body{Reader: reader, contentLength: 6, limit: 100}
		got := body.Bytes()
		if !got.IsErr() || !errors.Is(got.Err(), io.ErrUnexpectedEOF) {
			t.Fatalf("truncated declared body accepted: %v", got.Err())
		}
		if reader.closes.Load() != 1 {
			t.Fatal("body not closed exactly once")
		}
	})
	t.Run("limit", func(t *testing.T) {
		reader := &fathomryBody{Reader: strings.NewReader("abcdef")}
		body := &Body{Reader: reader, contentLength: -1, limit: 3}
		if got := body.Bytes(); !got.IsErr() {
			t.Fatal("size-limit prefix presented as complete")
		}
	})
	t.Run("close-cause", func(t *testing.T) {
		cause := errors.New("native close")
		reader := &fathomryBody{Reader: strings.NewReader("abc"), closeErr: cause}
		body := &Body{Reader: reader, contentLength: 3, limit: 100}
		if got := body.Bytes(); !got.IsErr() || !errors.Is(got.Err(), cause) {
			t.Fatal("read discarded close cause")
		}
		if !errors.Is(body.Close(), cause) {
			t.Fatal("repeated close erased original cause")
		}
		if reader.closes.Load() != 1 {
			t.Fatal("close repeated native work")
		}
	})
}

type fathomryRoundTrip func(*http.Request) (*http.Response, error)

func (fn fathomryRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

func TestFathomryMiddlewareFailureCleansBodies(t *testing.T) {
	cause := errors.New("native middleware")
	t.Run("request", func(t *testing.T) {
		reader := &fathomryBody{Reader: strings.NewReader("input")}
		client := NewClient()
		defer client.Close()
		client.Builder().With(func(*Request) error { return cause }).Build().Unwrap()
		request := client.Post("http://fixture.invalid")
		request.GetRequest().Body = reader
		got := request.Do()
		if !got.IsErr() || !errors.Is(got.Err(), cause) {
			t.Fatal("request cause changed")
		}
		if reader.closes.Load() != 1 {
			t.Fatal("request middleware failure leaked input")
		}
	})
	t.Run("response", func(t *testing.T) {
		reader := &fathomryBody{Reader: strings.NewReader("output")}
		client := NewClient()
		defer client.Close()
		client.GetClient().Transport = fathomryRoundTrip(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: reader, ContentLength: 6, Request: r}, nil
		})
		client.Builder().With(func(*Response) error { return cause }).Build().Unwrap()
		got := client.Get("http://fixture.invalid").Do()
		if !got.IsErr() || !errors.Is(got.Err(), cause) {
			t.Fatal("response cause changed")
		}
		if reader.closes.Load() != 1 {
			t.Fatal("response middleware failure leaked body")
		}
	})
}

func TestFathomryHelloSpecFrozenAtConfiguration(t *testing.T) {
	spec, err := utls.UTLSIdToSpec(utls.HelloChrome_Auto)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient()
	defer client.Close()
	ja := client.Builder().JA()
	ja.SetHelloSpec(spec)
	before := spec.CipherSuites[0]
	spec.CipherSuites[0] = 0xffff
	got := ja.getSpec()
	if got.IsErr() || got.Ok().CipherSuites[0] != before {
		t.Fatal("caller mutation changed selected native spec")
	}
}

type fathomryFallback struct {
	closed atomic.Int32
	cause  error
}

func (*fathomryFallback) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unused")
}
func (fallback *fathomryFallback) CloseIdleConnections() { fallback.closed.Add(1) }

func TestFathomryH3ShutdownIncludesFallback(t *testing.T) {
	fallback := &fathomryFallback{}
	transport := &uquicTransport{fallbackTransport: fallback}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if fallback.closed.Load() != 1 {
		t.Fatal("H3 Close omitted fallback idle pools")
	}
}

func TestFathomryH2ConstructionDoesNotMutateSharedSettings(t *testing.T) {
	peer := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }))
	peer.EnableHTTP2 = true
	peer.Config.ErrorLog = log.New(io.Discard, "", 0)
	peer.StartTLS()
	defer peer.Close()
	client := NewClient()
	defer client.Close()
	client.GetTLSConfig().RootCAs = x509.NewCertPool()
	client.GetTLSConfig().RootCAs.AddCert(peer.Certificate())
	builder := client.Builder().SecureTLS().Proxy("")
	builder.HTTP2Settings().MaxHeaderListSize(4096).Set()
	builder.JA().SetHelloID(utls.HelloChrome_Auto)
	built := builder.Build()
	if built.IsErr() {
		t.Fatal(built.Err())
	}
	transport := client.GetClient().Transport.(*roundtripper).http2tr
	before := transport.MaxHeaderListSize
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := client.Get(g.String(peer.URL)).WithContext(ctx).Do()
	if result.IsErr() {
		t.Fatal(result.Err())
	}
	if body := result.Ok().Body.Bytes(); body.IsErr() {
		t.Fatal(body.Err())
	}
	if transport.MaxHeaderListSize != before {
		t.Fatalf("connection construction mutated shared transport settings: %d -> %d", before, transport.MaxHeaderListSize)
	}
}
