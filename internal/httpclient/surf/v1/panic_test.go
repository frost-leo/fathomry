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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/enetx/g"
	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles"
	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/internal/fault"
	utls "github.com/refraction-networking/utls"
)

func TestReviewNativeMiddlewarePanicIsRetained(t *testing.T) {
	if os.Getenv("FATHOMRY_SURF_PANIC_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReviewNativeMiddlewarePanicIsRetained$")
		command.Env = append(os.Environ(), "FATHOMRY_SURF_PANIC_CHILD=1")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("native middleware escaped owned execution: %v\n%s", err, output)
		}
		if !strings.Contains(string(output), "PASS") {
			t.Fatal("panic child did not finish its assertions")
		}
		return
	}
	cause := errors.New("synthetic middleware panic")
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }))
	defer peer.Close()
	fix := newFixture(t, OptionsV1{Name: "panic", Mode: HTTP1Only, Native: NativeOptionsV1{
		RequestMiddleware: []func(*sdk.Request) error{func(*sdk.Request) error { panic(cause) }},
	}}, 1)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "panic"}, request(t, "GET", peer.URL, ""))
	if err == nil {
		t.Fatal("callback panic became success")
	}
	value := settle(t, fix, receipt)
	if !errors.Is(value.Outcome.Primary, cause) || !value.Released {
		t.Fatal("callback panic lost evidence or ownership")
	}
	for _, phase := range []string{"response", "redirect", "jar-read", "jar-write", "dial", "listen", "hello", "profile", "profile-headers", "verify", "verify-ja", "verify-h3", "cache", "cache-closed-cause", "replay", "reader"} {
		t.Run(phase, func(t *testing.T) {
			cause := cause
			if phase == "cache-closed-cause" {
				cause = sdk.ErrFathomryClosed
			}
			peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				if phase == "redirect" || phase == "replay" {
					w.Header().Set("Location", "/next")
					w.WriteHeader(307)
					return
				}
				w.Header().Set("Set-Cookie", "fixture=value")
				_, _ = io.WriteString(w, "ok")
			}, phase == "listen" || phase == "verify-h3")
			options := OptionsV1{Name: "panic-" + phase, Mode: HTTP1Only, Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots}}}
			input := request(t, "POST", peer.tcp.URL, "payload")
			switch phase {
			case "response":
				options.Native.ResponseMiddleware = []func(*sdk.Response) error{func(*sdk.Response) error { panic(cause) }}
			case "redirect":
				options.Native.CheckRedirect = func(*http.Request, []*http.Request) error { panic(cause) }
			case "jar-read", "jar-write":
				options.Native.Jar = panicJar{cause: cause, read: phase == "jar-read"}
			case "dial":
				options.Native.DialContext = func(context.Context, string, string) (net.Conn, error) { panic(cause) }
			case "listen":
				options.Mode = PreferHTTP3
				options.Native.ListenPacket = func(context.Context, string, string) (net.PacketConn, error) { panic(cause) }
			case "hello":
				options.Native.HelloSpecFactory = func(context.Context) (utls.ClientHelloSpec, error) { panic(cause) }
			case "profile", "profile-headers":
				profile := chrome.Desktop
				if phase == "profile" {
					profile.BuildHeaders = func(profiles.OSKey) *g.MapOrd[g.String, g.String] { panic(cause) }
				} else {
					profile.Headers = func(any, string) { panic(cause) }
				}
				options.Native.Profile = &profile
			case "verify", "verify-ja", "verify-h3":
				options.Native.TLSConfig.VerifyPeerCertificate = func([][]byte, [][]*x509.Certificate) error { panic(cause) }
				if phase == "verify-ja" {
					profile := chrome.Desktop
					options.Native.Profile = &profile
				}
				if phase == "verify-h3" {
					options.Mode = PreferHTTP3
				}
			case "cache", "cache-closed-cause":
				options.Native.TLSConfig.ClientSessionCache = panicTLSCache{cause}
			case "replay":
				input.GetBody = func() (io.ReadCloser, error) { panic(cause) }
			case "reader":
				input.Body = panicReader{cause}
			}
			fix := newFixture(t, options, 1)
			if strings.HasPrefix(phase, "cache") {
				fix.cleanupCause = cause
			}
			receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: phase}, input)
			if err == nil {
				t.Fatal("callback panic became success")
			}
			value := settle(t, fix, receipt)
			if !errors.Is(value.Outcome.Primary, cause) || value.Outcome.Value.Complete() {
				t.Fatal("callback panic lost original cause or became complete", value.Err())
			}
			if strings.HasPrefix(phase, "cache") {
				if err := fix.assembly.Close(testContext(t)); !errors.Is(err, cause) || !errors.Is(err, sdk.ErrFathomryCallback) {
					t.Fatal("notification failure absent from required resource cleanup", err)
				}
			}
		})
	}
}

type panicJar struct {
	cause error
	read  bool
}

func (jar panicJar) Cookies(*url.URL) []*http.Cookie {
	if jar.read {
		panic(jar.cause)
	}
	return nil
}
func (jar panicJar) SetCookies(*url.URL, []*http.Cookie) { panic(jar.cause) }

type panicTLSCache struct{ cause error }

func (cache panicTLSCache) Get(string) (*tls.ClientSessionState, bool) { panic(cache.cause) }
func (cache panicTLSCache) Put(string, *tls.ClientSessionState)        { panic(cache.cause) }

type panicReader struct{ cause error }

func (reader panicReader) Read([]byte) (int, error) { panic(reader.cause) }
func (reader panicReader) Close() error             { return nil }
