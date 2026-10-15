/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package lark

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"
)

type callbackExitReader struct{ stop func() }

func (reader callbackExitReader) Read([]byte) (int, error) { reader.stop(); return 0, io.EOF }

type callbackExitWriter struct {
	phase  string
	stop   func()
	header http.Header
}

func (writer *callbackExitWriter) Header() http.Header {
	if writer.phase == "header" {
		writer.stop()
	}
	return writer.header
}
func (writer *callbackExitWriter) WriteHeader(int) {
	if writer.phase == "write-header" {
		writer.stop()
	}
}
func (writer *callbackExitWriter) Write(body []byte) (int, error) {
	if writer.phase == "write" {
		writer.stop()
	}
	return len(body), nil
}

type callbackExitResult struct {
	value Result
	err   error
}

func TestWebSocketCredentialDeadlineRetainsCause(t *testing.T) {
	for _, mode := range []string{"error", "panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("expired provider reached wire") }))
			defer server.Close()
			settings := fixtureSettings(server)
			settings.Profile, settings.TenantToken, settings.TokenExpiresUnix = "application", "", 0
			settings.AppSecret, settings.Timeout = "synthetic-secret", 20*time.Millisecond
			settings.WebSocket = &WebSocketSettings{AllowedHosts: []string{"127.0.0.1"}, MaxPending: 1, MaxAssemblies: 1, Lifetime: time.Second}
			secondary := errors.New("private-readback-establishment-error")
			owner, _, inbox := testOwner(t, settings, NativeOptions{Credentials: func(ctx context.Context, _ CredentialRequest) (Credentials, error) {
				<-ctx.Done()
				switch mode {
				case "panic":
					panic(secondary)
				case "goexit":
					runtime.Goexit()
				}
				return Credentials{}, secondary
			}})
			result, err := owner.Client().Listen(context.Background(), publicEvents(t, 4))
			_ = consume(t, inbox).Ack()
			if !errors.Is(err, context.DeadlineExceeded) || mode != "goexit" && !errors.Is(err, secondary) {
				t.Fatalf("provider establishment deadline or secondary cause lost: deadline=%t secondary=%t err=%v", errors.Is(err, context.DeadlineExceeded), errors.Is(err, secondary), err)
			}
			if result.Attempts().Observed != 0 {
				t.Fatal("expired provider reached a network attempt")
			}
			regressionReadbackPrivacy(t, err, result)
		})
	}
}

func awaitCallbackReturn(t testing.TB, run func() (Result, error)) callbackExitResult {
	t.Helper()
	result := make(chan callbackExitResult, 1)
	go func() {
		value, err := run()
		result <- callbackExitResult{value: value, err: err}
	}()
	select {
	case value := <-result:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("facade did not return after borrowed callback terminated")
	}
	return callbackExitResult{}
}

func TestCallbackTerminationRetainsNativeOutcomeAndRelease(t *testing.T) {
	for _, phase := range []string{"asset", "credentials", "webhook", "websocket"} {
		for _, abrupt := range []string{"panic", "goexit"} {
			for _, canceled := range []bool{false, true} {
				label := phase + "/" + abrupt
				if canceled {
					label += "/canceled"
				}
				t.Run(label, func(t *testing.T) {
					peer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("aborted callback reached the network") }))
					defer peer.Close()
					ctx, cancel := context.WithCancelCause(context.Background())
					defer cancel(nil)
					cause, panicCause := errors.New("private callback cancellation"), errors.New("private callback panic")
					exited := make(chan struct{})
					stop := func() {
						defer close(exited)
						if canceled {
							cancel(cause)
						}
						if abrupt == "panic" {
							panic(panicCause)
						}
						runtime.Goexit()
					}
					settings := fixtureSettings(peer)
					hooks := NativeOptions{}
					switch phase {
					case "credentials", "websocket":
						hooks.Credentials = func(context.Context, CredentialRequest) (Credentials, error) { stop(); return Credentials{}, nil }
					case "webhook":
						settings.VerificationToken = "verification"
						hooks.WebhookCredentials = func(context.Context) (WebhookCredentials, error) { stop(); return WebhookCredentials{}, nil }
					}
					if phase == "websocket" {
						settings.Profile, settings.AppSecret, settings.TenantToken, settings.TokenExpiresUnix = "application", "fixture-secret", "", 0
						settings.WebSocket = &WebSocketSettings{AllowedHosts: []string{"127.0.0.1"}, MaxPending: 1, MaxAssemblies: 1}
					}
					owner, _, inbox := testOwner(t, settings, hooks)
					received := publicEvents(t, 4)
					result := awaitCallbackReturn(t, func() (Result, error) {
						switch phase {
						case "asset":
							return owner.Client().UploadAsset(ctx, true, AssetInput{Name: "fixture.png", ImageType: "message", Reader: callbackExitReader{stop: stop}}, RequestOptions{})
						case "webhook":
							return owner.Client().Receive(ctx, Callback{Body: callbackBody("card.action.trigger")})
						case "websocket":
							return owner.Client().Listen(ctx, received)
						default:
							return owner.Client().Send(ctx, Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t))
						}
					})
					select {
					case <-exited:
					default:
						t.Fatal("callback still active after facade returned")
					}
					if result.err == nil || result.value.Attribution().Sequence == 0 || result.value.Attempts().Observed != 0 {
						t.Fatal("abnormal callback did not retain its admitted failure", result.err)
					}
					if canceled && (!errors.Is(result.err, context.Canceled) || !errors.Is(result.err, cause)) {
						t.Fatal("callback cancellation cause lost")
					}
					if abrupt == "panic" && !canceled && !errors.Is(result.err, panicCause) {
						t.Fatal("callback panic cause lost")
					}
					if _, present := result.value.Event(); present {
						t.Fatal("aborted credentials authenticated an event")
					}
					record := consume(t, inbox)
					receipt, _ := record.Receipt()
					snapshot, _ := receipt.WaitReleased(context.Background())
					value, present := snapshot.ValueCopy()
					if !present || snapshot.Err() == nil || value.Attribution().Sequence != result.value.Attribution().Sequence {
						t.Fatal("independent failure evidence lost")
					}
					if err := record.Ack(); err != nil {
						t.Fatal(err)
					}
					closing, stopClose := context.WithTimeout(context.Background(), time.Second)
					defer stopClose()
					if err := owner.Close(closing); err != nil || !owner.ShutdownComplete() {
						t.Fatal("source retained exited callback", err)
					}
				})
			}
		}
	}
}

func TestHTTPCallbackTerminationPreservesAuthenticatedAndWriteFacts(t *testing.T) {
	for _, phase := range []string{"reader", "handler", "header", "write-header", "write"} {
		for _, abrupt := range []string{"panic", "goexit"} {
			t.Run(phase+"/"+abrupt, func(t *testing.T) {
				peer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("HTTP callback opened outbound connection") }))
				defer peer.Close()
				settings := fixtureSettings(peer)
				settings.VerificationToken = "verification"
				owner, _, inbox := testOwner(t, settings, NativeOptions{})
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				cause, panicCause := errors.New("private HTTP cancellation"), errors.New("private HTTP panic")
				stop := func() {
					cancel(cause)
					if abrupt == "panic" {
						panic(panicCause)
					}
					runtime.Goexit()
				}
				request := httptest.NewRequest(http.MethodPost, "https://callback.invalid", bytes.NewReader(callbackBody("card.action.trigger")))
				if phase == "reader" {
					request.Body = io.NopCloser(callbackExitReader{stop: stop})
				}
				writer := &callbackExitWriter{phase: phase, stop: stop, header: http.Header{}}
				result := awaitCallbackReturn(t, func() (Result, error) {
					return owner.Client().HandleHTTP(ctx, request, writer, false, func(context.Context, Event) (HTTPResponse, error) {
						if phase == "handler" {
							stop()
						}
						return HTTPResponse{Body: []byte("{}")}, nil
					})
				})
				if !errors.Is(result.err, context.Canceled) || !errors.Is(result.err, cause) {
					t.Fatal("HTTP termination/cause lost", result.err)
				}
				if abrupt == "panic" && !errors.Is(result.err, panicCause) {
					t.Fatal("HTTP panic cause lost")
				}
				_, authenticated := result.value.Event()
				entered := phase == "header" || phase == "write-header" || phase == "write"
				if authenticated != (phase != "reader") || result.value.HTTPWrite().Entered != entered || result.value.HTTPWrite().Written {
					t.Fatal("HTTP partial facts changed at callback exit")
				}
				record := consume(t, inbox)
				receipt, _ := record.Receipt()
				snapshot, _ := receipt.WaitReleased(context.Background())
				value, present := snapshot.ValueCopy()
				_, eventPresent := value.Event()
				if snapshot.Err() == nil || present != (phase != "reader") || eventPresent != authenticated || value.HTTPWrite() != result.value.HTTPWrite() {
					t.Fatal("independent HTTP facts lost")
				}
				if err := record.Ack(); err != nil {
					t.Fatal(err)
				}
				closing, stopClose := context.WithTimeout(context.Background(), time.Second)
				defer stopClose()
				if err := owner.Close(closing); err != nil || !owner.ShutdownComplete() {
					t.Fatal("HTTP source still retained", err)
				}
			})
		}
	}
}
