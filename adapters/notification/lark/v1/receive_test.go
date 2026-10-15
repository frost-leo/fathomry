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
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func callbackBody(kind string) []byte {
	return []byte(`{"schema":"2.0","header":{"event_id":"event_fixture","event_type":"` + kind + `","app_id":"app_fixture","tenant_key":"tenant_fixture","token":"verification","create_time":"1700000000000"},"event":{"number":9007199254740993,"empty":[],"null":null,"flag":false}}`)
}
func TestHTTPAuthenticationResponseAndRejectingControls(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("HTTP reception made outgoing request") }))
	defer server.Close()
	settings := fixtureSettings(server)
	settings.VerificationToken = "verification"
	settings.EncryptKey = "encryption"
	owner, _, inbox := testOwner(t, settings, NativeOptions{})
	for _, mode := range []string{"modern", "legacy", "challenge", "duplicate", "oversize", "handler-error", "handler-panic", "write-failure", "wrong-signature"} {
		t.Run(mode, func(t *testing.T) {
			legacy := mode == "legacy"
			body := callbackBody("url.preview.get")
			if legacy {
				body = []byte(`{"open_id":"ou_fixture","open_message_id":"om_fixture","tenant_key":"tenant_fixture","token":"update-only","action":{"value":{"n":9007199254740993}}}`)
			}
			if mode == "challenge" {
				body = []byte(`{"type":"url_verification","token":"verification","challenge":"quoted\\\"challenge"}`)
			}
			if mode == "oversize" {
				body = bytes.Repeat([]byte("x"), settings.MaxRequestBytes+1)
			}
			request := httptest.NewRequest("POST", "https://callback.invalid/", bytes.NewReader(body))
			stamp, nonce := strconv.FormatInt(time.Now().Unix(), 10), "nonce"
			digest := sha256.Sum256(append([]byte(stamp+nonce+"encryption"), body...))
			signature := hex.EncodeToString(digest[:])
			if legacy {
				digest := sha1.Sum(append([]byte(stamp+nonce+"verification"), body...))
				signature = hex.EncodeToString(digest[:])
			}
			if mode != "challenge" {
				request.Header.Set("X-Lark-Request-Timestamp", stamp)
				request.Header.Set("X-Lark-Request-Nonce", nonce)
				request.Header.Set("X-Lark-Signature", signature)
			}
			if mode == "duplicate" {
				request.Header["x-lark-signature"] = []string{signature}
			}
			if mode == "wrong-signature" {
				request.Header.Set("X-Lark-Signature", strings.Repeat("0", 64))
			}
			captured := httptest.NewRecorder()
			var writer http.ResponseWriter = captured
			if mode == "write-failure" {
				writer = &failedWriter{header: http.Header{}}
			}
			var calls atomic.Int32
			handler := func(context.Context, Event) (HTTPResponse, error) {
				calls.Add(1)
				if mode == "handler-error" {
					return HTTPResponse{}, errors.New("private-handler")
				}
				if mode == "handler-panic" {
					panic(errors.New("private-handler"))
				}
				return HTTPResponse{Status: 201, Body: []byte(`{"inline":{"title":"fixture"},"number":9007199254740993}`)}, nil
			}
			result, err := owner.Client().HandleHTTP(context.Background(), request, writer, legacy, handler)
			success := mode == "modern" || mode == "legacy" || mode == "challenge"
			if success {
				if err != nil || !result.HTTPWrite().Written {
					t.Fatal("callback response failed", err)
				}
				if mode == "challenge" {
					if calls.Load() != 0 {
						t.Fatal("challenge invoked business handler")
					}
					var value map[string]string
					if json.Unmarshal(captured.Body.Bytes(), &value) != nil || !strings.Contains(value["challenge"], "quoted") {
						t.Fatal("challenge not escaped")
					}
				} else if calls.Load() != 1 || captured.Code != 201 || !bytes.Contains(captured.Body.Bytes(), []byte("9007199254740993")) {
					t.Fatal("response choice/precision lost")
				}
			} else if err == nil {
				t.Fatal("rejecting control accepted")
			}
			if mode == "duplicate" || mode == "oversize" || mode == "wrong-signature" {
				if calls.Load() != 0 || captured.Body.Len() != 0 {
					t.Fatal("unauthenticated handler/write")
				}
			}
			if mode == "write-failure" && (!result.HTTPWrite().Entered || result.HTTPWrite().Written) {
				t.Fatal("failed response write reported complete")
			}
			_ = consume(t, inbox).Ack()
		})
	}
}

type failedWriter struct{ header http.Header }

func TestHTTPDisconnectCancelsExplicitHandling(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	settings := fixtureSettings(server)
	settings.VerificationToken = "verification"
	owner, _, _ := testOwner(t, settings, NativeOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest("POST", "https://callback.invalid", bytes.NewReader(callbackBody("card.action.trigger"))).WithContext(ctx)
	called := false
	_, err := owner.Client().HandleHTTP(context.Background(), request, httptest.NewRecorder(), false, func(context.Context, Event) (HTTPResponse, error) { called = true; return HTTPResponse{}, nil })
	if !errors.Is(err, context.Canceled) || called {
		t.Fatal("HTTP disconnect was ignored", err)
	}
}

func (value *failedWriter) Header() http.Header { return value.header }
func (*failedWriter) WriteHeader(int)           {}
func (*failedWriter) Write([]byte) (int, error) { return 1, io.ErrUnexpectedEOF }

func TestHTTPHandlerOutlivesCanceledWaitWithoutEarlySourceRelease(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	settings := fixtureSettings(server)
	settings.VerificationToken = "verification"
	owner, _, inbox := testOwner(t, settings, NativeOptions{})
	entered, proceed := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	writer := httptest.NewRecorder()
	go func() {
		_, err := owner.Client().HandleHTTP(context.Background(), httptest.NewRequest("POST", "https://callback.invalid", bytes.NewReader(callbackBody("card.action.trigger"))), writer, false,
			func(context.Context, Event) (HTTPResponse, error) {
				close(entered)
				<-proceed
				return HTTPResponse{Body: []byte("{}")}, nil
			})
		done <- err
	}()
	<-entered
	wait, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := owner.Close(wait); !errors.Is(err, context.DeadlineExceeded) || owner.ShutdownComplete() {
		close(proceed)
		t.Fatal("handler released before actual return", err)
	}
	close(proceed)
	if err := <-done; err == nil || writer.Body.Len() != 0 {
		t.Fatal("late handler wrote success")
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = consume(t, inbox).Ack()
}
