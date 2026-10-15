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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

type regressionReadbackReader func([]byte) (int, error)

func (reader regressionReadbackReader) Read(body []byte) (int, error) { return reader(body) }

type regressionReadbackWriter struct {
	header http.Header
	write  func([]byte) (int, error)
}

func (writer *regressionReadbackWriter) Header() http.Header            { return writer.header }
func (*regressionReadbackWriter) WriteHeader(int)                       {}
func (writer *regressionReadbackWriter) Write(body []byte) (int, error) { return writer.write(body) }

func regressionReadbackPrivacy(t *testing.T, values ...any) {
	t.Helper()
	for _, value := range values {
		for _, rendered := range []string{fmt.Sprintf("%v", value), fmt.Sprintf("%+v", value), fmt.Sprintf("%#v", value)} {
			if strings.Contains(rendered, "private-readback") {
				t.Fatal("private fmt diagnostic leaked")
			}
		}
		encoded, _ := json.Marshal(value)
		if bytes.Contains(encoded, []byte("private-readback")) {
			t.Fatal("private JSON diagnostic leaked")
		}
		var text, structured bytes.Buffer
		slog.New(slog.NewTextHandler(&text, nil)).Error("probe", "value", value)
		slog.New(slog.NewJSONHandler(&structured, nil)).Error("probe", "value", value)
		if bytes.Contains(text.Bytes(), []byte("private-readback")) || bytes.Contains(structured.Bytes(), []byte("private-readback")) {
			t.Fatal("private slog diagnostic leaked")
		}
	}
}

func TestRegressionReadbackCombinedCancellation(t *testing.T) {
	for _, mode := range []string{"reader-error", "reader-panic", "handler-error", "handler-panic", "writer-error", "writer-panic", "credential-error", "credential-panic"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected outbound request") }))
			defer server.Close()
			settings := fixtureSettings(server)
			settings.VerificationToken = "verification"
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause, secondary := errors.New("private-readback-cancellation"), errors.New("private-readback-secondary")
			trigger := func() error {
				cancel(cause)
				if strings.HasSuffix(mode, "panic") {
					panic(secondary)
				}
				return secondary
			}
			hooks := NativeOptions{}
			if strings.HasPrefix(mode, "credential") {
				hooks.WebhookCredentials = func(context.Context) (WebhookCredentials, error) { return WebhookCredentials{}, trigger() }
			}
			owner, _, inbox := testOwner(t, settings, hooks)
			request := httptest.NewRequest("POST", "https://callback.invalid", bytes.NewReader(callbackBody("card.action.trigger")))
			if strings.HasPrefix(mode, "reader") {
				request.Body = io.NopCloser(regressionReadbackReader(func([]byte) (int, error) { return 0, trigger() }))
			}
			var writer http.ResponseWriter = httptest.NewRecorder()
			if strings.HasPrefix(mode, "writer") {
				writer = &regressionReadbackWriter{header: http.Header{}, write: func([]byte) (int, error) { return 0, trigger() }}
			}
			result, err := owner.Client().HandleHTTP(ctx, request, writer, false, func(context.Context, Event) (HTTPResponse, error) {
				if strings.HasPrefix(mode, "handler") {
					return HTTPResponse{}, trigger()
				}
				return HTTPResponse{Body: []byte("{}")}, nil
			})
			_ = consume(t, inbox).Ack()
			if !errors.Is(err, secondary) || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
				t.Errorf("combined causes lost: secondary=%t canceled=%t custom=%t err=%v", errors.Is(err, secondary), errors.Is(err, context.Canceled), errors.Is(err, cause), err)
			}
			if result.HTTPWrite().Entered != strings.HasPrefix(mode, "writer") || result.HTTPWrite().Written {
				t.Fatal("canceled/failing work fabricated write facts")
			}
			regressionReadbackPrivacy(t, err, result)
		})
	}
}

func TestRegressionReadbackOAuthSchemaAndPrivacy(t *testing.T) {
	for _, item := range []struct {
		name, body, message string
		status              int
		accepted            bool
	}{
		{"description", `{"code":20050,"error":"private-readback-error","error_description":"private-readback-description"}`, "private-readback-description", 200, false},
		{"error-fallback", `{"code":20050,"error":"private-readback-error"}`, "private-readback-error", 200, false},
		{"missing-token", `{"code":0,"expires_in":7200}`, "oauth token response missing access token", 200, false},
		{"malformed-error", `{"code":20050,"error":{"message":"private-readback-error"}}`, "", 200, false},
		{"wrong-case", `{"code":20050,"Error_Description":"private-readback-error"}`, "", 200, false},
		{"missing-code", `{"access_token":"private-readback-token","expires_in":7200}`, "", 200, false},
		{"null-code", `{"code":null,"access_token":"private-readback-token","expires_in":7200}`, "", 200, false},
		{"zero-expiry", `{"code":0,"access_token":"private-readback-token","expires_in":0}`, "", 200, false},
		{"excess-expiry", `{"code":0,"access_token":"private-readback-token","expires_in":7201}`, "", 200, false},
		{"bad-http", `{"code":0,"access_token":"private-readback-token","expires_in":7200}`, "", 403, false},
		{"valid", `{"code":0,"access_token":"private-readback-token","expires_in":7200}`, "", 200, true},
	} {
		t.Run(item.name, func(t *testing.T) {
			var operations atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/oauth/v3/token" {
					w.WriteHeader(item.status)
					_, _ = io.WriteString(w, item.body)
					return
				}
				operations.Add(1)
				_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_fixture"}}`)
			}))
			defer server.Close()
			settings := fixtureSettings(server)
			settings.Profile, settings.TenantToken, settings.TokenExpiresUnix = "client-assertion", "", 0
			settings.ClientAssertion, settings.OAuthBaseURL = "synthetic-assertion", server.URL
			owner, _, inbox := testOwner(t, settings, NativeOptions{})
			result, err := owner.Client().Send(context.Background(), Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t))
			_ = consume(t, inbox).Ack()
			if (err == nil) != item.accepted || (operations.Load() == 1) != item.accepted {
				t.Fatal("OAuth qualification or no-mutation control failed", err)
			}
			if item.message != "" {
				var original *larkcore.CodeError
				if !errors.As(err, &original) || original.Msg != item.message {
					t.Fatal("OAuth native error detail was not preserved")
				}
			}
			if len(result.AuthenticationStages()) != 1 {
				t.Fatal("auxiliary stage lost")
			}
			regressionReadbackPrivacy(t, err, result, result.AuthenticationStages()[0].Err(), result.AuthenticationStages())
		})
	}
}

func TestRegressionReadbackHTTPParseCancellationCause(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected outbound request") }))
	defer server.Close()
	settings := fixtureSettings(server)
	settings.VerificationToken, settings.MaxRequestBytes = "verification", 1<<20
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("private-readback-parse-cancellation")
	entered := make(chan struct{})
	owner, _, inbox := testOwner(t, settings, NativeOptions{WebhookCredentials: func(context.Context) (WebhookCredentials, error) {
		close(entered)
		return WebhookCredentials{AppID: settings.AppID, TenantKey: settings.TenantKey, VerificationToken: settings.VerificationToken}, nil
	}})
	done := make(chan struct{})
	go func() { defer close(done); <-entered; time.Sleep(time.Millisecond); cancel(cause) }()
	body := bytes.Replace(callbackBody("card.action.trigger"), []byte(`"number":`), []byte(`"large":"`+strings.Repeat("x", 900000)+`","number":`), 1)
	_, err := owner.Client().Receive(ctx, Callback{Body: body})
	<-done
	_ = consume(t, inbox).Ack()
	if !errors.Is(err, context.Canceled) {
		t.Fatal("probe did not observe cancellation inside parsing", err)
	}
	if !errors.Is(err, cause) {
		t.Errorf("post-parse cancellation cause lost: err=%v", err)
	}
}

func TestRegressionReadbackLateWriterAndModernResponseValidation(t *testing.T) {
	for _, mode := range []string{"late-writer", "modern-raw", "modern-oversize"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected outbound request") }))
			defer server.Close()
			settings := fixtureSettings(server)
			settings.VerificationToken = "verification"
			settings.Timeout = 15 * time.Millisecond
			owner, _, inbox := testOwner(t, settings, NativeOptions{})
			request := httptest.NewRequest("POST", "https://callback.invalid", bytes.NewReader(callbackBody("card.action.trigger")))
			var handlerContext context.Context
			var writer http.ResponseWriter = httptest.NewRecorder()
			if mode == "late-writer" {
				writer = &regressionReadbackWriter{header: http.Header{}, write: func(body []byte) (int, error) { <-handlerContext.Done(); return len(body), nil }}
			}
			result, err := owner.Client().HandleHTTP(context.Background(), request, writer, false, func(ctx context.Context, _ Event) (HTTPResponse, error) {
				handlerContext = ctx
				body := []byte("{}")
				if mode == "modern-raw" {
					body = []byte("raw-legacy-only")
				}
				if mode == "modern-oversize" {
					body = []byte(`"` + strings.Repeat("x", 30<<10) + `"`)
				}
				return HTTPResponse{Status: 201, Body: body}, nil
			})
			_ = consume(t, inbox).Ack()
			if mode == "late-writer" {
				if !errors.Is(err, context.DeadlineExceeded) || !result.HTTPWrite().Written || result.HTTPWrite().Bytes != 2 {
					t.Fatal("late writer result/cancellation evidence lost", err)
				}
			} else if !errors.Is(err, ErrInput) || result.HTTPWrite().Entered {
				t.Fatal("modern response check was relaxed", err)
			}
		})
	}
}
