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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	larkcard "github.com/larksuite/oapi-sdk-go/v3/card"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
)

type regressionAssertionProvider struct{}

func (regressionAssertionProvider) RetrieveToken(context.Context, string) (*larkcore.Token, error) {
	return &larkcore.Token{Value: "synthetic-assertion"}, nil
}

type regressionQuietLogger struct{}

func (regressionQuietLogger) Debug(context.Context, ...interface{}) {}
func (regressionQuietLogger) Info(context.Context, ...interface{})  {}
func (regressionQuietLogger) Warn(context.Context, ...interface{})  {}
func (regressionQuietLogger) Error(context.Context, ...interface{}) {}

func TestRegressionOAuthNativeDescription(t *testing.T) {
	const description = "synthetic-private-oauth-description"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth/v3/token" {
			t.Errorf("unexpected notification mutation: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"code":20050,"error":"server_error","error_description":%q}`, description)
	}))
	defer server.Close()
	configuration := &larkcore.Config{BaseUrl: server.URL, OAuthBaseUrl: server.URL, AppId: "app_fixture", ClientAssertionProvider: regressionAssertionProvider{}, EnableTokenCache: true,
		HttpClient: server.Client(), Logger: regressionQuietLogger{}, Serializable: &larkcore.DefaultSerialization{}}
	_, originalErr := larkcore.Request(context.Background(), &larkcore.ApiReq{HttpMethod: "POST", ApiPath: "/open-apis/im/v1/messages", SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeTenant}}, configuration)
	var original *larkcore.CodeError
	if !errors.As(originalErr, &original) || original.Msg != description {
		t.Fatalf("native comparison did not preserve its OAuth description: %T %v", originalErr, originalErr)
	}
	settings := fixtureSettings(server)
	settings.Profile, settings.TenantToken, settings.TokenExpiresUnix = "client-assertion", "", 0
	settings.ClientAssertion, settings.OAuthBaseURL = "synthetic-assertion", server.URL
	owner, _, inbox := testOwner(t, settings, NativeOptions{})
	result, err := owner.Client().Send(context.Background(), Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t))
	_ = consume(t, inbox).Ack()
	var retained *larkcore.CodeError
	if !errors.As(err, &retained) {
		t.Fatal("public error omitted native OAuth CodeError")
	}
	if retained.Msg != original.Msg {
		t.Errorf("OAuth native error detail lost: public Code=%d Msg=%q; native Code=%d Msg=%q; stages=%d attempts=%d", retained.Code, retained.Msg, original.Code, original.Msg, len(result.AuthenticationStages()), result.Attempts().Observed)
	}
}

func TestRegressionLegacyRawResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected outbound request") }))
	defer server.Close()
	settings := fixtureSettings(server)
	settings.VerificationToken = "verification"
	owner, _, inbox := testOwner(t, settings, NativeOptions{})
	body := []byte(`{"open_id":"ou_fixture","open_message_id":"om_fixture","tenant_key":"tenant_fixture","token":"update-only","action":{}}`)
	stamp, nonce := strconv.FormatInt(time.Now().Unix(), 10), "nonce"
	digest := sha1.Sum(append([]byte(stamp+nonce+settings.VerificationToken), body...))
	header := http.Header{"X-Lark-Request-Timestamp": {stamp}, "X-Lark-Request-Nonce": {nonce}, "X-Lark-Signature": {hex.EncodeToString(digest[:])}}
	const raw = "native-raw-response"
	nativeHandler := larkcard.NewCardActionHandler(settings.VerificationToken, "", func(context.Context, *larkcard.CardAction) (interface{}, error) { return raw, nil })
	nativeHandler.Config.Logger = regressionQuietLogger{}
	original := nativeHandler.Handle(context.Background(), &larkevent.EventReq{Header: header, Body: body})
	if original.StatusCode != 200 || string(original.Body) != raw {
		t.Fatal("native raw response comparison did not reach success")
	}
	request := httptest.NewRequest("POST", "https://callback.invalid", bytes.NewReader(body))
	request.Header = header
	writer := httptest.NewRecorder()
	result, err := owner.Client().HandleHTTP(context.Background(), request, writer, true, func(context.Context, Event) (HTTPResponse, error) {
		return HTTPResponse{Status: 200, Body: []byte(raw)}, nil
	})
	_ = consume(t, inbox).Ack()
	if err != nil || string(writer.Body.Bytes()) != raw || !result.HTTPWrite().Written {
		t.Errorf("native raw legacy response rejected: err=%v write=%+v body=%q", err, result.HTTPWrite(), writer.Body.String())
	}
}

type regressionCancelWriter struct {
	header http.Header
	cancel context.CancelCauseFunc
	cause  error
}

func (writer *regressionCancelWriter) Header() http.Header { return writer.header }
func (*regressionCancelWriter) WriteHeader(int)            {}
func (writer *regressionCancelWriter) Write(body []byte) (int, error) {
	writer.cancel(writer.cause)
	return len(body), nil
}

func TestRegressionHTTPWriteCancellationCause(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected outbound request") }))
	defer server.Close()
	settings := fixtureSettings(server)
	settings.VerificationToken = "verification"
	owner, _, inbox := testOwner(t, settings, NativeOptions{})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("synthetic-write-cancellation")
	writer := &regressionCancelWriter{header: http.Header{}, cancel: cancel, cause: cause}
	request := httptest.NewRequest("POST", "https://callback.invalid", bytes.NewReader(callbackBody("card.action.trigger")))
	result, err := owner.Client().HandleHTTP(ctx, request, writer, false, func(context.Context, Event) (HTTPResponse, error) {
		return HTTPResponse{Status: 200, Body: []byte("{}")}, nil
	})
	_ = consume(t, inbox).Ack()
	if !result.HTTPWrite().Written {
		t.Fatal("successful local write fact was lost")
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Errorf("cancellation during borrowed write vanished: err=%v write=%+v", err, result.HTTPWrite())
	}
}

func TestRegressionCallbackErrorRetainsCancellation(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected outbound request") }))
	defer server.Close()
	settings := fixtureSettings(server)
	settings.VerificationToken = "verification"
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("synthetic-credential-cancellation")
	hookErr := io.ErrUnexpectedEOF
	owner, _, inbox := testOwner(t, settings, NativeOptions{WebhookCredentials: func(context.Context) (WebhookCredentials, error) {
		cancel(cause)
		return WebhookCredentials{}, hookErr
	}})
	_, err := owner.Client().Receive(ctx, Callback{Body: callbackBody("card.action.trigger")})
	_ = consume(t, inbox).Ack()
	if !errors.Is(err, hookErr) {
		t.Fatal("hook error cause was lost")
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Errorf("cancellation alongside webhook hook error vanished: err=%v", err)
	}
}
