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
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/notification/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	native "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

func fixtureSettings(server *httptest.Server) Settings {
	return Settings{Name: "fixture", Profile: "tenant-token", AppID: "app_fixture", TenantKey: "tenant_fixture", TenantToken: "fixture-token", TokenExpiresUnix: time.Now().Add(time.Hour).Unix(),
		BaseURL: server.URL, RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})),
		MaxActive: 2, QueuedCalls: 1, MaxRequestBytes: 16 << 10, MaxAssetBytes: 16 << 10, MaxResponseBytes: 32 << 10, Timeout: time.Second}
}
func testOwner(t testing.TB, settings Settings, hooks NativeOptions) (*Owner, *adapters.Runtime, *adapters.Inbox[Result]) {
	t.Helper()
	prepared, err := Prepare(settings, hooks)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := prepared.Open(context.Background(), Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error(err)
		}
		if !owner.ShutdownComplete() {
			t.Error("source not released")
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		for {
			state, _ := inbox.Inspect()
			if state.Outstanding == 0 {
				break
			}
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	return owner, runtime, inbox
}
func consume(t testing.TB, inbox *adapters.Inbox[Result]) adapters.Delivery[Result] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func fixtureJSON(t testing.TB, value string) JSON {
	t.Helper()
	result, err := NewJSON([]byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func fixtureText(t testing.TB) Content {
	t.Helper()
	result, err := Text("synthetic")
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestDirectExactFieldsErrorAndEvidenceRetry(t *testing.T) {
	var requests atomic.Int32
	bodies := make(chan []byte, 16)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		data, _ := io.ReadAll(r.Body)
		bodies <- data
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("user_id_type") == "invalid" {
			_, _ = io.WriteString(w, `{"code":999,"msg":"private-native-error","data":{"partial":true}}`)
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_fixture","future":9007199254740993,"false":false,"empty":[],"nil":null}}`)
	}))
	defer server.Close()
	owner, _, inbox := testOwner(t, fixtureSettings(server), NativeOptions{})
	query := url.Values{"receive_id_type": {"user_id"}}
	body := fixtureJSON(t, `{"receive_id":"user_fixture","msg_type":"system","content":"{\"type\":\"divider\",\"number\":9007199254740993}"}`)
	request, err := NewRequest(IMV1MessageCreate, nil, query, body)
	if err != nil {
		t.Fatal(err)
	}
	query["receive_id_type"][0] = "changed"
	result, err := owner.Client().Do(context.Background(), request, RequestOptions{})
	if err != nil || result.Effect() != Accepted || result.MessageID() != "om_fixture" || !bytes.Contains(result.JSONData(), []byte("9007199254740993")) {
		t.Fatal("public native result", err)
	}
	if got := <-bodies; !bytes.Equal(got, body.Bytes()) {
		t.Fatal("public payload changed")
	}
	if !slicesObservation(result.Observations(), notification.PlatformAcknowledged) {
		t.Fatal("platform observation absent")
	}
	delivery := consume(t, inbox)
	receipt, _ := delivery.Receipt()
	before, _ := receipt.Snapshot()
	if err := delivery.Retry(); err != nil {
		t.Fatal(err)
	}
	again := consume(t, inbox)
	receipt, _ = again.Receipt()
	after, _ := receipt.Snapshot()
	if before.Info().Sequence != after.Info().Sequence || requests.Load() != 1 {
		t.Fatal("evidence retry re-executed")
	}
	_ = again.Ack()
	bad, _ := NewRequest(IMV1MessageGet, []string{"om_fixture"}, url.Values{"user_id_type": {"invalid"}}, JSON{})
	result, err = owner.Client().Do(context.Background(), bad, RequestOptions{})
	var original *larkcore.CodeError
	if err == nil || !errors.As(err, &original) || original.Code != 999 || result.Effect() != Rejected {
		t.Fatal("native error cause/effect lost")
	}
	if strings.Contains(fmt.Sprintf("%+v", err), "private-native-error") {
		t.Fatal("error leaked")
	}
	_ = consume(t, inbox).Ack()
}
func slicesObservation(values []notification.Observation, want notification.Observation) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestPublicPreparationPrivacyAndCatalog(t *testing.T) {
	settings := Settings{Name: "fixture", Profile: "application", AppID: "app_fixture", AppSecret: "private-canary"}
	var invoked atomic.Int32
	hooks := NativeOptions{Credentials: func(context.Context, CredentialRequest) (Credentials, error) {
		invoked.Add(1)
		return Credentials{}, nil
	}}
	prepared, err := Prepare(settings, hooks)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := native.PrepareV1(options(settings, hooks))
	if err != nil {
		t.Fatal(err)
	}
	if prepared.metadata != expected.Metadata() || invoked.Load() != 0 {
		t.Fatal("preparation diverged or callback ran")
	}
	if _, err := Recommend(settings); err != nil {
		t.Fatal(err)
	}
	if err := Validate(settings); err != nil {
		t.Fatal(err)
	}
	if _, err := failure.Prepare(Definitions()...); err != nil {
		t.Fatal(err)
	}
	if _, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "notification_lark", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()}); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("private-canary")
	public := fail(ErrInput, "fixture", cause)
	for _, input := range []error{public, fmt.Errorf("private-canary: %w", public), errors.Join(public, fail(ErrState, "state"))} {
		mapped := translate(input, "outer")
		if !errors.Is(mapped, cause) || !errors.Is(mapped, ErrInput) || strings.Contains(fmt.Sprintf("%+v", mapped), "private-canary") {
			t.Fatal("public classification/privacy")
		}
	}
	mapped := translate(native.ErrAuth.New(fault.Context{}, cause), "authenticate")
	if !errors.Is(mapped, cause) {
		t.Fatal("native cause lost")
	}
	var log strings.Builder
	slog.New(slog.NewTextHandler(&log, nil)).Error("fixture", "error", mapped, "settings", &settings)
	if strings.Contains(log.String(), "private-canary") {
		t.Fatal("slog leaked")
	}
	for _, value := range []any{Result{}, JSON{}, Content{}, Request{}, RequestOptions{}, AssetInput{}, Callback{}, HTTPResponse{}, Incoming{}, NativeOptions{}, Credentials{}, WebhookCredentials{}, new(Owner), new(Client)} {
		if _, err := json.Marshal(value); err == nil {
			t.Fatalf("runtime serialized: %T", value)
		}
	}
}
func TestPublicEvidenceSaturationRefusesBeforeWire(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_fixture"}}`)
	}))
	defer server.Close()
	owner, _, inbox := testOwner(t, fixtureSettings(server), NativeOptions{})
	options, _ := inbox.Options()
	for index := 1; index < options.Capacity; index++ {
		if _, err := owner.Client().Send(context.Background(), Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t)); err != nil {
			t.Fatal(err)
		}
	}
	before := count.Load()
	if _, err := owner.Client().Send(context.Background(), Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t)); !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("saturation not rejected", err)
	}
	if count.Load() != before {
		t.Fatal("saturated public work reached native")
	}
	_ = consume(t, inbox).Ack()
	if _, err := owner.Client().Send(context.Background(), Recipient{Type: "open_id", ID: "ou_fixture"}, "", fixtureText(t)); err != nil {
		t.Fatal("capacity did not recover", err)
	}
}

type heldReader struct {
	entered, release chan struct{}
	once             sync.Once
}

func (reader *heldReader) Read([]byte) (int, error) {
	reader.once.Do(func() { close(reader.entered) })
	<-reader.release
	return 0, io.EOF
}
func TestCooperativeReaderRetainsSourceAfterCanceledClose(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("canceled read reached HTTP") }))
	defer server.Close()
	owner, _, _ := testOwner(t, fixtureSettings(server), NativeOptions{})
	reader := &heldReader{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(reader.release) })
	defer release()
	done := make(chan error, 1)
	go func() {
		_, err := owner.Client().UploadAsset(context.Background(), false, AssetInput{Name: "fixture", FileType: "stream", Reader: reader}, RequestOptions{})
		done <- err
	}()
	select {
	case <-reader.entered:
	case <-time.After(time.Second):
		t.Fatal("reader not entered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := owner.Close(ctx); !errors.Is(err, context.DeadlineExceeded) || owner.ShutdownComplete() {
		t.Fatal("early close released reader", err)
	}
	release()
	if err := <-done; err == nil {
		t.Fatal("canceled read accepted")
	}
	if err := owner.Close(context.Background()); err != nil || !owner.ShutdownComplete() {
		t.Fatal("cleanup continuation failed", err)
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal("repeat close failed", err)
	}
}
