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
	"github.com/frost-leo/fathomry/internal/fault"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

type noProgressReader struct{}

func (noProgressReader) Read([]byte) (int, error) { return 0, nil }

type cyclicCredentialError struct{}

func (*cyclicCredentialError) Error() string       { return "private-cycle" }
func (value *cyclicCredentialError) Unwrap() error { return value }

func TestCooperativeReadDeadlineAndCredentialCauseCycle(t *testing.T) {
	peer := newPeer(t, nil)
	options := testOptions(peer)
	options.Timeout = 20 * time.Millisecond
	bound := bindTest(t, options, 1)
	receipt, err := bound.client.UploadAsset(context.Background(), fault.Correlation{Call: "no-progress"}, false, AssetInput{Name: "fixture", FileType: "stream", Reader: noProgressReader{}}, RequestOptions{})
	result := resolved(t, receipt, err)
	if !errors.Is(result.Err(), context.DeadlineExceeded) || result.Outcome.Value.Effect() != NotAttempted || len(peer.snapshot()) != 0 {
		t.Fatal("reader ignored execution deadline", result.Err())
	}
	if socketRetryable(failure(ErrAuth, "credentials", &cyclicCredentialError{})) {
		t.Fatal("failed credentials enabled reconnect")
	}
}

func TestClientAssertionUserTokensAndDownloadMetadata(t *testing.T) {
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/v3/token" {
			var value map[string]string
			if json.NewDecoder(r.Body).Decode(&value) != nil || value["client_assertion"] != "assertion-fixture" || value["client_id"] != "app_fixture" || value["grant_type"] != "urn:ietf:params:oauth:grant-type:jwt-bearer" || r.Header.Get("Authorization") != "" {
				t.Error("assertion authority changed")
			}
			_, _ = io.WriteString(w, `{"code":0,"access_token":"assertion-token","expires_in":7200}`)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/open-apis/im/v1/files/") {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Disposition", `attachment; filename*=UTF-8''fixture%20file.csv`)
			w.Header().Add("X-Fixture", "one")
			w.Header().Add("X-Fixture", "two")
			_, _ = io.WriteString(w, "fixture")
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_fixture","items":[]}}`)
	})
	options := testOptions(peer)
	options.Profile = "client-assertion"
	options.AppSecret = ""
	options.TenantKey = "tenant_fixture"
	options.ClientAssertion = "assertion-fixture"
	options.OAuthBaseURL = peer.server.URL
	bound := bindTest(t, options, 1)
	receipt, err := bound.client.Send(context.Background(), fault.Correlation{Call: "assertion"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "", textContent(t))
	result := resolved(t, receipt, err)
	if result.Err() != nil || result.Attempts.Observed != 2 {
		t.Fatal("client assertion acquisition", result.Err())
	}
	stages := result.Outcome.Value.AuthenticationStages()
	if len(stages) != 1 || stages[0].Name() != "client-assertion" || stages[0].Effect() != Accepted {
		t.Fatal("assertion stage missing")
	}
	drain(t, bound.inbox)
	receipt, err = bound.client.DownloadFile(context.Background(), fault.Correlation{Call: "download"}, "file_fixture")
	result = resolved(t, receipt, err)
	if result.Err() != nil || result.Outcome.Value.FileName() != "fixture file.csv" || !bytes.Equal(result.Outcome.Value.Bytes(), []byte("fixture")) {
		t.Fatal("download native metadata lost", result.Err())
	}
	headers := result.Outcome.Value.Headers()
	headers["X-Fixture"][0] = "changed"
	if result.Outcome.Value.Headers().Values("X-Fixture")[0] != "one" {
		t.Fatal("response header alias")
	}
	other := testOptions(peer)
	other.Profile = "user-token"
	other.AppSecret = ""
	other.UserToken = "user-fixture"
	other.TenantKey = "tenant_fixture"
	other.TokenExpiresAt = time.Now().Add(time.Hour)
	user := bindTest(t, other, 1)
	request, _ := NewRequest(IMV1MessageGet, []string{"om_fixture"}, url.Values{"user_id_type": {"user_id"}}, JSON{})
	receipt, err = user.client.Do(context.Background(), fault.Correlation{Call: "user"}, request, RequestOptions{})
	result = resolved(t, receipt, err)
	if result.Err() != nil || result.Attempts.Observed != 1 || len(result.Outcome.Value.AuthenticationStages()) != 0 {
		t.Fatal("external user token was mislabeled acquisition", result.Err())
	}
	calls := peer.snapshot()
	if calls[len(calls)-1].auth != "Bearer user-fixture" {
		t.Fatal("user token substituted")
	}
	drain(t, user.inbox)
	request, _ = NewRequest(IMV1BatchMessageGetProgress, []string{"bm_fixture"}, nil, JSON{})
	if _, err := user.client.Do(context.Background(), fault.Correlation{Call: "tenant-only"}, request, RequestOptions{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("tenant-only route accepted user authority", err)
	}
	if _, err := user.client.UpdateMessage(context.Background(), fault.Correlation{Call: "convenience"}, "om_fixture", textContent(t)); !errors.Is(err, ErrUnsupported) {
		t.Fatal("convenience bypassed native token policy", err)
	}
	if defaults(OptionsV1{Profile: "client-assertion"}).OAuthBaseURL != "https://accounts.feishu.cn" {
		t.Fatal("OAuth default diverged from SDK")
	}
}
