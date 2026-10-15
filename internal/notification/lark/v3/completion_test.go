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
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
)

// The oracle reads the actual selected SDK, not the implementation's own table.
func TestPinnedNotificationCatalogMatchesNativeSource(t *testing.T) {
	cmd := exec.Command("go", "list", "-m", "-json", "github.com/larksuite/oapi-sdk-go/v3")
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var module struct{ Dir, Version, Sum string }
	if json.Unmarshal(output, &module) != nil || module.Version != "v3.12.1" || module.Sum != "h1:5Zu2z+meHR/ETyTSWOB6gyRN0FIiImXdq/ReQGlGYH8=" {
		t.Fatal("wrong SDK identity")
	}
	methods := regexp.MustCompile(`(?s)func \(\w+ \*(\w+)\) (\w+)\(ctx context.Context, req \*(\w+), options .*?\n\}`)
	pathRE := regexp.MustCompile(`apiReq.ApiPath = "([^"]+)"`)
	verbRE := regexp.MustCompile(`apiReq.HttpMethod = http.Method(\w+)`)
	queryRE := regexp.MustCompile(`QueryParams\.(?:Set|Add)\("([^"]+)"`)
	seen := map[Operation]bool{}
	for _, pkg := range []string{"im/v1", "im/v2", "cardkit/v1", "contact/v3"} {
		source, err := os.ReadFile(filepath.Join(module.Dir, "service", pkg, "resource.go"))
		if err != nil {
			t.Fatal(err)
		}
		model, err := os.ReadFile(filepath.Join(module.Dir, "service", pkg, "model.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, method := range methods.FindAllStringSubmatch(string(source), -1) {
			if pkg == "contact/v3" && method[3] != "BatchGetIdUserReq" {
				continue
			}
			path := pathRE.FindStringSubmatch(method[0])
			if path == nil {
				continue
			}
			key := Operation(strings.ReplaceAll(pkg, "/", ".") + "." + method[1] + "." + method[2])
			spec, ok := operations[key]
			if !ok || spec.path != path[1] || spec.method != strings.ToUpper(verbRE.FindStringSubmatch(method[0])[1]) {
				t.Fatalf("native route absent/different: %s", key)
			}
			if spec.user != strings.Contains(method[0], "larkcore.AccessTokenTypeUser") || spec.tenant != strings.Contains(method[0], "larkcore.AccessTokenTypeTenant") {
				t.Fatalf("native token policy: %s", key)
			}
			if spec.download != strings.Contains(method[0], "resp.File = bytes.NewBuffer") || spec.upload != strings.Contains(method[0], "WithFileUpload()") {
				t.Fatalf("native transfer shape differs: %s", key)
			}
			builder := regexp.MustCompile(`(?s)func \(builder \*` + method[3] + `Builder\).*?\n\}`)
			var queries []string
			for _, function := range builder.FindAllString(string(model), -1) {
				for _, field := range queryRE.FindAllStringSubmatch(function, -1) {
					if !slices.Contains(queries, field[1]) {
						queries = append(queries, field[1])
					}
				}
			}
			if !slices.Equal(queries, spec.query) {
				t.Fatalf("native query options differ: %s", key)
			}
			seen[key] = true
		}
	}
	if len(seen) != 85 || len(operations) != len(seen)+2 {
		t.Fatalf("closed native inventory changed: %d/%d", len(seen), len(operations))
	}
}

func TestEveryCatalogRouteOnIndependentTLSWire(t *testing.T) {
	var selected operationSpec
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && (strings.HasPrefix(r.URL.Path, "/open-apis/im/v1/files/") || strings.HasPrefix(r.URL.Path, "/open-apis/im/v1/images/") || strings.Contains(r.URL.Path, "/resources/")) {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.WriteString(w, "binary-fixture")
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"extra":9007199254740993,"bot":{"open_id":"ou_bot"},"data":{"message_id":"om_result","chat_id":"oc_fixture","card_id":"card_result","reaction_id":"reaction_result","message":{"message_id":"om_merge"},"invalid_message_id_list":[],"invalid_user_id_list":[],"large":9007199254740993,"null":null,"empty":[],"false":false,"zero":0}}`)
	})
	options := testOptions(peer)
	options.Profile = "tenant-token"
	options.AppSecret = ""
	options.TenantKey = "tenant_fixture"
	options.TenantToken = "token-fixture"
	options.TokenExpiresAt = time.Now().Add(time.Hour)
	bound := bindTest(t, options, 1)
	keys := make([]string, 0, len(operations))
	for key := range operations {
		keys = append(keys, string(key))
	}
	slices.Sort(keys)
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			t.Cleanup(func() { drain(t, bound.inbox) })
			operation := Operation(key)
			selected = operations[operation]
			if selected.upload {
				image := operation == IMV1ImageCreate
				input := AssetInput{Name: "fixture.dat", Bytes: []byte("payload")}
				if image {
					input.ImageType = "avatar"
				} else {
					input.FileType = "stream"
					input.Duration = pointer(0)
				}
				receipt, err := bound.client.UploadAsset(context.Background(), fault.Correlation{Call: key}, image, input, RequestOptions{})
				result := resolved(t, receipt, err)
				// Independent response above lacks the required upload key; absence must fail.
				if !errors.Is(result.Err(), ErrResponse) || result.Outcome.Value.Effect() != Unknown {
					t.Fatal("missing upload identity accepted", result.Err())
				}
				drain(t, bound.inbox)
				return
			}
			parts := []string{}
			for _, part := range strings.Split(selected.path, "/") {
				if strings.HasPrefix(part, ":") {
					parts = append(parts, "id_fixture")
				}
			}
			query := url.Values{}
			for _, key := range selected.query {
				query[key] = []string{"0", ""}
			}
			body := jsonValue(t, `{"message_id_list":["om_a"],"msg_type":"text","content":{"text":"synthetic"},"open_ids":["ou_a"],"large":9007199254740993,"nil":null,"empty":[],"zero":0,"false":false}`)
			if selected.method == "GET" {
				body = JSON{}
			}
			request, err := NewRequest(operation, parts, query, body)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := bound.client.Do(context.Background(), fault.Correlation{Call: key}, request, RequestOptions{RequestID: "fixture-id", Headers: http.Header{"X-Fixture": {"one", "two"}}})
			result := resolved(t, receipt, err)
			if result.Err() != nil || result.Outcome.Value.Effect() != Accepted {
				t.Fatal("catalog execution", result.Err())
			}
			captured := peer.snapshot()
			last := captured[len(captured)-1]
			wantPath := selected.path
			for _, part := range strings.Split(selected.path, "/") {
				if strings.HasPrefix(part, ":") {
					wantPath = strings.Replace(wantPath, part, "id_fixture", 1)
				}
			}
			if last.path != wantPath || last.method != selected.method || last.query != query.Encode() || last.auth != "Bearer token-fixture" {
				t.Fatal("native route/query/auth changed")
			}
			if body.value != "" && !bytes.Equal(last.body, body.Bytes()) {
				t.Fatal("body representation changed")
			}
			if !selected.download && !bytes.Contains(result.Outcome.Value.ResponseJSON(), []byte("9007199254740993")) {
				t.Fatal("response precision/fields lost")
			}
			drain(t, bound.inbox)
		})
	}
}
func TestMarketplaceAuxiliaryStagesAndNoMutationRetry(t *testing.T) {
	for _, failed := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			var requests atomic.Int32
			peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
				count := int(requests.Add(1))
				if count == failed {
					_, _ = io.WriteString(w, `{"code":99991663,"msg":"private-auth-canary"}`)
					return
				}
				switch r.URL.Path {
				case "/open-apis/auth/v3/app_access_token":
					var input map[string]string
					_ = json.NewDecoder(r.Body).Decode(&input)
					if input["app_ticket"] != "ticket-fixture" || input["app_secret"] != "secret-app_fixture" || input["app_id"] != "app_fixture" {
						t.Error("mixed credentials")
					}
					_, _ = io.WriteString(w, `{"code":0,"app_access_token":"app-token","expire":7200}`)
				case "/open-apis/auth/v3/tenant_access_token":
					var input map[string]string
					_ = json.NewDecoder(r.Body).Decode(&input)
					if input["app_access_token"] != "app-token" || input["tenant_key"] != "tenant_fixture" {
						t.Error("wrong tenant tuple")
					}
					_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"tenant-token","expire":7200}`)
				default:
					if r.Header.Get("Authorization") != "Bearer tenant-token" {
						t.Error("wrong operation token")
					}
					_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_result"}}`)
				}
			})
			options := testOptions(peer)
			options.Profile = "marketplace"
			options.AppTicket = "ticket-fixture"
			options.TenantKey = "tenant_fixture"
			bound := bindTest(t, options, 1)
			receipt, err := bound.client.Send(context.Background(), fault.Correlation{Call: "cold"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "", textContent(t))
			got := resolved(t, receipt, err)
			attempts := 3
			if failed > 0 {
				attempts = failed
			}
			if int(requests.Load()) != attempts || got.Attempts.Observed != uint64(attempts) || !got.Attempts.Exact {
				t.Fatal("auxiliary accounting", got.Attempts)
			}
			stages := got.Outcome.Value.AuthenticationStages()
			if len(stages) != min(attempts, 2) {
				t.Fatal("auxiliary evidence lost")
			}
			if failed == 0 && got.Err() != nil || failed > 0 && got.Err() == nil {
				t.Fatal("failure not preserved", got.Err())
			}
			if failed > 0 && failed < 3 && got.Outcome.Value.Effect() != NotAttempted {
				t.Fatal("auth traffic became message effect")
			}
			if strings.Contains(fmt.Sprint(got.Err()), "private-auth-canary") {
				t.Fatal("native diagnostic leak")
			}
		})
	}
}

func TestDynamicCredentialsOfflineAndFailClosed(t *testing.T) {
	for _, mode := range []string{"empty", "error", "panic", "canceled", "identity", "valid"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			peer := newPeer(t, nil)
			options := testOptions(peer)
			options.Native.Credentials = func(ctx context.Context, _ CredentialRequest) (Credentials, error) {
				calls.Add(1)
				switch mode {
				case "empty":
					return Credentials{}, nil
				case "error":
					return Credentials{}, errors.New("private-provider")
				case "panic":
					panic(errors.New("private-provider"))
				case "canceled":
					<-ctx.Done()
					return Credentials{AppID: options.AppID, AppSecret: options.AppSecret}, nil
				case "identity":
					return Credentials{AppID: "other", AppSecret: options.AppSecret}, nil
				default:
					return Credentials{AppID: options.AppID, AppSecret: options.AppSecret}, nil
				}
			}
			if _, err := PrepareV1(options); err != nil || calls.Load() != 0 {
				t.Fatal("preparation invoked callback", err)
			}
			bound := bindTest(t, options, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			receipt, err := bound.client.Send(ctx, fault.Correlation{Call: "credential"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "", textContent(t))
			got := resolved(t, receipt, err)
			if mode == "valid" {
				if got.Err() != nil {
					t.Fatal(got.Err())
				}
			} else if got.Err() == nil || len(peer.snapshot()) != 0 || got.Outcome.Value.Effect() != NotAttempted {
				t.Fatal("credential failure opened authority")
			}
		})
	}
}
func TestOptionalUUIDPresentationAndPartialNamespaces(t *testing.T) {
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/internal") {
			defaultReply(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "urgent_app") {
			_, _ = io.WriteString(w, `{"code":0,"data":{"invalid_user_id_list":["ou_bad"]}}`)
			return
		}
		if strings.Contains(r.URL.Path, "batch_send") {
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"bm_fixture","invalid_user_ids":["bad"],"invalid_open_ids":[],"invalid_union_ids":null}}`)
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_result"}}`)
	})
	bound := bindTest(t, testOptions(peer), 1)
	receipt, err := bound.client.Send(context.Background(), fault.Correlation{Call: "uuid"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "", textContent(t))
	if got := resolved(t, receipt, err); got.Err() != nil {
		t.Fatal(got.Err())
	}
	last := peer.snapshot()
	if bytes.Contains(last[len(last)-1].body, []byte("uuid")) {
		t.Fatal("optional UUID synthesized")
	}
	drain(t, bound.inbox)
	for _, operation := range []Operation{IMV1MessageUrgentApp, LegacyBatchSend} {
		path := []string{"om_fixture"}
		body := jsonValue(t, `{"user_id_list":["ou_bad"]}`)
		if operation == LegacyBatchSend {
			path = nil
			body = jsonValue(t, `{"msg_type":"text","content":{"text":"fixture"},"user_ids":["bad"]}`)
		}
		request, err := NewRequest(operation, path, nil, body)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := bound.client.Do(context.Background(), fault.Correlation{Call: string(operation)}, request, RequestOptions{})
		result := resolved(t, receipt, err)
		if result.Err() != nil || result.Outcome.Value.Effect() != Partial || len(result.Outcome.Value.InvalidMessageIDs()) != 0 {
			t.Fatal("partial namespaces conflated", result.Err())
		}
		if operation == LegacyBatchSend {
			targets := result.Outcome.Value.InvalidTargets()
			if targets["invalid_open_ids"] == nil || targets["invalid_union_ids"] != nil {
				t.Fatal("empty/null lost")
			}
		} else if len(result.Outcome.Value.InvalidUserIDs()) != 1 {
			t.Fatal("invalid recipients lost")
		}
		drain(t, bound.inbox)
	}
	card, err := ComposeCard("fixture", jsonValue(t, `{"tag":"markdown","content":"fixture"}`))
	if err != nil || bytes.Contains(card.JSON().Bytes(), []byte("blue")) || bytes.Contains(card.JSON().Bytes(), []byte("update_multi")) {
		t.Fatal("presentation default remains")
	}
	explicit, err := ComposeCardWithOptions("fixture", CardOptions{HeaderTemplate: "blue", UpdateMulti: pointer(false)}, jsonValue(t, `{"tag":"markdown","content":"fixture"}`))
	if err != nil || !bytes.Contains(explicit.JSON().Bytes(), []byte(`"update_multi":false`)) {
		t.Fatal("explicit false lost")
	}
}
