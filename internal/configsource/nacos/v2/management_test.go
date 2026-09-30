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

package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConfigurationManagement(t *testing.T) {
	fixture := newFixture(t, false)
	options := fixture.options()
	options.Writable = true
	options.DynamicKeys = true
	client := openClient(t, options)
	selected := KeyV1{Group: "managed", DataID: "dynamic.toml"}
	result, err := client.Publish(context.Background(), PublishInputV1{Key: selected, Content: "value=1", ContentType: "toml"})
	if err != nil || result.State() != MutationAcknowledged {
		t.Fatal("native publication failed", err)
	}
	document, err := client.Read(context.Background(), selected)
	if err != nil || string(document.RawCopy()) != "value=1" {
		t.Fatal("dynamic read failed", err)
	}
	result, err = client.Publish(context.Background(), PublishInputV1{Key: selected, Content: "value=2", CASMD5: document.MD5()})
	if err != nil || result.State() != MutationAcknowledged {
		t.Fatal("CAS update failed", err)
	}
	result, err = client.Publish(context.Background(), PublishInputV1{Key: selected, Content: "value=3", CASMD5: document.MD5()})
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.ErrorCode() != 409 || result.State() != MutationRejected {
		t.Fatal("CAS refusal lost native evidence")
	}
	before := fixture.mutations.Load()
	fixture.mu.Lock()
	fixture.mutationError = status.Error(codes.Unavailable, "lost response")
	fixture.mu.Unlock()
	result, err = client.Publish(context.Background(), PublishInputV1{Key: selected, Content: "value=4"})
	if err == nil || result.State() != MutationUnknown || fixture.mutations.Load() != before+1 {
		t.Fatal("ambiguous mutation retried or reported no effect")
	}
	fixture.mu.Lock()
	fixture.mutationError = nil
	fixture.mutationReply = encoded(&response.ErrorResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 500}})
	fixture.mu.Unlock()
	result, err = client.Delete(context.Background(), selected)
	if err == nil || result.State() != MutationUnknown {
		t.Fatal("generic server failure claimed rollback")
	}
	fixture.mu.Lock()
	fixture.mutationReply = nil
	fixture.mu.Unlock()
	result, err = client.Delete(context.Background(), selected)
	if err != nil || result.State() != MutationAcknowledged {
		t.Fatal(err)
	}
	missing, err := client.ReadRaw(context.Background(), selected)
	if err != nil || !missing.Missing() {
		t.Fatal("raw dynamic absence lost", err)
	}

	readonly := openClient(t, fixture.options())
	result, err = readonly.Delete(context.Background(), KeyV1{DataID: "settings.yaml"})
	if !errors.Is(err, ErrInput) || result.State() != MutationNotIssued {
		t.Fatal("readonly mutation dispatched")
	}
	if _, err := readonly.Read(context.Background(), selected); !errors.Is(err, ErrInput) {
		t.Fatal("fixed key scope broadened")
	}
	subscription, err := client.WatchKeys(context.Background(), []KeyV1{selected})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close(context.Background())
	wait, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	change, err := subscription.Next(wait)
	if err != nil || !change.Resync() {
		t.Fatal("dynamic watch did not establish scope", err)
	}
}

func TestSearchPages(t *testing.T) {
	for _, v3 := range []bool{false, true} {
		t.Run(map[bool]string{false: "v1", true: "v3"}[v3], func(t *testing.T) {
			fixture := newFixture(t, false)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/nacos/v1/auth/users/login" {
					json.NewEncoder(writer).Encode(map[string]any{"accessToken": "search-token", "tokenTtl": 100})
					return
				}
				if request.URL.Query().Get("accessToken") != "search-token" {
					writer.WriteHeader(403)
					return
				}
				if v3 && request.URL.Path == "/nacos/v1/cs/configs" {
					writer.WriteHeader(404)
					return
				}
				tagParameter := "config_tags"
				if v3 {
					tagParameter = "configTags"
				}
				if request.URL.Query().Get(tagParameter) != "selected-tag" {
					t.Error("search tags omitted or mapped to ignored native parameter")
				}
				page := model.ConfigPage{TotalCount: 1, PageNumber: 1, PagesAvailable: 1, PageItems: []model.ConfigItem{
					{Id: "1", DataId: "target", Group: "group", Content: "value", Md5: checksum("value"), Tenant: fixture.options().Namespace},
				}}
				if v3 {
					json.NewEncoder(writer).Encode(map[string]any{"code": 0, "data": map[string]any{"totalCount": 1, "pageNumber": 1, "pagesAvailable": 1,
						"pageItems": []any{map[string]any{"id": 1, "dataId": "target", "groupName": "group", "namespaceId": fixture.options().Namespace, "md5": checksum("value")}}}})
				} else {
					json.NewEncoder(writer).Encode(page)
				}
			}))
			defer server.Close()
			options := fixture.options()
			options.DynamicKeys = true
			options.Username, options.Password = "reader", "credential-canary"
			options.Servers[0].HTTPURL = server.URL + "/nacos"
			client := openClient(t, options)
			page, err := client.Search(context.Background(), SearchInputV1{Mode: "accurate", DataID: "target", Group: "group", ConfigTags: "selected-tag"})
			if err != nil || page.Total() != 1 || len(page.ItemsCopy()) != 1 {
				t.Fatal("native search failed", err)
			}
			items := page.ItemsCopy()
			items[0].content = "changed"
			if v3 && (page.ItemsCopy()[0].ContentPresent() || page.ItemsCopy()[0].RawCopy() != nil) {
				t.Fatal("metadata-only listing fabricated empty content")
			}
			if !v3 && string(page.ItemsCopy()[0].RawCopy()) != "value" {
				t.Fatal("search storage escaped")
			}
		})
	}
}

func TestMutationBoundaries(t *testing.T) {
	fixture := newFixture(t, false)
	second := newFixture(t, false)
	options := fixture.options()
	options.Writable = true
	options.Servers = append(options.Servers, second.options().Servers...)
	client := openClient(t, options)
	input := PublishInputV1{Key: options.Keys[0], Content: "value: admitted"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := client.Publish(ctx, input)
	if !errors.Is(err, context.Canceled) || result.State() != MutationNotIssued || fixture.mutations.Load() != 0 {
		t.Fatal("canceled admission dispatched a mutation")
	}
	for _, input := range []PublishInputV1{
		{Key: options.Keys[0]},
		{Key: options.Keys[0], Content: strings.Repeat("x", MaxDocumentBytes+1)},
		{Key: options.Keys[0], Content: "value", CASMD5: "invalid"},
	} {
		result, err := client.Publish(context.Background(), input)
		if !errors.Is(err, ErrInput) || result.State() != MutationNotIssued {
			t.Fatal("invalid publication crossed admission")
		}
	}
	for _, code := range []int{301, 403, 409, 500} {
		fixture.mu.Lock()
		fixture.mutationReply = encoded(&response.ErrorResponse{Response: &response.Response{ResultCode: 500, ErrorCode: code}})
		fixture.mu.Unlock()
		before := fixture.mutations.Load()
		result, err := client.Publish(context.Background(), input)
		want := MutationRejected
		if code == 500 {
			want = MutationUnknown
		}
		if err == nil || result.State() != want || fixture.mutations.Load() != before+1 || second.mutations.Load() != 0 {
			t.Fatal("post-dispatch rejection/failure replayed or effect evidence changed")
		}
	}
	fixture.mu.Lock()
	fixture.mutationReply = nil
	fixture.mutationBlock = true
	fixture.mu.Unlock()
	work, stop := context.WithCancelCause(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); result, err = client.Publish(work, input) }()
	select {
	case <-fixture.entered:
	case <-time.After(2 * time.Second):
		stop(nil)
		t.Fatal("mutation did not reach fixture")
	}
	cause := &RemoteError{errorCode: 409}
	stop(cause)
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("mutation cancellation did not join")
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || result.State() != MutationUnknown || second.mutations.Load() != 0 {
		t.Fatal("caller cause manufactured rejection or mutation replay")
	}
}

func TestDynamicObservationAndPrivacy(t *testing.T) {
	fixture := newFixture(t, false)
	options := fixture.options()
	options.Keys = nil
	options.DynamicKeys = true
	client := openClient(t, options)
	if _, err := client.ReadAll(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal("empty default batch admitted")
	}
	if _, err := client.Watch(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal("empty default watch admitted")
	}
	selected := []KeyV1{{DataID: "settings.yaml"}}
	batches := make(chan []*Document, 1)
	subscription, err := client.ObserveRawKeys(context.Background(), selected, func(documents []*Document, _ int, err error) {
		if err == nil {
			select {
			case batches <- documents:
			default:
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close(context.Background())
	selected[0].DataID = "mutated"
	select {
	case documents := <-batches:
		if len(documents) != 1 || documents[0].Key().DataID != "settings.yaml" {
			t.Fatal("selected-key storage escaped")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dynamic raw observation did not publish")
	}
	for _, keys := range [][]KeyV1{nil, {{DataID: "same"}, {DataID: "same"}}} {
		if _, err := client.WatchKeys(context.Background(), keys); !errors.Is(err, ErrInput) {
			t.Fatal("invalid key set admitted")
		}
	}
	publication := PublishInputV1{Content: "publication-canary"}
	search := SearchInputV1{DataID: "search-canary"}
	item := SearchItem{content: "content-canary"}
	page := &SearchPage{items: []SearchItem{item}}
	conformance.Runtime(t, publication, new(PublishInputV1), "publication-canary")
	conformance.Runtime(t, &publication, new(PublishInputV1), "publication-canary")
	conformance.Runtime(t, search, new(SearchInputV1), "search-canary")
	conformance.Runtime(t, item, new(SearchItem), "content-canary")
	conformance.Runtime(t, page, new(SearchPage), "content-canary")
	conformance.Runtime(t, MutationResult{state: MutationUnknown}, new(MutationResult))
}

func TestSearchDenialDoesNotFallBackOrFailOver(t *testing.T) {
	for _, envelope := range []bool{false, true} {
		t.Run(map[bool]string{false: "http", true: "v3_envelope"}[envelope], func(t *testing.T) {
			fixture := newFixture(t, false)
			var legacy, modern, other atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/nacos/v1/auth/users/login":
					_ = json.NewEncoder(writer).Encode(map[string]any{"accessToken": "token", "tokenTtl": 100})
				case "/nacos/v1/cs/configs":
					legacy.Add(1)
					if envelope {
						writer.WriteHeader(404)
					} else {
						writer.WriteHeader(403)
					}
				default:
					modern.Add(1)
					_, _ = writer.Write([]byte(`{"code":403,"message":"denied-canary"}`))
				}
			}))
			defer server.Close()
			second := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { other.Add(1); writer.WriteHeader(500) }))
			defer second.Close()
			options := fixture.options()
			options.DynamicKeys = true
			options.Username, options.Password = "reader", "credential-canary"
			options.Servers[0].HTTPURL = server.URL + "/nacos"
			options.Servers = append(options.Servers, ServerV1{HTTPURL: second.URL + "/nacos", GRPCAddress: "127.0.0.1:1"})
			client := openClient(t, options)
			if page, err := client.Search(context.Background(), SearchInputV1{Mode: "accurate"}); page != nil || !errors.Is(err, ErrDenied) {
				t.Fatal("native denial was not preserved", err)
			}
			client.mu.Lock()
			cached := client.tokens[0].value
			client.mu.Unlock()
			if cached != "" || legacy.Load() != 1 || !envelope && modern.Load() != 0 || envelope && modern.Load() != 1 || other.Load() != 0 {
				t.Fatal("denial retained token or broadened request route")
			}
		})
	}
}

func TestSearchMalformedPages(t *testing.T) {
	input := SearchInputV1{Mode: "accurate", DataID: "target", Group: "group", Page: 1, PageSize: 1}
	for _, raw := range []string{
		`{}`, `{"pageNumber":1}`, `{"totalCount":0,"pageNumber":2,"pagesAvailable":0}`,
		`{"totalCount":0,"totalCount":1,"pageNumber":1,"pagesAvailable":0}`,
		`{"totalCount":1,"pageNumber":1,"pagesAvailable":1,"pageItems":[{"dataId":"other","group":"group"}]}`,
		`{"totalCount":1,"pageNumber":1,"pagesAvailable":1,"pageItems":[{"dataId":"target","group":"group","tenant":"other"}]}`,
		`{"totalCount":1,"pageNumber":1,"pagesAvailable":1,"pageItems":[{"dataId":"target","group":"group","content":"value","md5":"invalid"}]}`,
	} {
		if page, err := decodeSearchPage([]byte(raw), input, "", false); page != nil || !errors.Is(err, ErrDecode) {
			t.Fatal("malformed or cross-scope search admitted")
		}
	}
	if page, err := decodeSearchPage([]byte(`{"data":{"totalCount":0,"pageNumber":1,"pagesAvailable":0}}`), input, "", true); page != nil || !errors.Is(err, ErrDecode) {
		t.Fatal("missing v3 result code manufactured success")
	}
}

func FuzzSearchPage(f *testing.F) {
	f.Add([]byte(`{"totalCount":1,"pageNumber":1,"pagesAvailable":1,"pageItems":[{"id":"1","dataId":"target","group":"group","content":"value"}]}`), false)
	f.Add([]byte(`{"code":0,"data":{"totalCount":0,"pageNumber":1,"pagesAvailable":0,"pageItems":[]}}`), true)
	f.Fuzz(func(t *testing.T, raw []byte, modern bool) {
		if len(raw) > 64<<10 {
			return
		}
		page, err := decodeSearchPage(raw, SearchInputV1{Mode: "blur", Page: 1, PageSize: 10}, "", modern)
		if err != nil && page != nil {
			t.Fatal("failed page escaped")
		}
		if page != nil && len(page.ItemsCopy()) > 10 {
			t.Fatal("page bound ignored")
		}
	})
}
