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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	rpcRequest "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

type serviceFixture struct {
	wire.UnimplementedRequestServer
	wire.UnimplementedBiRequestStreamServer
	mu               sync.Mutex
	values           map[Key]string
	published        rpcRequest.ConfigPublishRequest
	denied           bool
	blocked          bool
	version3         bool
	malformed        bool
	server           *grpc.Server
	http             *httptest.Server
	address          string
	queries          atomic.Int32
	mutations        atomic.Int32
	searchV1         atomic.Int32
	searchV3         atomic.Int32
	setups           atomic.Int32
	entered          chan struct{}
	queryEntered     chan string
	queryRelease     <-chan struct{}
	namespaceContent bool
}

func payload(value response.IResponse) *wire.Payload {
	raw, _ := json.Marshal(value)
	return &wire.Payload{Metadata: &wire.Metadata{Type: value.GetResponseType()}, Body: &anypb.Any{Value: raw}}
}
func checksum(value string) string { sum := md5.Sum([]byte(value)); return hex.EncodeToString(sum[:]) }
func newService(t *testing.T) *serviceFixture {
	t.Helper()
	fixture := &serviceFixture{values: map[Key]string{{Group: "DEFAULT_GROUP", DataID: "main"}: "value: initial\n", {Group: "DEFAULT_GROUP", DataID: "empty"}: ""}, entered: make(chan struct{}, 1)}
	fixture.http = httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture.address = listener.Addr().String()
	fixture.server = grpc.NewServer()
	wire.RegisterRequestServer(fixture.server, fixture)
	wire.RegisterBiRequestStreamServer(fixture.server, fixture)
	go func() { _ = fixture.server.Serve(listener) }()
	t.Cleanup(func() { fixture.server.Stop(); fixture.http.Close() })
	return fixture
}
func (fixture *serviceFixture) settings() Settings {
	return Settings{Name: "fixture", Servers: []Server{{HTTPURL: fixture.http.URL + "/nacos", GRPCAddress: fixture.address}}, Keys: []Key{{DataID: "main"}}, DynamicKeys: true, Writable: true, AllowInsecure: true, RequestTimeout: time.Second, RetryDelay: time.Millisecond, ReconcileInterval: time.Second}
}
func openService(t *testing.T, fixture *serviceFixture, settings Settings) (*Owner, *adapters.Runtime, *adapters.Inbox[Evidence]) {
	t.Helper()
	runtime, err := adapters.New(context.Background(), adapters.Options{MaxWorkBytes: 256 << 20})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Evidence](adapters.EvidenceOptions{Capacity: 128})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := Open(context.Background(), settings, Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return owner, runtime, inbox
}
func (fixture *serviceFixture) Request(ctx context.Context, input *wire.Payload) (*wire.Payload, error) {
	if input == nil || input.Metadata == nil || input.Body == nil {
		return nil, errors.New("invalid test payload")
	}
	var correlation struct {
		ID string `json:"requestId"`
	}
	if err := json.Unmarshal(input.Body.Value, &correlation); err != nil {
		return nil, err
	}
	base := &response.Response{ResultCode: 200, Success: true, RequestId: correlation.ID}
	switch input.Metadata.Type {
	case "ServerCheckRequest":
		return payload(&response.ServerCheckResponse{Response: base, ConnectionId: "fixture"}), nil
	case "HealthCheckRequest":
		return payload(&response.HealthCheckResponse{Response: base}), nil
	}
	fixture.mu.Lock()
	denied, blocked, malformed := fixture.denied, fixture.blocked, fixture.malformed
	fixture.mu.Unlock()
	if denied {
		return payload(&response.ErrorResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 403, RequestId: correlation.ID, Message: "server-private-canary"}}), nil
	}
	switch input.Metadata.Type {
	case "ConfigQueryRequest":
		fixture.queries.Add(1)
		if malformed {
			return &wire.Payload{Metadata: &wire.Metadata{Type: "ConfigQueryResponse"}, Body: &anypb.Any{Value: []byte("{")}}, nil
		}
		var query rpcRequest.ConfigQueryRequest
		if err := json.Unmarshal(input.Body.Value, &query); err != nil {
			return nil, err
		}
		fixture.mu.Lock()
		content, exists := fixture.values[Key{Group: query.Group, DataID: query.DataId}]
		entered, release, namespaceContent := fixture.queryEntered, fixture.queryRelease, fixture.namespaceContent
		fixture.mu.Unlock()
		if entered != nil && query.DataId == "main" {
			select {
			case entered <- query.Tenant:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if namespaceContent {
			content, exists = query.Tenant, true
		}
		if !exists {
			return payload(&response.ConfigQueryResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 300, RequestId: correlation.ID}}), nil
		}
		return payload(&response.ConfigQueryResponse{Response: base, Content: content, Md5: checksum(content), ContentType: "yaml", LastModified: 7}), nil
	case "ConfigPublishRequest", "ConfigRemoveRequest":
		fixture.mutations.Add(1)
		if blocked {
			select {
			case fixture.entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		if input.Metadata.Type == "ConfigPublishRequest" {
			var value rpcRequest.ConfigPublishRequest
			if err := json.Unmarshal(input.Body.Value, &value); err != nil {
				return nil, err
			}
			fixture.published = value
			key := Key{Group: value.Group, DataID: value.DataId}
			if value.CasMd5 != "" && value.CasMd5 != checksum(fixture.values[key]) {
				return payload(&response.ErrorResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 409, RequestId: correlation.ID, Message: "cas-private-canary"}}), nil
			}
			fixture.values[key] = value.Content
			return payload(&response.ConfigPublishResponse{Response: base}), nil
		}
		var value rpcRequest.ConfigRemoveRequest
		if err := json.Unmarshal(input.Body.Value, &value); err != nil {
			return nil, err
		}
		delete(fixture.values, Key{Group: value.Group, DataID: value.DataId})
		return payload(&response.ConfigRemoveResponse{Response: base}), nil
	case "ConfigBatchListenRequest":
		var value rpcRequest.ConfigBatchListenRequest
		if err := json.Unmarshal(input.Body.Value, &value); err != nil {
			return nil, err
		}
		changes := []model.ConfigContext{}
		fixture.mu.Lock()
		for _, key := range value.ConfigListenContexts {
			if content, exists := fixture.values[Key{Group: key.Group, DataID: key.DataId}]; exists && checksum(content) != key.Md5 {
				changes = append(changes, model.ConfigContext{Group: key.Group, DataId: key.DataId, Tenant: key.Tenant})
			}
		}
		fixture.mu.Unlock()
		return payload(&response.ConfigChangeBatchListenResponse{Response: base, ChangedConfigs: changes}), nil
	}
	return nil, errors.New("unsupported test request")
}
func (fixture *serviceFixture) RequestBiStream(stream wire.BiRequestStream_RequestBiStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.Metadata.GetType() != "ConnectionSetupRequest" {
		return errors.New("missing setup")
	}
	fixture.setups.Add(1)
	<-stream.Context().Done()
	return stream.Context().Err()
}
func (fixture *serviceFixture) serveHTTP(writer http.ResponseWriter, input *http.Request) {
	if strings.HasSuffix(input.URL.Path, "/auth/users/login") {
		_ = json.NewEncoder(writer).Encode(map[string]any{"accessToken": "fixture-token", "tokenTtl": 60})
		return
	}
	fixture.mu.Lock()
	denied, version3 := fixture.denied, fixture.version3
	fixture.mu.Unlock()
	v3 := strings.Contains(input.URL.Path, "/v3/")
	if v3 {
		fixture.searchV3.Add(1)
	} else {
		fixture.searchV1.Add(1)
	}
	if denied {
		writer.WriteHeader(http.StatusForbidden)
		return
	}
	if version3 && !v3 {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	item := map[string]any{"id": 1, "dataId": "main", "md5": checksum("value: initial\n"), "appName": "fixture"}
	if v3 {
		item["groupName"] = "DEFAULT_GROUP"
		item["namespaceId"] = ""
	} else {
		item["group"] = "DEFAULT_GROUP"
		item["tenant"] = ""
		item["content"] = "value: initial\n"
	}
	page := map[string]any{"totalCount": 1, "pageNumber": 1, "pagesAvailable": 1, "pageItems": []any{item}}
	if v3 {
		_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "data": page})
	} else {
		_ = json.NewEncoder(writer).Encode(page)
	}
}
