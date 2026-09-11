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
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	request "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

type fixtureStream struct {
	out    chan *wire.Payload
	closed chan struct{}
}
type fixtureServer struct {
	wire.UnimplementedRequestServer
	wire.UnimplementedBiRequestStreamServer
	testing          testing.TB
	mu               sync.Mutex
	values           map[key]string
	streams          map[string]*fixtureStream
	token            string
	override         *wire.Payload
	block            bool
	unregistered     bool
	entered          chan struct{}
	queries          atomic.Int32
	setups           atomic.Int32
	listens          atomic.Int32
	acknowledgements atomic.Int32
	listener         net.Listener
	server           *grpc.Server
	auth             *httptest.Server
	loginCount       atomic.Int32
}

func encoded(value response.IResponse) *wire.Payload {
	raw, _ := json.Marshal(value)
	return &wire.Payload{Metadata: &wire.Metadata{Type: value.GetResponseType()}, Body: payloadBytes(raw)}
}
func newFixture(t testing.TB, secure bool) *fixtureServer {
	t.Helper()
	fixture := &fixtureServer{testing: t, values: map[key]string{{"DEFAULT_GROUP", "settings.yaml"}: "host: initial\nlarge: 18446744073709551615\n"},
		streams: make(map[string]*fixtureStream), entered: make(chan struct{}, 64)}
	fixture.auth = httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, httpRequest *http.Request) {
		if httpRequest.Method != http.MethodPost || httpRequest.URL.Path != "/nacos/v1/auth/users/login" {
			writer.WriteHeader(404)
			return
		}
		_ = httpRequest.ParseForm()
		if httpRequest.Form.Get("username") != "reader" || httpRequest.Form.Get("password") != "credential-canary" {
			writer.WriteHeader(403)
			return
		}
		fixture.loginCount.Add(1)
		fixture.mu.Lock()
		token := fixture.token
		fixture.mu.Unlock()
		if token == "" {
			token = "access-token-canary"
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"accessToken": token, "tokenTtl": 2})
	}))
	var options []grpc.ServerOption
	if secure {
		fixture.auth.StartTLS()
		config := fixture.auth.TLS.Clone()
		config.NextProtos = []string{"h2"}
		options = append(options, grpc.Creds(credentials.NewTLS(config)))
	} else {
		fixture.auth.Start()
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture.listener = listener
	fixture.server = grpc.NewServer(options...)
	wire.RegisterRequestServer(fixture.server, fixture)
	wire.RegisterBiRequestStreamServer(fixture.server, fixture)
	go func() { _ = fixture.server.Serve(listener) }()
	t.Cleanup(func() { fixture.server.Stop(); fixture.auth.Close() })
	return fixture
}
func (fixture *fixtureServer) options() OptionsV1 {
	input := OptionsV1{Name: "fixture", Servers: []ServerV1{{HTTPURL: fixture.auth.URL + "/nacos", GRPCAddress: fixture.listener.Addr().String()}},
		Keys: []KeyV1{{DataID: "settings.yaml"}}, AllowInsecure: fixture.auth.TLS == nil,
		RequestTimeout: 3 * time.Second, RetryDelay: 10 * time.Millisecond, ReconcileInterval: time.Second}
	if fixture.auth.TLS != nil {
		input.RootCAPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fixture.auth.Certificate().Raw}))
	}
	return input
}
func openClient(t testing.TB, input OptionsV1) *Client {
	t.Helper()
	client, err := Open(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Close(ctx); err != nil {
			t.Error("cleanup incomplete", err)
		}
	})
	return client
}
func (fixture *fixtureServer) Request(ctx context.Context, payload *wire.Payload) (*wire.Payload, error) {
	if payload == nil || payload.Metadata == nil || payload.Body == nil {
		return nil, errors.New("invalid fixture payload")
	}
	var correlation struct {
		ID string `json:"requestId"`
	}
	_ = json.Unmarshal(payload.Body.Value, &correlation)
	base := &response.Response{ResultCode: 200, Success: true, RequestId: correlation.ID}
	if payload.Metadata.Type == "ServerCheckRequest" {
		return encoded(&response.ServerCheckResponse{Response: base, ConnectionId: "fixture-connection"}), nil
	}
	connection, _ := peer.FromContext(ctx)
	fixture.mu.Lock()
	stream := fixture.streams[connection.Addr.String()]
	token, override, block := fixture.token, fixture.override, fixture.block
	unregistered := fixture.unregistered
	fixture.mu.Unlock()
	if stream == nil || unregistered {
		return encoded(&response.ErrorResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 301, RequestId: correlation.ID}}), nil
	}
	if payload.Metadata.Type == "HealthCheckRequest" {
		return encoded(&response.HealthCheckResponse{Response: base}), nil
	}
	if token != "" && payload.Metadata.Headers["accessToken"] != token {
		return encoded(&response.ErrorResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 403, Message: "denied-native-canary", RequestId: correlation.ID}}), nil
	}
	stamp := payload.Metadata.Headers["Client-RequestTS"]
	digest := md5.Sum([]byte(stamp))
	if stamp == "" || payload.Metadata.Headers["Client-RequestToken"] != hex.EncodeToString(digest[:]) || payload.Metadata.Headers["charset"] != "utf-8" || payload.Metadata.Headers["exConfigInfo"] != "true" {
		fixture.testing.Error("native configuration headers were not sent")
	}
	if payload.Metadata.Type == "ConfigQueryRequest" {
		fixture.queries.Add(1)
		if payload.Metadata.Headers["notify"] != "false" {
			fixture.testing.Error("finite read enabled implicit native listening")
		}
		var query request.ConfigQueryRequest
		if json.Unmarshal(payload.Body.Value, &query) != nil {
			return nil, errors.New("invalid query")
		}
		if block {
			fixture.entered <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if override != nil {
			return override, nil
		}
		fixture.mu.Lock()
		content, found := fixture.values[key{query.Group, query.DataId}]
		fixture.mu.Unlock()
		if !found {
			return encoded(&response.ConfigQueryResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 300, RequestId: correlation.ID}}), nil
		}
		return encoded(&response.ConfigQueryResponse{Response: base, Content: content, Md5: checksum(content), ContentType: "yaml", LastModified: 1}), nil
	}
	if payload.Metadata.Type == "ConfigBatchListenRequest" {
		fixture.listens.Add(1)
		var listen request.ConfigBatchListenRequest
		if json.Unmarshal(payload.Body.Value, &listen) != nil || !listen.Listen {
			return nil, errors.New("invalid listen")
		}
		changes := []model.ConfigContext{}
		fixture.mu.Lock()
		for _, selected := range listen.ConfigListenContexts {
			if content, ok := fixture.values[key{selected.Group, selected.DataId}]; ok && checksum(content) != selected.Md5 {
				changes = append(changes, model.ConfigContext{Group: selected.Group, DataId: selected.DataId, Tenant: selected.Tenant})
			}
		}
		fixture.mu.Unlock()
		return encoded(&response.ConfigChangeBatchListenResponse{Response: base, ChangedConfigs: changes}), nil
	}
	return nil, errors.New("unexpected native operation")
}
func (fixture *fixtureServer) RequestBiStream(stream wire.BiRequestStream_RequestBiStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.Metadata.GetType() != "ConnectionSetupRequest" {
		return errors.New("missing setup")
	}
	var setup request.ConnectionSetupRequest
	if json.Unmarshal(first.Body.Value, &setup) != nil || setup.Labels["source"] != "sdk" || setup.Labels["module"] != "config" || setup.ClientVersion != "Nacos-Go-Client:v2.3.5" {
		return errors.New("wrong native setup")
	}
	connection, _ := peer.FromContext(stream.Context())
	session := &fixtureStream{out: make(chan *wire.Payload, 64), closed: make(chan struct{})}
	fixture.mu.Lock()
	fixture.streams[connection.Addr.String()] = session
	fixture.mu.Unlock()
	fixture.setups.Add(1)
	defer func() {
		fixture.mu.Lock()
		delete(fixture.streams, connection.Addr.String())
		fixture.mu.Unlock()
		close(session.closed)
	}()
	for {
		select {
		case payload := <-session.out:
			if err := stream.Send(payload); err != nil {
				return err
			}
			reply, err := stream.Recv()
			if err != nil {
				return err
			}
			var sent, got struct {
				ID string `json:"requestId"`
			}
			_ = json.Unmarshal(payload.Body.Value, &sent)
			_ = json.Unmarshal(reply.Body.Value, &got)
			expected := "NotifySubscriberResponse"
			if payload.Metadata.Type == "ClientDetectionRequest" {
				expected = "ClientDetectionResponse"
			}
			if payload.Metadata.Type == "ConnectResetRequest" {
				expected = "ConnectResetResponse"
			}
			if reply.Metadata.Type != expected || got.ID != sent.ID {
				fixture.testing.Error("native push acknowledgement changed")
			}
			fixture.acknowledgements.Add(1)
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}
func (fixture *fixtureServer) push(t testing.TB, payload *wire.Payload) {
	t.Helper()
	fixture.mu.Lock()
	var streams []*fixtureStream
	for _, stream := range fixture.streams {
		streams = append(streams, stream)
	}
	fixture.mu.Unlock()
	if len(streams) == 0 {
		t.Fatal("no registered native stream")
	}
	for _, stream := range streams {
		select {
		case stream.out <- payload:
		case <-stream.closed:
		case <-time.After(time.Second):
			t.Fatal("fixture push blocked")
		}
	}
}
func TestClientOptionsAndTLS(t *testing.T) {
	fixture := newFixture(t, true)
	input := fixture.options()
	input.Username, input.Password = "reader", "credential-canary"
	fixture.token = "access-token-canary"
	client := openClient(t, input)
	doc, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"})
	if err != nil || doc == nil || fixture.loginCount.Load() != 1 {
		t.Fatal("authenticated TLS acquisition failed", err)
	}
	wrong := input
	wrong.Name = "untrusted"
	wrong.RootCAPEM = ""
	_, err = openClient(t, wrong).Read(context.Background(), KeyV1{DataID: "settings.yaml"})
	var authority x509.UnknownAuthorityError
	if !errors.As(err, &authority) {
		t.Fatal("original TLS failure was not retained", err)
	}
}
func TestClientCloseCancelsAdmittedRequests(t *testing.T) {
	fixture := newFixture(t, false)
	fixture.block = true
	input := fixture.options()
	input.ConcurrentRequests = 2
	input.QueuedRequests = 2
	client := openClient(t, input)
	results := make(chan error, 2)
	for range 2 {
		go func() { _, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"}); results <- err }()
	}
	for range 2 {
		select {
		case <-fixture.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("native query did not start")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatal("close failed", err)
	}
	for range 2 {
		if err := <-results; !errors.Is(err, ErrClosed) {
			t.Fatal("close cause lost", err)
		}
	}
	if client.assembly.Snapshot().Sources[0].Usage != (resource.Usage{}) {
		t.Fatal("admission remained after joined close")
	}
	if _, err := client.Read(ctx, KeyV1{DataID: "settings.yaml"}); !errors.Is(err, ErrClosed) {
		t.Fatal("closed read admitted")
	}
}
func TestCloseTimeoutRetainsOwnership(t *testing.T) {
	client := openClient(t, newFixture(t, false).options())
	_, release, err := client.enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Close(ctx); !errors.Is(err, context.Canceled) || !client.assembly.Snapshot().Sources[0].Pending {
		t.Fatal("unfinished ownership was lost", err)
	}
	release()
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
