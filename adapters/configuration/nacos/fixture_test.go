/*
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

package nacos_test

import (
	"context"
	"crypto/md5"
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

	"github.com/frost-leo/fathomry/adapters/configuration/nacos"
	"github.com/frost-leo/fathomry/framework/configuration"
	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type fixture struct {
	wire.UnimplementedRequestServer
	wire.UnimplementedBiRequestStreamServer
	testing     *testing.T
	mu          sync.Mutex
	content     map[string]string
	sessions    map[string]bool
	faultID     string
	fault       int
	malformed   bool
	rpcDeadline bool
	encrypted   bool
	delay       time.Duration
	requireAuth bool
	queries     atomic.Int32
	logins      atomic.Int32
	watches     atomic.Int32
	auth        *httptest.Server
	listener    net.Listener
	server      *grpc.Server
}

func newFixture(t *testing.T, secure bool) *fixture {
	t.Helper()
	f := &fixture{testing: t, content: map[string]string{"private-base": "name: base\nlarge: 18446744073709551615\nheaders: {X-Tenant: exact}\nitems: [base]"},
		sessions: make(map[string]bool), requireAuth: true}
	f.auth = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/nacos/v1/auth/users/login" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.ParseForm() != nil || r.Form.Get("username") != "private-reader" || r.Form.Get("password") != "private-password" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		f.logins.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"accessToken": "private-token", "tokenTtl": 3600})
	}))
	var options []grpc.ServerOption
	if secure {
		f.auth.StartTLS()
		trust := f.auth.TLS.Clone()
		trust.NextProtos = []string{"h2"}
		options = append(options, grpc.Creds(credentials.NewTLS(trust)))
	} else {
		f.auth.Start()
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.listener, f.server = listener, grpc.NewServer(options...)
	wire.RegisterRequestServer(f.server, f)
	wire.RegisterBiRequestStreamServer(f.server, f)
	go func() { _ = f.server.Serve(listener) }()
	t.Cleanup(func() { f.server.Stop(); f.auth.Close() })
	return f
}

func (f *fixture) options() nacos.Options {
	options := nacos.Options{
		Namespace: "private-namespace",
		Servers:   []nacos.Server{{HTTPURL: f.auth.URL + "/nacos", GRPCAddress: f.listener.Addr().String()}},
		Sources:   []nacos.Source{{Name: "base", DataID: "private-base", Layer: configuration.Base}},
		Username:  "private-reader", Password: "private-password", AllowInsecure: f.auth.TLS == nil,
		RequestTimeout: 3 * time.Second, RetryDelay: time.Millisecond,
	}
	if f.auth.TLS != nil {
		options.RootCAPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.auth.Certificate().Raw}))
	}
	return options
}

func reply(kind, id string, fields map[string]any) *wire.Payload {
	value := map[string]any{"resultCode": 200, "success": true, "requestId": id}
	for key, field := range fields {
		value[key] = field
	}
	data, _ := json.Marshal(value)
	return &wire.Payload{Metadata: &wire.Metadata{Type: kind}, Body: &anypb.Any{Value: data}}
}

func (f *fixture) Request(ctx context.Context, payload *wire.Payload) (*wire.Payload, error) {
	if payload == nil || payload.Metadata == nil || payload.Body == nil {
		return nil, errors.New("invalid fixture request")
	}
	var request struct {
		ID     string `json:"requestId"`
		DataID string `json:"dataId"`
		Group  string `json:"group"`
		Tenant string `json:"tenant"`
	}
	if json.Unmarshal(payload.Body.Value, &request) != nil {
		return nil, errors.New("invalid fixture body")
	}
	if payload.Metadata.Type == "ServerCheckRequest" {
		return reply("ServerCheckResponse", request.ID, map[string]any{"connectionId": "fixture"}), nil
	}
	remote, _ := peer.FromContext(ctx)
	f.mu.Lock()
	registered := f.sessions[remote.Addr.String()]
	content, present := f.content[request.DataID]
	faultID, fault, malformed, encrypted, delay := f.faultID, f.fault, f.malformed, f.encrypted, f.delay
	rpcDeadline := f.rpcDeadline
	requireAuth := f.requireAuth
	f.mu.Unlock()
	if !registered {
		return reply("ErrorResponse", request.ID, map[string]any{"resultCode": 500, "errorCode": 301}), nil
	}
	if payload.Metadata.Type == "HealthCheckRequest" {
		return reply("HealthCheckResponse", request.ID, nil), nil
	}
	if payload.Metadata.Type != "ConfigQueryRequest" {
		f.watches.Add(1)
		return nil, errors.New("unexpected non-read operation")
	}
	f.queries.Add(1)
	if payload.Metadata.Headers["notify"] != "false" || request.Tenant != "private-namespace" || request.Group != "DEFAULT_GROUP" {
		f.testing.Error("native read scope or no-watch contract changed")
	}
	if requireAuth && payload.Metadata.Headers["accessToken"] != "private-token" {
		return reply("ErrorResponse", request.ID, map[string]any{"resultCode": 500, "errorCode": 403, "message": "private-token"}), nil
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if request.DataID == faultID {
		if rpcDeadline {
			return nil, status.Error(codes.DeadlineExceeded, "private-native-deadline-detail")
		}
		if malformed {
			return &wire.Payload{Metadata: &wire.Metadata{Type: "ConfigQueryResponse"}, Body: &anypb.Any{Value: []byte("{")}}, nil
		}
		if fault != 0 {
			return reply("ConfigQueryResponse", request.ID, map[string]any{"resultCode": 500, "errorCode": fault, "message": "private-native-message"}), nil
		}
		if encrypted {
			return reply("ConfigQueryResponse", request.ID, map[string]any{"content": "ciphertext", "encryptedDataKey": "private-key"}), nil
		}
	}
	if !present {
		return reply("ConfigQueryResponse", request.ID, map[string]any{"resultCode": 500, "errorCode": 300}), nil
	}
	hash := md5.Sum([]byte(content))
	return reply("ConfigQueryResponse", request.ID, map[string]any{"content": content, "md5": hex.EncodeToString(hash[:]), "contentType": "yaml"}), nil
}

func (f *fixture) RequestBiStream(stream wire.BiRequestStream_RequestBiStreamServer) error {
	payload, err := stream.Recv()
	if err != nil {
		return err
	}
	if payload.Metadata.GetType() != "ConnectionSetupRequest" {
		return errors.New("missing setup")
	}
	remote, _ := peer.FromContext(stream.Context())
	key := remote.Addr.String()
	f.mu.Lock()
	f.sessions[key] = true
	f.mu.Unlock()
	defer func() { f.mu.Lock(); delete(f.sessions, key); f.mu.Unlock() }()
	<-stream.Context().Done()
	return stream.Context().Err()
}
