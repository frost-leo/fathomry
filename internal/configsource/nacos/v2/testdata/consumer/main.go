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

// This is an in-module maintainer acceptance executable, not a public loader.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	nacos "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
	"github.com/frost-leo/fathomry/internal/resource"
	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/anypb"
)

type server struct {
	wire.UnimplementedRequestServer
	wire.UnimplementedBiRequestStreamServer
	mu      sync.Mutex
	ready   map[string]bool
	content map[string]string
}

func (value *server) Request(ctx context.Context, payload *wire.Payload) (*wire.Payload, error) {
	kind := payload.GetMetadata().GetType()
	var header struct {
		RequestID string `json:"requestId"`
	}
	_ = json.Unmarshal(payload.GetBody().GetValue(), &header)
	result := map[string]any{"resultCode": 200, "requestId": header.RequestID}
	switch kind {
	case "ServerCheckRequest":
		kind = "ServerCheckResponse"
		result["connectionId"] = "consumer"
	default:
		remote, _ := peer.FromContext(ctx)
		value.mu.Lock()
		ready := value.ready[remote.Addr.String()]
		value.mu.Unlock()
		if !ready {
			kind = "ErrorResponse"
			result["resultCode"] = 500
			result["errorCode"] = 301
		} else if kind == "HealthCheckRequest" {
			kind = "HealthCheckResponse"
		} else if kind == "ConfigQueryRequest" {
			kind = "ConfigQueryResponse"
			var query struct {
				DataID string `json:"dataId"`
			}
			_ = json.Unmarshal(payload.Body.Value, &query)
			value.mu.Lock()
			content, found := value.content[query.DataID]
			value.mu.Unlock()
			if found {
				result["content"] = content
			} else {
				result["resultCode"], result["errorCode"] = 500, 300
			}
		} else if kind == "ConfigPublishRequest" || kind == "ConfigRemoveRequest" {
			var query struct {
				DataID  string `json:"dataId"`
				Content string `json:"content"`
			}
			_ = json.Unmarshal(payload.Body.Value, &query)
			value.mu.Lock()
			if kind == "ConfigPublishRequest" {
				value.content[query.DataID] = query.Content
				kind = "ConfigPublishResponse"
			} else {
				delete(value.content, query.DataID)
				kind = "ConfigRemoveResponse"
			}
			value.mu.Unlock()
		} else {
			return nil, errors.New("unexpected consumer request")
		}
	}
	raw, _ := json.Marshal(result)
	return &wire.Payload{Metadata: &wire.Metadata{Type: kind}, Body: &anypb.Any{Value: raw}}, nil
}
func (value *server) RequestBiStream(stream wire.BiRequestStream_RequestBiStreamServer) error {
	payload, err := stream.Recv()
	if err != nil {
		return err
	}
	if payload.GetMetadata().GetType() != "ConnectionSetupRequest" {
		return errors.New("missing setup")
	}
	remote, _ := peer.FromContext(stream.Context())
	value.mu.Lock()
	value.ready[remote.Addr.String()] = true
	value.mu.Unlock()
	defer func() { value.mu.Lock(); delete(value.ready, remote.Addr.String()); value.mu.Unlock() }()
	<-stream.Context().Done()
	return stream.Context().Err()
}
func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic("consumer listener failed")
	}
	endpoint := &server{ready: make(map[string]bool), content: map[string]string{"settings.json": `{"labels":{"X-Case":"kept"},"optional":null,"large":18446744073709551615}`}}
	service := grpc.NewServer()
	wire.RegisterRequestServer(service, endpoint)
	wire.RegisterBiRequestStreamServer(service, endpoint)
	go func() { _ = service.Serve(listener) }()
	defer service.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := nacos.Open(ctx, nacos.OptionsV1{Name: "consumer", AllowInsecure: true, DynamicKeys: true, Writable: true,
		Servers: []nacos.ServerV1{{HTTPURL: "http://127.0.0.1:8848/nacos", GRPCAddress: listener.Addr().String()}}, Keys: []nacos.KeyV1{{DataID: "settings.json"}}})
	if err != nil {
		panic("consumer construction failed")
	}
	documents, err := client.ReadAll(ctx)
	if err != nil {
		panic("consumer acquisition failed")
	}
	type config struct {
		Labels   map[string]string `json:"labels"`
		Optional *string           `json:"optional"`
		Large    uint64            `json:"large"`
	}
	calls := 0
	prepared, err := resource.Prepare(resource.Schema[config]{Format: 1, Validate: func(value config) error {
		calls++
		if value.Labels["X-Case"] != "kept" || value.Optional != nil || value.Large != ^uint64(0) {
			return errors.New("lossy handoff")
		}
		return nil
	}}, resource.Input{Identity: resource.Identity{Provider: "consumer.settings", Name: "application"}, Format: 1,
		Layers: []resource.Layer{{Kind: resource.Base, Content: documents[0].RawCopy()}}})
	if err != nil || calls != 1 || prepared.Description().Revision == "" {
		panic("consumer preparation failed")
	}
	selected := nacos.KeyV1{DataID: "dynamic.toml"}
	result, err := client.Publish(ctx, nacos.PublishInputV1{Key: selected, Content: "value=1", ContentType: "toml"})
	if err != nil || result.State() != nacos.MutationAcknowledged {
		panic("consumer publication failed")
	}
	document, err := client.ReadRaw(ctx, selected)
	if err != nil || document.Missing() || string(document.RawCopy()) != "value=1" {
		panic("consumer dynamic read failed")
	}
	result, err = client.Delete(ctx, selected)
	if err != nil || result.State() != nacos.MutationAcknowledged {
		panic("consumer deletion failed")
	}
	document, err = client.ReadRaw(ctx, selected)
	if err != nil || !document.Missing() {
		panic("consumer dynamic absence failed")
	}
	if err := client.Close(context.Background()); err != nil {
		panic("consumer cleanup failed")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/nacos-group/nacos-sdk-go/v2"}})
	if err != nil || len(build.SDKs) != 1 {
		panic("consumer build inspection failed")
	}
	report := struct {
		Go, SDK, SDKSum                  string
		PreparationVerified, RealService bool
	}{
		build.Go.Value, build.SDKs[0].Version.Value, build.SDKs[0].Sum.Value, true, false}
	if json.NewEncoder(os.Stdout).Encode(report) != nil {
		panic("consumer report failed")
	}
}
