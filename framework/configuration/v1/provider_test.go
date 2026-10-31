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

package configuration

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	request "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

type peer struct {
	wire.UnimplementedRequestServer
	wire.UnimplementedBiRequestStreamServer
	mu      sync.Mutex
	value   string
	grpc    *grpc.Server
	http    *httptest.Server
	address string
	queries atomic.Int32
	writes  atomic.Int32
}

func hash(value string) string {
	result := md5.Sum([]byte(value))
	return hex.EncodeToString(result[:])
}
func payload(value response.IResponse) *wire.Payload {
	raw, _ := json.Marshal(value)
	return &wire.Payload{Metadata: &wire.Metadata{Type: value.GetResponseType()}, Body: &anypb.Any{Value: raw}}
}
func newPeer(t *testing.T) *peer {
	t.Helper()
	value := &peer{value: "workers: 2"}
	value.http = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNotFound) }))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	value.address = listener.Addr().String()
	value.grpc = grpc.NewServer()
	wire.RegisterRequestServer(value.grpc, value)
	wire.RegisterBiRequestStreamServer(value.grpc, value)
	go func() { _ = value.grpc.Serve(listener) }()
	t.Cleanup(func() { value.grpc.Stop(); value.http.Close() })
	return value
}
func (value *peer) Request(ctx context.Context, input *wire.Payload) (*wire.Payload, error) {
	if input == nil || input.Metadata == nil || input.Body == nil {
		return nil, errors.New("invalid fixture request")
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
		return payload(&response.ServerCheckResponse{Response: base, ConnectionId: "configuration-fixture"}), nil
	case "HealthCheckRequest":
		return payload(&response.HealthCheckResponse{Response: base}), nil
	case "ConfigQueryRequest":
		value.queries.Add(1)
		value.mu.Lock()
		content := value.value
		value.mu.Unlock()
		return payload(&response.ConfigQueryResponse{Response: base, Content: content, Md5: hash(content), ContentType: "yaml", LastModified: 1}), nil
	case "ConfigBatchListenRequest":
		var selected request.ConfigBatchListenRequest
		if err := json.Unmarshal(input.Body.Value, &selected); err != nil {
			return nil, err
		}
		value.mu.Lock()
		content := value.value
		value.mu.Unlock()
		changes := []model.ConfigContext{}
		for _, key := range selected.ConfigListenContexts {
			if key.Md5 != hash(content) {
				changes = append(changes, model.ConfigContext{Group: key.Group, DataId: key.DataId, Tenant: key.Tenant})
			}
		}
		return payload(&response.ConfigChangeBatchListenResponse{Response: base, ChangedConfigs: changes}), nil
	default:
		value.writes.Add(1)
		return nil, errors.New("unsupported fixture operation")
	}
}
func (value *peer) RequestBiStream(stream wire.BiRequestStream_RequestBiStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.Metadata.GetType() != "ConnectionSetupRequest" {
		return errors.New("missing connection setup")
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func (value *peer) plan(watch bool) SourcePlan {
	return SourcePlan{Version: 1, Environment: "fixture", Mode: "remote", Provider: "nacos", Watch: watch,
		Documents: []Document{{Kind: "base", Encoding: YAML}},
		Remote: &Remote{Settings: nacos.Settings{Name: "fixture", Servers: []nacos.Server{{HTTPURL: value.http.URL + "/nacos", GRPCAddress: value.address}},
			Keys: []nacos.Key{{DataID: "main"}}, AllowInsecure: true, RequestTimeout: time.Second, RetryDelay: time.Millisecond, ReconcileInterval: time.Second}}}
}
func remoteDependencies(t *testing.T, capacity int, work int64) (Dependencies, *adapters.Inbox[nacos.Evidence]) {
	t.Helper()
	runtime, err := adapters.New(context.Background(), adapters.Options{MaxWorkBytes: work})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[nacos.Evidence](adapters.EvidenceOptions{Capacity: capacity})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		status, _ := inbox.Inspect()
		for range status.Queued {
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
			}
		}
	})
	return Dependencies{Remote: nacos.Dependencies{Runtime: runtime, Evidence: inbox}}, inbox
}

func TestRemoteProfiles(t *testing.T) {
	for _, watch := range []bool{false, true} {
		t.Run(map[bool]string{false: "finite", true: "watch"}[watch], func(t *testing.T) {
			fixture := newPeer(t)
			dependencies, inbox := remoteDependencies(t, 16, 64<<20)
			session, err := startSession(context.Background(), fixture.plan(watch), schema(), options(), dependencies)
			if err != nil {
				t.Fatal(err)
			}
			defer closeSession(t, session, false)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := session.waitInitial(ctx); err != nil {
				t.Fatal(err)
			}
			if read(t, session.Reader()).Workers != 2 {
				t.Fatal("wrong remote candidate")
			}
			first, err := session.Next(ctx)
			if err != nil || !first.Published {
				t.Fatal(err)
			}
			if watch {
				fixture.mu.Lock()
				fixture.value = "workers: 0"
				fixture.mu.Unlock()
				for {
					result, err := session.Next(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if errors.Is(result.Err, invalidWorkers) {
						break
					}
				}
				if read(t, session.Reader()).Workers != 2 {
					t.Fatal("invalid remote update published")
				}
				fixture.mu.Lock()
				fixture.value = "workers: 4"
				fixture.mu.Unlock()
				for {
					result, err := session.Next(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if result.Published && read(t, session.Reader()).Workers == 4 {
						break
					}
				}
			} else {
				// Finite capture is released while the earlier Open remains live.
				delivery, err := inbox.NextReleased(ctx)
				if err != nil {
					t.Fatal("finite evidence blocked behind owner", err)
				}
				if err := delivery.Ack(); err != nil {
					t.Fatal(err)
				}
			}
			if fixture.writes.Load() != 0 {
				t.Fatal("read-only profile issued another operation")
			}
			closeSession(t, session, false)
		})
	}
}
func TestRemoteInitialObservationDoesNotRefetch(t *testing.T) {
	fixture := newPeer(t)
	dependencies, _ := remoteDependencies(t, 16, 64<<20)
	plan := fixture.plan(true)
	plan.Remote.Settings.ReconcileInterval = time.Minute
	session, err := startSession(context.Background(), plan, schema(), options(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, false)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.waitInitial(ctx); err != nil {
		t.Fatal(err)
	}
	if fixture.queries.Load() != 1 {
		t.Fatal("initial observer batch was reacquired", fixture.queries.Load())
	}
}
func TestRemotePermissionsAndBudgets(t *testing.T) {
	fixture := newPeer(t)
	for _, test := range []struct {
		name     string
		capacity int
		work     int64
		change   func(*SourcePlan)
	}{
		{"one_evidence_record", 1, 64 << 20, func(*SourcePlan) {}},
		{"undersized_work", 2, 16 << 20, func(*SourcePlan) {}},
		{"oversized_observation_queue", 2, 64 << 20, func(value *SourcePlan) { value.Watch = true; value.Remote.Observation.QueueCapacity = 12 }},
		{"writes", 2, 64 << 20, func(value *SourcePlan) { value.Remote.Settings.Writable = true }},
		{"dynamic_keys", 2, 64 << 20, func(value *SourcePlan) { value.Remote.Settings.DynamicKeys = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependencies, inbox := remoteDependencies(t, test.capacity, test.work)
			plan := fixture.plan(false)
			test.change(&plan)
			want := ErrLimit
			if test.name == "writes" || test.name == "dynamic_keys" {
				want = ErrDeclaration
			}
			if session, err := startSession(context.Background(), plan, schema(), options(), dependencies); !errors.Is(err, want) || session != nil {
				t.Fatal("invalid remote admission did not fail for the intended reason", err)
			}
			evidence, _ := inbox.Inspect()
			if evidence.Queued != 0 || fixture.queries.Load() != 0 {
				t.Fatal("preflight performed source work")
			}
		})
	}
}

func TestSelectedObserverAdmission(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "local", true: "remote"}[remote], func(t *testing.T) {
			var dependencies Dependencies
			var inspect func() (adapters.EvidenceStatus, error)
			var plan SourcePlan
			if remote {
				fixture := newPeer(t)
				plan = fixture.plan(false)
				selected, inbox := remoteDependencies(t, 16, 64<<20)
				dependencies, inspect = selected, inbox.Inspect
				dependencies.Remote.Observer = &adapters.Observer{}
				t.Cleanup(func() {
					if fixture.queries.Load() != 0 {
						t.Error("invalid observer reached the remote source")
					}
				})
			} else {
				path := filepath.Join(t.TempDir(), "application.yaml")
				write(t, path, "workers: 2")
				plan = localPlan(path, false)
				selected, inbox := localDependencies(t, 16)
				dependencies, inspect = selected, inbox.Inspect
				dependencies.Local.Observer = &adapters.Observer{}
			}
			before, err := inspect()
			if err != nil {
				t.Fatal(err)
			}
			session, err := startSession(context.Background(), plan, schema(), options(), dependencies)
			if session != nil {
				closeSession(t, session, true)
			}
			if session != nil || !errors.Is(err, ErrDeclaration) || !errors.Is(err, adapters.ErrHandle) {
				t.Fatal("invalid selected observer was not rejected at admission with its cause", err)
			}
			after, err := inspect()
			if err != nil || after != before {
				t.Fatal("rejected observer changed evidence custody", err)
			}
		})
	}
}

func TestObserverSelectionAndOptionality(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, sealed := range []bool{false, true} {
			name := map[bool]string{false: "local", true: "remote"}[remote] + map[bool]string{false: "/nil", true: "/sealed"}[sealed]
			t.Run(name, func(t *testing.T) {
				var observer *adapters.Observer
				if sealed {
					var err error
					observer, err = adapters.NewObserver(1)
					if err != nil {
						t.Fatal(err)
					}
					if err := observer.Seal(); err != nil {
						t.Fatal(err)
					}
				}
				var dependencies Dependencies
				var plan SourcePlan
				if remote {
					plan = newPeer(t).plan(false)
					dependencies, _ = remoteDependencies(t, 16, 64<<20)
					dependencies.Remote.Observer = observer
					dependencies.Local.Observer = &adapters.Observer{}
				} else {
					path := filepath.Join(t.TempDir(), "application.yaml")
					write(t, path, "workers: 2")
					plan = localPlan(path, false)
					dependencies, _ = localDependencies(t, 16)
					dependencies.Local.Observer = observer
					dependencies.Remote.Observer = &adapters.Observer{}
				}
				session, err := startSession(context.Background(), plan, schema(), options(), dependencies)
				if err != nil {
					t.Fatal(err)
				}
				defer closeSession(t, session, false)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := session.waitInitial(ctx); err != nil {
					t.Fatal(err)
				}
				if read(t, session.Reader()).Workers != 2 {
					t.Fatal("optional diagnostics changed acquisition")
				}
			})
		}
	}
}
