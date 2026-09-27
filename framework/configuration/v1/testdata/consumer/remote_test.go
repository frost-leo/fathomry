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

package consumer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remote "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	adapters "github.com/frost-leo/fathomry/adapters/v1"
	c "github.com/frost-leo/fathomry/framework/configuration/v1"
	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	request "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type protocolFixture struct {
	wire.UnimplementedRequestServer
	wire.UnimplementedBiRequestStreamServer
	t                                      testing.TB
	mu                                     sync.Mutex
	values                                 map[string]string
	streams                                map[string]chan *wire.Payload
	deny                                   bool
	denialCode                             codes.Code
	queryBlock                             <-chan struct{}
	malformed                              *wire.Payload
	loginToken                             string
	changeOnListen                         string
	dirtyReplies                           int
	badListen                              bool
	listenTimes                            []time.Time
	failNext                               atomic.Int32
	failCode                               atomic.Int32
	denials                                atomic.Int32
	queries, listens, setups, acks, logins atomic.Int32
	server                                 *grpc.Server
	listener                               net.Listener
	auth                                   *httptest.Server
}

func nativePayload(value response.IResponse) *wire.Payload {
	data, _ := json.Marshal(value)
	return &wire.Payload{Metadata: &wire.Metadata{Type: value.GetResponseType()}, Body: &anypb.Any{Value: data}}
}
func checksum(value string) string { sum := md5.Sum([]byte(value)); return hex.EncodeToString(sum[:]) }
func newProtocolFixture(t testing.TB, secure bool) *protocolFixture {
	t.Helper()
	fixture := &protocolFixture{t: t, values: map[string]string{"settings.yaml": "format: 1\nproject: {count: 21}"}, streams: make(map[string]chan *wire.Payload)}
	fixture.auth = httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.URL.Path != "/nacos/v1/auth/users/login" {
			writer.WriteHeader(404)
			return
		}
		_ = req.ParseForm()
		if req.Form.Get("username") != "reader" || req.Form.Get("password") != "credential-canary" {
			writer.WriteHeader(403)
			return
		}
		fixture.logins.Add(1)
		fixture.mu.Lock()
		token := fixture.loginToken
		fixture.mu.Unlock()
		_ = json.NewEncoder(writer).Encode(map[string]any{"accessToken": token, "tokenTtl": 60})
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
	var err error
	fixture.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture.server = grpc.NewServer(options...)
	wire.RegisterRequestServer(fixture.server, fixture)
	wire.RegisterBiRequestStreamServer(fixture.server, fixture)
	go func() { _ = fixture.server.Serve(fixture.listener) }()
	t.Cleanup(func() { fixture.server.Stop(); fixture.auth.Close() })
	return fixture
}
func (fixture *protocolFixture) settings() remote.Settings {
	value := remote.Settings{Name: "remote", Servers: []remote.Server{{HTTPURL: fixture.auth.URL + "/nacos", GRPCAddress: fixture.listener.Addr().String()}}, Documents: []remote.Document{{Name: "document", DataID: "settings.yaml"}},
		AllowInsecure: fixture.auth.TLS == nil, RequestTimeout: time.Second, RetryDelay: 20 * time.Millisecond, ReconcileInterval: time.Second}
	if fixture.auth.TLS != nil {
		value.RootCAPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fixture.auth.Certificate().Raw}))
	}
	return value
}
func (fixture *protocolFixture) selectSource(t testing.TB) source.Selection {
	t.Helper()
	value, err := remote.Select(fixture.settings())
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func remotePlan(selection source.Selection) c.Plan {
	return c.Plan{Modules: []adapters.Module{remote.Module()}, Inputs: []c.Input{{Source: selection, Documents: []c.LayerDocument{{Document: "document", Layer: c.Base}}}}}
}
func (fixture *protocolFixture) set(value string) {
	fixture.mu.Lock()
	fixture.values["settings.yaml"] = value
	fixture.mu.Unlock()
}
func (fixture *protocolFixture) Request(ctx context.Context, payload *wire.Payload) (*wire.Payload, error) {
	if payload == nil || payload.Metadata == nil || payload.Body == nil {
		return nil, status.Error(codes.InvalidArgument, "fixture")
	}
	var header struct {
		ID string `json:"requestId"`
	}
	_ = json.Unmarshal(payload.Body.Value, &header)
	base := &response.Response{ResultCode: 200, Success: true, RequestId: header.ID}
	if payload.Metadata.Type == "ServerCheckRequest" {
		return nativePayload(&response.ServerCheckResponse{Response: base, ConnectionId: "fixture"}), nil
	}
	connection, _ := peer.FromContext(ctx)
	fixture.mu.Lock()
	stream := fixture.streams[connection.Addr.String()]
	deny, token, malformed := fixture.deny, fixture.loginToken, fixture.malformed
	denialCode, queryBlock := fixture.denialCode, fixture.queryBlock
	fixture.mu.Unlock()
	if stream == nil {
		return nativePayload(&response.ErrorResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 301, RequestId: header.ID}}), nil
	}
	if payload.Metadata.Type == "HealthCheckRequest" {
		return nativePayload(&response.HealthCheckResponse{Response: base}), nil
	}
	if deny || token != "" && payload.Metadata.Headers["accessToken"] != token {
		fixture.denials.Add(1)
		if denialCode != codes.OK {
			return nil, status.Error(denialCode, "native-secret-canary")
		}
		return nativePayload(&response.ErrorResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 403, RequestId: header.ID, Message: "native-secret-canary"}}), nil
	}
	switch payload.Metadata.Type {
	case "ConfigQueryRequest":
		fixture.queries.Add(1)
		if queryBlock != nil {
			select {
			case <-queryBlock:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if code := fixture.failCode.Load(); code != 0 {
			return nil, status.Error(codes.Code(code), "rpc-secret-canary")
		}
		if payload.Metadata.Headers["notify"] != "false" {
			fixture.t.Error("unexpected implicit watch")
		}
		if code := fixture.failNext.Swap(0); code != 0 {
			return nil, status.Error(codes.Code(code), "rpc-secret-canary")
		}
		if malformed != nil {
			return malformed, nil
		}
		var query request.ConfigQueryRequest
		if err := json.Unmarshal(payload.Body.Value, &query); err != nil {
			return nil, err
		}
		if query.Group != "DEFAULT_GROUP" {
			fixture.t.Error("native group was not explicit")
		}
		fixture.mu.Lock()
		value, present := fixture.values[query.DataId]
		fixture.mu.Unlock()
		if !present {
			return nativePayload(&response.ConfigQueryResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 300, RequestId: header.ID}}), nil
		}
		return nativePayload(&response.ConfigQueryResponse{Response: base, Content: value, Md5: checksum(value), ContentType: "yaml", LastModified: 1}), nil
	case "ConfigBatchListenRequest":
		fixture.listens.Add(1)
		var listen request.ConfigBatchListenRequest
		if err := json.Unmarshal(payload.Body.Value, &listen); err != nil || !listen.Listen {
			return nil, status.Error(codes.InvalidArgument, "listen")
		}
		fixture.mu.Lock()
		fixture.listenTimes = append(fixture.listenTimes, time.Now())
		changed := fixture.changeOnListen != "" || fixture.dirtyReplies > 0 || fixture.badListen
		if fixture.changeOnListen != "" {
			fixture.values["settings.yaml"] = fixture.changeOnListen
			fixture.changeOnListen = ""
		}
		if fixture.dirtyReplies > 0 {
			fixture.dirtyReplies--
		}
		key := "settings.yaml"
		if fixture.badListen {
			key = "private-canary-unselected"
		}
		fixture.mu.Unlock()
		var changes []model.ConfigContext
		if changed {
			changes = []model.ConfigContext{{Group: "DEFAULT_GROUP", DataId: key, Tenant: ""}}
		}
		return nativePayload(&response.ConfigChangeBatchListenResponse{Response: base, ChangedConfigs: changes}), nil
	}
	return nil, status.Error(codes.InvalidArgument, "unexpected request")
}
func (fixture *protocolFixture) RequestBiStream(stream wire.BiRequestStream_RequestBiStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.Metadata.GetType() != "ConnectionSetupRequest" {
		return errors.New("missing registration")
	}
	connection, _ := peer.FromContext(stream.Context())
	queue := make(chan *wire.Payload, 128)
	fixture.mu.Lock()
	fixture.streams[connection.Addr.String()] = queue
	fixture.mu.Unlock()
	fixture.setups.Add(1)
	defer func() { fixture.mu.Lock(); delete(fixture.streams, connection.Addr.String()); fixture.mu.Unlock() }()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case payload := <-queue:
			if err := stream.Send(payload); err != nil {
				return err
			}
			acknowledgement, err := stream.Recv()
			if err != nil {
				return err
			}
			var sent, received struct {
				ID string `json:"requestId"`
			}
			_ = json.Unmarshal(payload.Body.Value, &sent)
			_ = json.Unmarshal(acknowledgement.Body.Value, &received)
			if received.ID != sent.ID {
				fixture.t.Error("incorrect push acknowledgement")
			}
			fixture.acks.Add(1)
		}
	}
}
func (fixture *protocolFixture) push(t testing.TB, reset bool) {
	t.Helper()
	kind := "ConfigChangeNotifyRequest"
	if reset {
		kind = "ConnectResetRequest"
	}
	data, _ := json.Marshal(map[string]any{"requestId": fmt.Sprint(time.Now().UnixNano()), "group": "DEFAULT_GROUP", "dataId": "settings.yaml", "tenant": ""})
	payload := &wire.Payload{Metadata: &wire.Metadata{Type: kind}, Body: &anypb.Any{Value: data}}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.streams) == 0 {
		t.Fatal("no registered stream")
	}
	for _, queue := range fixture.streams {
		select {
		case queue <- payload:
		default:
			t.Fatal("fixture queue overflow")
		}
	}
}
func eventual(t testing.TB, predicate func() bool) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for !predicate() {
		if time.Now().After(end) {
			t.Fatal("fixture state did not converge")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func TestRemoteCaptureAndLoadTLSAuthentication(t *testing.T) {
	fixture := newProtocolFixture(t, true)
	fixture.mu.Lock()
	fixture.loginToken = "test-access-token"
	fixture.mu.Unlock()
	settings := fixture.settings()
	settings.Username = "reader"
	settings.Password = "credential-canary"
	selected, err := remote.Select(settings)
	if err != nil {
		t.Fatal(err)
	}
	settings.Servers[0].GRPCAddress = "mutated"
	if fixture.setups.Load() != 0 || fixture.logins.Load() != 0 {
		t.Fatal("Select performed I/O")
	}
	for _, content := range []string{"", " \n", "format: 1\nproject: {count: 31}"} {
		fixture.set(content)
		batch, err := selected.Capture(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		raw, presence, err := batch.RawCopy("document")
		if err != nil || presence != source.Present || string(raw) != content {
			t.Fatal("raw remote fidelity failed")
		}
	}
	snapshot, err := c.Load(context.Background(), schema(), remotePlan(selected))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := snapshot.ValueCopy()
	if value.Project.Count != 31 || fixture.logins.Load() < 1 || fixture.listens.Load() != 0 {
		t.Fatal("finite Load used observer or lost auth/data")
	}
	fixture.mu.Lock()
	delete(fixture.values, "settings.yaml")
	fixture.mu.Unlock()
	batch, err := selected.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, presence, _ := batch.RawCopy("document"); presence != source.Missing {
		t.Fatal("code 300 not distinguished")
	}
	fixture.mu.Lock()
	fixture.deny = true
	fixture.mu.Unlock()
	if batch, err := selected.Capture(context.Background()); batch != nil || !errors.Is(err, remote.ErrDenied) || strings.Contains(fmt.Sprintf("%+v", err), "native-secret-canary") {
		t.Fatal("denial/privacy", err)
	}
	fixture.mu.Lock()
	fixture.deny = false
	fixture.mu.Unlock()
	fixture.set("format: 1")
	fixture.failNext.Store(int32(codes.DeadlineExceeded))
	if _, err := selected.Capture(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("RPC deadline lost while caller context remains active", err)
	}
}
func TestRemoteObserveRecoveryAndBounds(t *testing.T) {
	fixture := newProtocolFixture(t, false)
	selected := fixture.selectSource(t)
	observer, err := selected.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, observer.Close) })
	initial := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Available })
	fixture.set("format: 1\nproject: {count: 32}")
	fixture.failCode.Store(int32(codes.DeadlineExceeded))
	fixture.push(t, false)
	failed := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Degraded })
	if failed.Generation != initial.Generation || !errors.Is(failed.Failure, context.DeadlineExceeded) {
		t.Fatal("failure erased raw state/cause")
	}
	fixture.failCode.Store(0)
	// There is deliberately no second event. Recovery must remain source-owned.
	recovered := awaitRaw(t, observer, func(state source.State) bool {
		return state.Status == source.Available && state.Generation > initial.Generation
	})
	raw, _, _ := recovered.Batch.RawCopy("document")
	if !strings.Contains(string(raw), "32") {
		t.Fatal("pending failed acquisition not recovered")
	}
	previous := fixture.setups.Load()
	fixture.failCode.Store(int32(codes.DeadlineExceeded))
	fixture.push(t, true)
	awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Degraded })
	fixture.failCode.Store(0)
	recovered = awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Available })
	if fixture.setups.Load() <= previous || recovered.Generation != initial.Generation+1 {
		t.Fatal("registration recovery changed data identity")
	}
	closeOwner(t, observer.Close)
	settings := fixture.settings()
	fixture.mu.Lock()
	settings.Documents = nil
	for index := range 5 {
		id := fmt.Sprintf("doc%d", index)
		fixture.values[id] = strings.Repeat("x", source.MaxDocumentBytes)
		settings.Documents = append(settings.Documents, remote.Document{Name: id, DataID: id})
	}
	fixture.mu.Unlock()
	bounded, err := remote.Select(settings)
	if err != nil {
		t.Fatal(err)
	}
	if batch, err := bounded.Capture(context.Background()); batch != nil || !errors.Is(err, remote.ErrLimit) {
		t.Fatal("aggregate remote prefix escaped", err)
	}
	observing, err := bounded.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, observing.Close) })
	state := awaitRaw(t, observing, func(state source.State) bool { return state.Status == source.Degraded })
	if state.Batch != nil || !errors.Is(state.Failure, remote.ErrLimit) {
		t.Fatal("reconciliation retained over-limit batch", state.Failure)
	}
}
func TestRemoteFrameworkWatchNoSecondAcquisition(t *testing.T) {
	fixture := newProtocolFixture(t, false)
	selected := fixture.selectSource(t)
	live, err := c.Watch(context.Background(), schema(), remotePlan(selected))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, live.Close) })
	first := await(t, live, func(state c.State[project]) bool { return state.Status == c.Ready })
	if fixture.queries.Load() != 1 || fixture.listens.Load() != 1 {
		t.Fatal("Framework refetched the native observation", fixture.queries.Load())
	}
	firstDescription, _ := first.Snapshot.Description()
	fixture.set("format: 1\nproject: {count: 44}")
	fixture.push(t, false)
	changed := await(t, live, func(state c.State[project]) bool {
		value, err := state.Snapshot.ValueCopy()
		return err == nil && state.Status == c.Ready && value.Project.Count == 44
	})
	if fixture.queries.Load() != 2 {
		t.Fatal("push triggered duplicate read", fixture.queries.Load())
	}
	changedDescription, _ := changed.Snapshot.Description()
	if changedDescription.Revision == firstDescription.Revision {
		t.Fatal("changed data reused revision")
	}
	fixture.failCode.Store(int32(codes.DeadlineExceeded))
	fixture.push(t, false)
	await(t, live, func(state c.State[project]) bool { return state.Status == c.Degraded })
	fixture.failCode.Store(0)
	recovered := await(t, live, func(state c.State[project]) bool { return state.Status == c.Ready })
	recoveredDescription, _ := recovered.Snapshot.Description()
	if recoveredDescription.Revision != changedDescription.Revision {
		t.Fatal("status-only recovery reprepared unchanged data")
	}
	fixture.set("format: 1\nproject: {count: invalid}")
	fixture.push(t, false)
	failed := await(t, live, func(state c.State[project]) bool { return state.Status == c.Degraded })
	value, _ := failed.Snapshot.ValueCopy()
	if value.Project.Count != 44 {
		t.Fatal("invalid update corrupted typed state")
	}
	fixture.set("format: 1\nproject: {count: 45}")
	fixture.push(t, false)
	await(t, live, func(state c.State[project]) bool {
		value, err := state.Snapshot.ValueCopy()
		return err == nil && state.Status == c.Ready && value.Project.Count == 45
	})
	closeOwner(t, live.Close)
	eventual(t, func() bool { fixture.mu.Lock(); defer fixture.mu.Unlock(); return len(fixture.streams) == 0 })
}

func TestRemoteProtocolRejectionsAndPushCoalescing(t *testing.T) {
	fixture := newProtocolFixture(t, false)
	selected := fixture.selectSource(t)
	for _, body := range []string{
		`{"resultCode":200,"errorCode":0,"success":true}`,
		`{"resultCode":200,"errorCode":0,"success":true,"content":"x","md5":"bad"}`,
		`{"resultCode":200,"errorCode":0,"success":true,"content":"x","encryptedDataKey":"unsupported"}`,
		`{"resultCode":200,"resultCode":200,"errorCode":0,"success":true,"content":"x"}`,
		string([]byte{0xff}),
	} {
		fixture.mu.Lock()
		fixture.malformed = &wire.Payload{Metadata: &wire.Metadata{Type: "ConfigQueryResponse"}, Body: &anypb.Any{Value: []byte(body)}}
		fixture.mu.Unlock()
		if batch, err := selected.Capture(context.Background()); batch != nil || !errors.Is(err, remote.ErrProtocol) {
			t.Fatal("malformed protocol became raw success", err)
		}
	}
	fixture.mu.Lock()
	fixture.malformed = nil
	fixture.mu.Unlock()
	fixture.set(strings.Repeat("x", source.MaxDocumentBytes+1))
	if batch, err := selected.Capture(context.Background()); batch != nil || !errors.Is(err, remote.ErrLimit) {
		t.Fatal("remote per-document bound failed", err)
	}
	fixture.set("initial")
	observer, err := selected.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, observer.Close) })
	initial := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Available })
	fixture.set("latest")
	for range 64 {
		fixture.push(t, false)
	}
	eventual(t, func() bool { return fixture.acks.Load() >= 64 })
	latest := awaitRaw(t, observer, func(state source.State) bool {
		if state.Batch == nil {
			return false
		}
		raw, _, _ := state.Batch.RawCopy("document")
		return state.Status == source.Available && string(raw) == "latest"
	})
	if latest.Generation != initial.Generation+1 {
		t.Fatal("coalesced repeated push manufactured data generations")
	}
	closeOwner(t, observer.Close)
	eventual(t, func() bool { fixture.mu.Lock(); defer fixture.mu.Unlock(); return len(fixture.streams) == 0 })
}

func TestRegistrationGapReacquiresWithoutPushOrFrameworkRefetch(t *testing.T) {
	for _, framework := range []bool{false, true} {
		t.Run(fmt.Sprint(framework), func(t *testing.T) {
			fixture := newProtocolFixture(t, false)
			fixture.set("format: 1\nproject: {count: 61}")
			fixture.mu.Lock()
			fixture.changeOnListen = "format: 1\nproject: {count: 62}"
			fixture.mu.Unlock()
			settings := fixture.settings()
			settings.ReconcileInterval = 5 * time.Minute
			selected, err := remote.Select(settings)
			if err != nil {
				t.Fatal(err)
			}
			if framework {
				live, err := c.Watch(context.Background(), schema(), remotePlan(selected))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { closeOwner(t, live.Close) })
				await(t, live, func(state c.State[project]) bool {
					value, err := state.Snapshot.ValueCopy()
					return err == nil && state.Status == c.Ready && value.Project.Count == 62
				})
			} else {
				observer, err := selected.Observe(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { closeOwner(t, observer.Close) })
				awaitRaw(t, observer, func(state source.State) bool {
					if state.Batch == nil {
						return false
					}
					raw, _, _ := state.Batch.RawCopy("document")
					return state.Status == source.Available && strings.Contains(string(raw), "62")
				})
			}
			if fixture.queries.Load() != 2 || fixture.setups.Load() != 1 || fixture.acks.Load() != 0 {
				t.Fatal("gap recovery recreated a session, needed a push, or refetched in Framework")
			}
		})
	}
}
func TestRepeatedDirtyRegistrationIsPacedAndCancelable(t *testing.T) {
	fixture := newProtocolFixture(t, false)
	fixture.mu.Lock()
	fixture.dirtyReplies = 10000
	fixture.mu.Unlock()
	settings := fixture.settings()
	settings.RetryDelay = 100 * time.Millisecond
	settings.ReconcileInterval = 5 * time.Minute
	selected, err := remote.Select(settings)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := selected.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, observer.Close) })
	eventual(t, func() bool { return fixture.listens.Load() >= 1 })
	time.Sleep(350 * time.Millisecond)
	closeOwner(t, observer.Close)
	fixture.mu.Lock()
	times := append([]time.Time(nil), fixture.listenTimes...)
	fixture.mu.Unlock()
	if len(times) < 2 || len(times) > 5 {
		t.Fatal("dirty registration did not use one paced loop", len(times))
	}
	for index := 1; index < len(times); index++ {
		if times[index].Sub(times[index-1]) < settings.RetryDelay {
			t.Fatal("dirty response caused an unpaced retry")
		}
	}
}
func TestMalformedListenerKeepsUnknownSlot(t *testing.T) {
	fixture := newProtocolFixture(t, false)
	fixture.mu.Lock()
	fixture.badListen = true
	fixture.mu.Unlock()
	selected := fixture.selectSource(t)
	observer, err := selected.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, observer.Close) })
	failed := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Degraded })
	info := acquisition(t, failed.Failure)
	if !errors.Is(failed.Failure, remote.ErrProtocol) || info.Document != "" || info.Phase != source.ObservePhase || failed.Batch != nil {
		t.Fatal("invalid listener scope acquired a slot or batch")
	}
}
