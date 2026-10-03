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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	rpcRequest "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
	rpcModel "github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/pelletier/go-toml/v2"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

type remoteFixture struct {
	wire.UnimplementedRequestServer
	wire.UnimplementedBiRequestStreamServer
	mu        sync.Mutex
	documents map[string]string
	address   string
	httpURL   string
	queries   atomic.Int32
	mutations atomic.Int32
}

func newRemoteFixture(t *testing.T) *remoteFixture {
	t.Helper()
	fixture := &remoteFixture{documents: map[string]string{}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture.address = listener.Addr().String()
	server := grpc.NewServer()
	wire.RegisterRequestServer(server, fixture)
	wire.RegisterBiRequestStreamServer(server, fixture)
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = server.Serve(listener) }()
	httpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusForbidden) }))
	fixture.httpURL = httpServer.URL + "/nacos"
	t.Cleanup(func() { server.Stop(); <-stopped; httpServer.Close() })
	return fixture
}

func (fixture *remoteFixture) set(key, value string) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.documents[key] = value
}
func remoteDigest(value string) string {
	sum := md5.Sum([]byte(value))
	return hex.EncodeToString(sum[:])
}
func remotePayload(value response.IResponse) (*wire.Payload, error) {
	raw, err := json.Marshal(value)
	return &wire.Payload{Metadata: &wire.Metadata{Type: value.GetResponseType()}, Body: &anypb.Any{Value: raw}}, err
}
func (fixture *remoteFixture) Request(_ context.Context, input *wire.Payload) (*wire.Payload, error) {
	if input == nil || input.Metadata == nil || input.Body == nil {
		return nil, errors.New("invalid fixture request")
	}
	var identity struct {
		RequestID string `json:"requestId"`
	}
	if err := json.Unmarshal(input.Body.Value, &identity); err != nil {
		return nil, err
	}
	base := &response.Response{ResultCode: 200, Success: true, RequestId: identity.RequestID}
	switch input.Metadata.Type {
	case "ServerCheckRequest":
		return remotePayload(&response.ServerCheckResponse{Response: base, ConnectionId: "owned-fixture"})
	case "HealthCheckRequest":
		return remotePayload(&response.HealthCheckResponse{Response: base})
	case "ConfigQueryRequest":
		var query rpcRequest.ConfigQueryRequest
		if err := json.Unmarshal(input.Body.Value, &query); err != nil {
			return nil, err
		}
		fixture.queries.Add(1)
		fixture.mu.Lock()
		value, present := fixture.documents[query.DataId]
		fixture.mu.Unlock()
		if query.Tenant != "explicit-namespace" || query.Group != "CONFIG" {
			return nil, errors.New("document ownership changed")
		}
		if !present {
			return remotePayload(&response.ConfigQueryResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 300, RequestId: identity.RequestID}})
		}
		return remotePayload(&response.ConfigQueryResponse{Response: base, Content: value, Md5: remoteDigest(value)})
	case "ConfigBatchListenRequest":
		var query rpcRequest.ConfigBatchListenRequest
		if err := json.Unmarshal(input.Body.Value, &query); err != nil {
			return nil, err
		}
		changes := []rpcModel.ConfigContext{}
		fixture.mu.Lock()
		for _, key := range query.ConfigListenContexts {
			if value, present := fixture.documents[key.DataId]; present && remoteDigest(value) != key.Md5 {
				changes = append(changes, rpcModel.ConfigContext{Group: key.Group, DataId: key.DataId, Tenant: key.Tenant})
			}
		}
		fixture.mu.Unlock()
		return remotePayload(&response.ConfigChangeBatchListenResponse{Response: base, ChangedConfigs: changes})
	case "ConfigPublishRequest", "ConfigRemoveRequest":
		fixture.mutations.Add(1)
	}
	return nil, errors.New("unsupported fixture operation")
}
func (fixture *remoteFixture) RequestBiStream(stream wire.BiRequestStream_RequestBiStreamServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func (fixture *remoteFixture) options() NacosOptions {
	return NacosOptions{Connection: NacosConnection{Name: "configuration", Namespace: "explicit-namespace", Servers: []NacosServer{{HTTPURL: fixture.httpURL, GRPCAddress: fixture.address}}, AllowInsecure: true, RequestTimeout: time.Second, RetryDelay: time.Millisecond, ReconcileInterval: time.Second}, Documents: []NacosDocument{{Key: NacosKey{Group: "CONFIG", DataID: "primary"}, Kind: Base, Encoding: YAML}}}
}

func TestExplicitRemoteLoad(t *testing.T) {
	for _, encoding := range []Encoding{YAML, TOML} {
		t.Run(string(encoding), func(t *testing.T) {
			fixture := newRemoteFixture(t)
			raw := "value: selected\n"
			if encoding == TOML {
				raw = "value='selected'\n"
			}
			fixture.set("primary", raw)
			options := fixture.options()
			options.Documents[0].Encoding = encoding
			provider, err := Nacos(options)
			if err != nil {
				t.Fatal(err)
			}
			deps := Dependencies{Provider: provider}
			t.Setenv("FATHOMRY_CONFIG_SOURCE", "local")
			t.Setenv("FATHOMRY_CONFIG_PROVIDER", "not-the-selected-provider")
			type app struct {
				Value string `json:"value"`
			}
			declaration := Declaration[app]{Schema: Schema[app]{Version: 1}}
			result, err := Load(context.Background(), declaration, deps)
			if err != nil {
				t.Fatal(err)
			}
			accepted, _ := result.State.Capture()
			value, _ := accepted.ValueCopy()
			if value.Value != "selected" || fixture.queries.Load() == 0 || len(result.Records) != 3 {
				t.Fatal("explicit scenario lost data/evidence")
			}
			for _, record := range result.Records {
				if !record.Info().Released {
					t.Fatal("live source escaped finite load")
				}
			}
			deps.Provider = Provider{}
			before := fixture.queries.Load()
			if result, err := Load(context.Background(), declaration, deps); result.State != nil || !errors.Is(err, ErrDeclaration) || fixture.queries.Load() != before {
				t.Fatal("missing dependency inferred", err)
			}
			if fixture.mutations.Load() != 0 {
				t.Fatal("read-only scenario mutated source")
			}
		})
	}
}

func TestGeneratedRemoteProjects(t *testing.T) {
	if testing.Short() {
		t.Skip("independent generated remote executable qualification")
	}
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	var environment []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(name, "FATHOMRY_") && name != "GOWORK" && name != "GOFLAGS" && name != "GOTOOLCHAIN" {
			environment = append(environment, entry)
		}
	}
	environment = append(environment, "GOWORK=off", "GOTOOLCHAIN=local")
	run := func(t *testing.T, directory, command string, args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		process := exec.CommandContext(ctx, command, args...)
		process.Dir, process.Env = directory, environment
		output, err := process.CombinedOutput()
		if err != nil {
			t.Fatalf("generated remote command %s %v: %v\n%s", filepath.Base(command), args, err, output)
		}
		return output
	}
	job := t.TempDir()
	goName, suffix := "go", ""
	if runtime.GOOS == "windows" {
		goName, suffix = "go.exe", ".exe"
	}
	goTool := filepath.Join(runtime.GOROOT(), "bin", goName)
	cli := filepath.Join(job, "fathomry"+suffix)
	run(t, root, goTool, "build", "-o", cli, "./cmd/fathomry")
	fixture := newRemoteFixture(t)
	for _, encoding := range []string{"yaml", "toml"} {
		t.Run(encoding, func(t *testing.T) {
			directory := filepath.Join(job, encoding)
			run(t, root, cli, "new", directory, "--name=demo", "--module=example.org/demo", "--framework-source", root, "--config-source=remote", "--config-provider=nacos", "--config-format", encoding)
			run(t, directory, goTool, "mod", "tidy")
			binary := filepath.Join(job, "application-"+encoding+suffix)
			run(t, directory, goTool, "build", "-mod=readonly", "-o", binary, "./cmd/demo")
			base, err := os.ReadFile(filepath.Join(directory, "configs", "base."+encoding))
			if err != nil {
				t.Fatal(err)
			}
			fixture.set("shared", string(base))
			selected := map[string]any{}
			for _, name := range []string{"development", "testing", "production"} {
				raw, err := os.ReadFile(filepath.Join(directory, "configs", "environments", name+"."+encoding))
				if err != nil {
					t.Fatal(err)
				}
				fixture.set(name, string(raw))
				selected[name] = map[string]any{"group": "CONFIG", "data_id": name}
			}
			data := map[string]any{
				"connection": map[string]any{"name": "configuration", "namespace": "explicit-namespace", "servers": []map[string]any{{"http_url": fixture.httpURL, "grpc_address": fixture.address}}, "allow_insecure": true, "request_timeout_ns": int64(time.Second)},
				"base":       map[string]any{"group": "CONFIG", "data_id": "shared"}, "environments": selected,
			}
			var raw []byte
			if encoding == "yaml" {
				raw, err = yaml.Marshal(data)
			} else {
				raw, err = toml.Marshal(data)
			}
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, "bootstrap."+encoding)
			write(t, path, string(raw))
			for name := range selected {
				output := run(t, directory, binary, "--bootstrap", path, "--env", name)
				var report struct {
					Status      string `json:"status"`
					Application string `json:"application"`
					Accepted    bool   `json:"configuration_accepted"`
					Records     int    `json:"evidence_records"`
					Worker      bool   `json:"worker_started"`
				}
				if json.Unmarshal(output, &report) != nil || !report.Accepted || report.Status != "configuration_accepted" || report.Application != "demo" || report.Records != 5 || report.Worker {
					t.Fatalf("generated remote bootstrap result: %s", output)
				}
			}
		})
	}
	if fixture.queries.Load() < 12 || fixture.mutations.Load() != 0 {
		t.Fatal("generated remote entry did not perform six read-only layered loads")
	}
}

// This cross-boundary test belongs to the consuming Framework, not its Adapter.
func TestFrameworkNacosConsumption(t *testing.T) {
	fixture := newRemoteFixture(t)
	fixture.set("primary", "access: first\nsecret: secret-first\n")
	options := fixture.options()
	options.ObservationCapacity = 16
	provider, err := Nacos(options)
	if err != nil {
		t.Fatal(err)
	}
	deps := Dependencies{Provider: provider}
	type bundle struct {
		Access string `json:"access"`
		Secret string `json:"secret"`
	}
	declaration := Declaration[bundle]{Schema: Schema[bundle]{Version: 1, Validate: func(_ context.Context, value bundle) error {
		if value.Access == "" || value.Secret != "secret-"+value.Access {
			return errors.New("invalid credential pair")
		}
		return nil
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	loaded, err := Load(ctx, declaration, deps)
	if err != nil {
		t.Fatal(err)
	}
	initial, _ := loaded.State.Capture()
	value, _ := initial.ValueCopy()
	if value.Access != "first" {
		t.Fatal("initial batch lost")
	}
	watch, err := Watch(ctx, declaration, deps, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close(context.Background())
	nextDecision(t, watch, true)
	before, _ := watch.Capture()
	fixture.set("primary", "access: second\nsecret: wrong\n")
	nextDecision(t, watch, false)
	retained, _ := watch.Capture()
	if retained.Description().Revision != before.Description().Revision {
		t.Fatal("invalid native batch republished")
	}
	fixture.set("primary", "access: second\nsecret: secret-second\n")
	nextDecision(t, watch, true)
	current, _ := watch.Capture()
	value, _ = current.ValueCopy()
	if value.Access != "second" || value.Secret != "secret-second" {
		t.Fatal("recovery mixed values")
	}
	if err := watch.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if records, ready := watch.Records(); !ready || len(records) != 3 {
		t.Fatal("scenario cleanup/evidence incomplete")
	}
}

type credentials struct {
	Account string `json:"account"`
	Access  string `json:"access"`
	Secret  string `json:"secret"`
}

func TestIndependentDomainsAndAdoption(t *testing.T) {
	deps := Dependencies{}
	scope, err := resource.New(context.Background(), resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close(context.Background())
	directory := t.TempDir()
	applicationPath, businessPath := filepath.Join(directory, "application.yaml"), filepath.Join(directory, "credentials.yaml")
	write(t, applicationPath, "service: {port: 1}\n")
	write(t, businessPath, "account: fixed\naccess: incomplete\n")
	applicationSource, err := Viper(ViperOptions{Documents: []File{{Path: applicationPath, Kind: Base, Encoding: YAML}}, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	businessSource, err := Viper(ViperOptions{Documents: []File{{Path: businessPath, Kind: Base, Encoding: YAML}}, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("component construction rejected")
	bind := func(name string, policy resource.Policy) resource.Ref[uint16] {
		ref, err := resource.Bind(scope, resource.Binding[uint16, uint16]{
			Name: name, Policy: policy,
			Select: func(view settings.View) (uint16, error) {
				value, present, err := settings.Read(view, "/service/port", func(value uint16) uint16 { return value })
				if err != nil {
					return 0, err
				}
				if !present {
					return 0, errors.New("missing selected port")
				}
				return value, nil
			},
			Clone: func(value uint16) uint16 { return value }, Equal: func(left, right uint16) bool { return left == right },
			Build: func(_ context.Context, value uint16) (*resource.Instance[uint16], error) {
				if value == 3 {
					return nil, failure
				}
				return &resource.Instance[uint16]{Value: value}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	fixed, following := bind("fixed", resource.Fixed), bind("following", resource.Follow)
	applicationDependencies := deps
	applicationDependencies.Resources = scope
	applicationDependencies.Provider = applicationSource
	application, err := Watch(context.Background(), Declaration[model]{Schema: modelSchema()}, applicationDependencies, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close(context.Background())
	deps.Provider = businessSource
	business, err := Watch(context.Background(), Declaration[credentials]{
		Schema: Schema[credentials]{Version: 1, Validate: func(_ context.Context, value credentials) error {
			if value.Account != "fixed" || value.Access == "" || value.Secret != "secret-"+value.Access {
				return errors.New("invalid related credential bundle")
			}
			return nil
		}},
	}, deps, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer business.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	accepted := nextDecision(t, application, true)
	if accepted.Status.Adoption == nil || accepted.Status.AdoptionError != nil {
		t.Fatal("explicit adoption not coordinated")
	}
	if err := accepted.Status.Adoption.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if event := nextDecision(t, business, false); event.Err == nil {
		t.Fatal("initial invalid credentials accepted")
	}
	if _, err := business.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("invalid initial business domain was ready")
	}
	before, _ := application.Capture()
	write(t, businessPath, "account: fixed\naccess: first\nsecret: secret-first\n")
	nextDecision(t, business, true)
	businessBefore, _ := business.Capture()
	write(t, businessPath, "account: fixed\naccess: second\nsecret: wrong\n")
	nextDecision(t, business, false)
	retained, _ := business.Capture()
	if retained.Description().Revision != businessBefore.Description().Revision {
		t.Fatal("invalid credentials replaced last-good")
	}
	write(t, businessPath, "account: fixed\naccess: second\nsecret: secret-second\n")
	nextDecision(t, business, true)
	after, _ := application.Capture()
	if after.Description().Revision != before.Description().Revision {
		t.Fatal("business variables republished application")
	}
	bundle, _ := business.Capture()
	value, _ := bundle.ValueCopy()
	if value.Access != "second" || value.Secret != "secret-second" || value.Account != "fixed" {
		t.Fatal("credential fields mixed")
	}
	old, err := following.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	write(t, applicationPath, "service: {port: 2}\n")
	updated := nextDecision(t, application, true)
	if err := updated.Status.Adoption.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		ref      resource.Ref[uint16]
		expected uint16
	}{{fixed, 1}, {following, 2}} {
		lease, err := entry.ref.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got, err := lease.Value()
		if err != nil || got != entry.expected {
			t.Fatal("wrong per-binding policy")
		}
		if err := lease.Release(); err != nil {
			t.Fatal(err)
		}
	}
	oldValue, _ := old.Value()
	if oldValue != 1 {
		t.Fatal("borrowed generation retargeted")
	}
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	write(t, applicationPath, "service: {port: 3}\n")
	failedAdoption := nextDecision(t, application, true)
	if err := failedAdoption.Status.Adoption.Wait(ctx); !errors.Is(err, failure) {
		t.Fatal("construction failure hidden", err)
	}
	latest, _ := application.Capture()
	latestValue, _ := latest.ValueCopy()
	if latestValue.Service.Port != 3 {
		t.Fatal("adoption failure rewrote accepted settings")
	}
	lease, err := following.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := lease.Value()
	if got != 2 {
		t.Fatal("failed adoption replaced usable instance")
	}
	_ = lease.Release()
}

func TestNacosBootstrap(t *testing.T) {
	for _, encoding := range []Encoding{YAML, TOML, JSON} {
		t.Run(string(encoding), func(t *testing.T) {
			fixture := newRemoteFixture(t)
			fixture.set("shared", `{"value":"base"}`)
			fixture.set("production", `{"value":"production"}`)
			input := map[string]any{
				"connection":   map[string]any{"name": "configuration", "namespace": "explicit-namespace", "servers": []map[string]any{{"http_url": fixture.httpURL, "grpc_address": fixture.address}}, "allow_insecure": true},
				"base":         map[string]any{"group": "CONFIG", "data_id": "shared"},
				"environments": map[string]any{"production": map[string]any{"group": "CONFIG", "data_id": "production"}},
			}
			var raw []byte
			var err error
			switch encoding {
			case YAML:
				raw, err = yaml.Marshal(input)
			case TOML:
				raw, err = toml.Marshal(input)
			case JSON:
				raw, err = json.Marshal(input)
			}
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "bootstrap."+string(encoding))
			write(t, path, string(raw))
			provider, records, err := PrepareNacos(context.Background(), NacosBootstrapOptions{File: path, Environment: "production", Encoding: JSON})
			if err != nil || len(records) != 2 || fixture.queries.Load() != 0 {
				t.Fatal("bootstrap opened a remote source or lost records", err)
			}
			type project struct {
				Value string `json:"value"`
			}
			loaded, err := Load(context.Background(), Declaration[project]{Schema: Schema[project]{Version: 1}}, Dependencies{Provider: provider})
			if err != nil {
				t.Fatal(err)
			}
			accepted, _ := loaded.State.Capture()
			value, _ := accepted.ValueCopy()
			if value.Value != "production" {
				t.Fatal("environment document not selected")
			}
			before := fixture.queries.Load()
			refused, records, err := PrepareNacos(context.Background(), NacosBootstrapOptions{File: path, Environment: "undeclared", Encoding: JSON})
			if err == nil || refused.state != nil || len(records) != 2 || fixture.queries.Load() != before {
				t.Fatal("unknown environment guessed")
			}
			for _, options := range []NacosBootstrapOptions{
				{File: path, Environment: "../production", Encoding: JSON},
				{File: path, Environment: "production", Encoding: "xml"},
				{File: path + ".unsupported", Environment: "production", Encoding: JSON},
			} {
				if _, records, err := PrepareNacos(context.Background(), options); err == nil || len(records) != 0 {
					t.Fatal("invalid bootstrap inputs reached acquisition")
				}
			}
			write(t, path, "invalid original input")
			if provider, records, err := PrepareNacos(context.Background(), NacosBootstrapOptions{File: path, Environment: "production", Encoding: JSON}); err == nil || provider.state != nil || len(records) != 2 {
				t.Fatal("invalid bootstrap lost refusal or cleanup")
			}
		})
	}
}
