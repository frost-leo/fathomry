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

package temporal_test

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
)

type localResolverBuilder struct {
	scheme, address string
	schemes, builds atomic.Int32
	options         resolver.BuildOptions
	target          resolver.Target
	callback        resolver.ClientConn
}

func (builder *localResolverBuilder) Scheme() string                   { builder.schemes.Add(1); return builder.scheme }
func (*localResolverBuilder) OverrideAuthority(resolver.Target) string { return "not-selected" }
func (builder *localResolverBuilder) Build(target resolver.Target, client resolver.ClientConn, options resolver.BuildOptions) (resolver.Resolver, error) {
	builder.builds.Add(1)
	builder.options, builder.target, builder.callback = options, target, client
	if err := client.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: builder.address}}}); err != nil {
		return nil, err
	}
	return &localResolver{}, nil
}

type localResolver struct{}

func (*localResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (*localResolver) Close()                                {}

type unsupportedCompression struct{ sdk.GrpcCompression }

func TestPreparationFreezesExactOptionsWithoutNativeCallbacks(t *testing.T) {
	builder := &localResolverBuilder{scheme: "fixture"}
	runtime := temporal.RuntimeOptions{ResolverBuilders: []temporal.ResolverBinding{{Scheme: "fixture", Builder: builder}}, UserAgent: "fixture/1"}
	options := temporal.OptionsV1{Name: "prepared", Endpoint: "fixture:///source", Namespace: "selected", Plaintext: true, RPCs: []string{countMethod}}
	layer := resource.Layer{Kind: resource.Local, Content: []byte("max_active: 3\nqueued_calls: 2\nmax_request_bytes: 2048\nmax_response_bytes: 4096\n")}
	prepared, err := temporal.PrepareV1(options, runtime, layer)
	if err != nil {
		t.Fatal(err)
	}
	if builder.schemes.Load() != 0 || builder.builds.Load() != 0 {
		t.Fatal("offline preparation entered resolver")
	}
	runtime.ResolverBuilders[0].Scheme = "changed"
	options.RPCs[0] = "changed"
	layer.Content[0] = 'X'
	effective, budget := prepared.Options(), prepared.Metadata()
	if effective.Endpoint != "fixture:///source" || effective.Namespace != "selected" || effective.RPCs[0] != countMethod ||
		effective.MaxActive != 3 || effective.QueuedCalls != 2 || budget.MaxActive != 3 || budget.QueuedCalls != 2 ||
		budget.WorkBytes != 2048+4096+(16<<10) || budget.Limits.Bytes != 3*budget.WorkBytes ||
		budget.Limits.QueuedBytes != 2*budget.WorkBytes || budget.SourceBytes < budget.WorkBytes || budget.EvidenceBytes != temporal.ExecutionEvidenceBytes {
		t.Fatal("prepared metadata/options differ from selected configuration")
	}
	effective.RPCs[0] = "changed"
	if prepared.Options().RPCs[0] != countMethod {
		t.Fatal("effective options are not detached")
	}
	options.Endpoint, options.RPCs = "127.0.0.1:7233", []string{countMethod}
	for _, runtime := range []temporal.RuntimeOptions{
		{UserAgent: "invalid\nmetadata"}, {UserAgent: strings.Repeat("a", 257)},
		{ResolverBuilders: []temporal.ResolverBinding{{Scheme: "Fixture", Builder: builder}}},
		{ResolverBuilders: []temporal.ResolverBinding{{Scheme: "fixture"}}},
		{ConnectionOptions: sdk.ConnectionOptions{GrpcCompression: &unsupportedCompression{}}},
	} {
		if _, err := temporal.PrepareV1(options, runtime); err == nil {
			t.Fatal("invalid structured transport accepted")
		}
	}
}

func TestSourceLocalResolverExplicitDialerAndUserAgent(t *testing.T) {
	builder := &localResolverBuilder{scheme: "fixture", address: "logical-backend"}
	var endpoint string
	var calls, dialed atomic.Int32
	runtime := temporal.RuntimeOptions{ResolverBuilders: []temporal.ResolverBinding{{Scheme: "fixture", Builder: builder}}, UserAgent: "fixture-agent/1",
		ContextDialer: func(ctx context.Context, address string) (net.Conn, error) {
			dialed.Add(1)
			if address != "logical-backend" {
				return nil, errors.New("resolver address not passed to explicit dialer")
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
		}}
	runtime.ConnectionOptions.Authority = "selected-authority"
	fixture := newRuntimeFixture(t, 2, runtime, nil, func(options *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
		endpoint = options.Endpoint
		options.Endpoint = "fixture://resolver-authority/selected"
		server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			headers, _ := metadata.FromIncomingContext(ctx)
			if !strings.HasPrefix(headers.Get("user-agent")[0], "fixture-agent/1 ") ||
				!reflect.DeepEqual(headers.Get(":authority"), []string{"selected-authority"}) {
				return nil, errors.New("controlled metadata lost")
			}
			calls.Add(1)
			return next(ctx, request)
		}
	})
	if builder.builds.Load() != 1 || builder.schemes.Load() != 1 || builder.target.URL.Host != "resolver-authority" || builder.target.Endpoint() != "selected" ||
		builder.options.Authority != "selected-authority" || !builder.options.DisableServiceConfig || dialed.Load() != 1 {
		t.Fatal("native source-local resolver path changed")
	}
	service := fixture.client.WorkflowService(fault.Correlation{Call: "resolved"})
	result, err := service.CountWorkflowExecutions(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if err != nil || result.Count != 7 {
		t.Fatal("explicit selected dialer did not reach local peer", err)
	}
	before := calls.Load()
	if _, err := service.CountWorkflowExecutions(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: "other"}); !errors.Is(err, temporal.ErrAuthority) || calls.Load() != before {
		t.Fatal("custom resolver/dialer bypassed namespace", err)
	}
	prepared, err := temporal.PrepareFromExistingV1(temporal.OptionsV1{Name: "derived", Namespace: "other", MaxActive: 1, RPCs: []string{countMethod}}, temporal.RuntimeOptions{}, fixture.client, fixture.client.RPCReservation())
	if err != nil {
		t.Fatal("offline derived custom target rejected", err)
	}
	if builder.builds.Load() != 1 || dialed.Load() != 1 {
		t.Fatal("derived preparation entered transport hooks")
	}
	selected := prepared.Selection()
	derived, err := resource.Assemble(context.Background(), context.Background(), "derived", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = derived.Close(context.Background()) })
	inbox, err := invocation.NewInbox[temporal.RPCResult](1, prepared.Metadata().RPCEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	child, err := temporal.Bind(derived, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	count, err := child.WorkflowService(fault.Correlation{Call: "derived"}).CountWorkflowExecutions(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: "other"})
	if err != nil || count.Count != 11 || builder.builds.Load() != 1 || dialed.Load() != 1 {
		t.Fatal("derived source did not inherit exact custom transport", err)
	}
	if err := derived.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := builder.callback.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: "logical-backend"}}}); !errors.Is(err, net.ErrClosed) {
		t.Fatal("retained resolver callback survived release", err)
	}
}

func TestSourceIgnoresGlobalResolverOverrides(t *testing.T) {
	const marker = "FATHOMRY_TEST_TEMPORAL_RESOLVER_CHILD"
	if os.Getenv(marker) == "" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(executable, "-test.run=^TestSourceIgnoresGlobalResolverOverrides$")
		command.Env = append(os.Environ(), marker+"=1", "HTTPS_PROXY=http://127.0.0.1:1", "HTTP_PROXY=http://127.0.0.1:1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("isolated resolver control: %v\n%s", err, output)
		}
		return
	}
	poison := &localResolverBuilder{scheme: "dns", address: "127.0.0.1:1"}
	resolver.Register(poison)
	custom := &localResolverBuilder{scheme: "ambient", address: "127.0.0.1:1"}
	resolver.Register(custom)
	resolver.SetDefaultScheme("ambient")
	fixture := newFixture(t, 1)
	result, err := fixture.client.WorkflowService(fault.Correlation{Call: "pinned"}).CountWorkflowExecutions(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if err != nil || result.Count != 7 || poison.builds.Load() != 0 || custom.builds.Load() != 0 {
		t.Fatal("global resolver/proxy settings changed source route", err)
	}
}
