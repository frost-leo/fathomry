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
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

func TestNativeResolverTargetsPreserveOwnedRPCs(t *testing.T) {
	for _, scheme := range []string{"", "dns:///", "passthrough:///"} {
		t.Run(scheme, func(t *testing.T) {
			fixture := newFixture(t, 4, func(options *temporal.OptionsV1, _ *resource.Limits, _ *rpcServer) {
				_, port, err := net.SplitHostPort(options.Endpoint)
				if err != nil {
					t.Fatal(err)
				}
				options.Endpoint = scheme + net.JoinHostPort("localhost", port)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			service := fixture.client.WorkflowService(fault.Correlation{Call: "resolver-target"})
			result, err := service.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
			if err != nil || result.GetCount() != 7 {
				t.Fatal("native resolver did not reach the controlled service", err)
			}
			if _, err := service.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "other"}); !errors.Is(err, temporal.ErrAuthority) {
				t.Fatal("resolver target bypassed the namespace boundary", err)
			}
			if err := fixture.assembly.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := service.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"}); err == nil {
				t.Fatal("retained resolver client survived source release")
			}
		})
	}
}

func TestLazyAcquisitionDefersDiscoveryUntilAdmittedUse(t *testing.T) {
	var discovery atomic.Int32
	var lazyMode atomic.Bool
	plugin := &controlledClientPlugin{create: func(ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
		lazyMode.Store(options.Lazy)
		return next(ctx, options)
	}}
	fixture := newRuntimeFixture(t, 2, temporal.RuntimeOptions{Plugins: []sdk.Plugin{plugin}}, nil,
		func(options *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
			options.Lazy = true
			server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
				if _, ok := request.(*workflowservice.GetSystemInfoRequest); ok {
					discovery.Add(1)
				}
				return next(ctx, request)
			}
		})
	if !lazyMode.Load() || discovery.Load() != 0 || fixture.client.Profile().ServiceVersion.Kind != compatibility.UnknownFact {
		t.Fatal("lazy acquisition performed or invented discovery")
	}
	executions, _ := executionBinding(t, fixture)
	reply, err := executions.CountWorkflow(context.Background(), fault.Correlation{Call: "lazy-first-use"}, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if err != nil || reply.Count != 7 || discovery.Load() != 1 {
		t.Fatal("admitted lazy initialization failed", err)
	}
	if fact := fixture.client.Profile().ServiceVersion; fact.Kind != compatibility.Observed || fact.Value != "1.32.0" {
		t.Fatal("lazy discovery was not reflected as observed knowledge")
	}
}

func TestLazyClientCanBeOwnedBeforeServerExists(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, lazy := range []bool{true, false} {
		selected, err := temporal.Select(temporal.OptionsV1{Name: "offline", Endpoint: address, Namespace: "test", Plaintext: true, Lazy: lazy, ConnectTimeout: 40 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		assembly, err := resource.Assemble(ctx, ctx, "offline", selected)
		if (err == nil) != lazy {
			t.Fatal("eager/lazy offline control differs", err)
		}
		if assembly != nil {
			if err := assembly.Close(ctx); err != nil {
				t.Fatal("offline client not released", err)
			}
		}
	}
}

func TestDerivedNamespaceClientsShareOwnedTransportAndRetainParent(t *testing.T) {
	var originalPeer string
	var peerMismatch atomic.Bool
	fixture := newFixture(t, 8, func(options *temporal.OptionsV1, limits *resource.Limits, server *rpcServer) {
		options.MaxActive = 4
		limits.Active = 4
		limits.Bytes *= 4
		server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if _, ok := request.(*workflowservice.CountWorkflowExecutionsRequest); ok {
				connection, _ := peer.FromContext(ctx)
				if originalPeer == "" {
					originalPeer = connection.Addr.String()
				} else if originalPeer != connection.Addr.String() {
					peerMismatch.Store(true)
				}
			}
			return next(ctx, request)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	parentEnvelope := fixture.client.RPCReservation()
	var sharedMode atomic.Bool
	plugin := &controlledClientPlugin{create: func(ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
		sharedMode.Store(options.FromExisting != nil && !options.Lazy)
		return next(ctx, options)
	}}
	selected, err := temporal.SelectFromExisting(temporal.OptionsV1{Name: "other", Namespace: "other", MaxActive: 1, RPCs: []string{countMethod}}, temporal.RuntimeOptions{Plugins: []sdk.Plugin{plugin}}, fixture.client, 2*parentEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, resource.Limits{Active: 1, Bytes: 2 * parentEnvelope, MaxLeases: 8})
	assembly, err := resource.Assemble(ctx, ctx, "derived", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = assembly.Close(context.Background()) })
	inbox, err := invocation.NewInbox[temporal.RPCResult](4, 4096)
	if err != nil {
		t.Fatal(err)
	}
	client, err := temporal.Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !sharedMode.Load() {
		t.Fatal("native plugin FromExisting mode was hidden")
	}
	first, err := fixture.client.WorkflowService(fault.Correlation{Call: "parent-before"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	if err != nil || first.Count != 7 {
		t.Fatal("parent control failed", err)
	}
	second, err := client.WorkflowService(fault.Correlation{Call: "child"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "other"})
	if err != nil || second.Count != 11 || peerMismatch.Load() {
		t.Fatal("derived namespace did not share the exact transport", err)
	}
	if _, err := client.WorkflowService(fault.Correlation{Call: "wrong-namespace"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"}); !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("derived namespace could retarget", err)
	}
	if err := fixture.assembly.Close(ctx); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("parent released its derived client", err)
	}
	second, err = client.WorkflowService(fault.Correlation{Call: "child-after-parent-close"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "other"})
	if err != nil || second.Count != 11 {
		t.Fatal("closing parent interrupted held derived ownership", err)
	}
	releaseServiceEvidence(t, inbox)
	if err := assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal("derived close did not release parent dependency", err)
	}
}

func TestDerivedClientRejectsLayeredLazyBeforeDiscovery(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		t.Run(map[bool]string{false: "eager-control", true: "lazy-layer-refused"}[lazy], func(t *testing.T) {
			var discovery atomic.Int32
			fixture := newFixture(t, 1, func(options *temporal.OptionsV1, limits *resource.Limits, server *rpcServer) {
				options.Lazy, options.MaxActive = true, 2
				limits.Active, limits.Bytes = 2, 4*limits.Bytes
				server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
					if _, ok := request.(*workflowservice.GetSystemInfoRequest); ok {
						discovery.Add(1)
					}
					return next(ctx, request)
				}
			})
			content := []byte("lazy: false\n")
			if lazy {
				content = []byte("lazy: true\n")
			}
			envelope := fixture.client.RPCReservation()
			selected, err := temporal.SelectFromExisting(temporal.OptionsV1{Name: "layered", Namespace: "other", MaxActive: 1}, temporal.RuntimeOptions{}, fixture.client, 2*envelope,
				resource.Layer{Kind: resource.Local, Content: content})
			if err != nil {
				t.Fatal(err)
			}
			selected = resource.WithLimits(selected, resource.Limits{Active: 1, Bytes: 2 * envelope, MaxLeases: 8})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			assembly, err := resource.Assemble(ctx, ctx, "layered-mode", selected)
			if assembly != nil {
				t.Cleanup(func() {
					if err := assembly.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
			}
			if lazy {
				if !errors.Is(err, temporal.ErrAuthority) || discovery.Load() != 0 {
					t.Fatalf("resolved lazy mode was executed eagerly: error=%v discovery=%d", err, discovery.Load())
				}
			} else if err != nil || discovery.Load() != 1 {
				t.Fatal("native eager derivation control failed", err)
			}
		})
	}
}

func TestDerivedClientPluginCanReplaceContextWithoutLosingOwner(t *testing.T) {
	for _, replaceContext := range []bool{false, true} {
		t.Run(map[bool]string{false: "forward-context", true: "replace-context"}[replaceContext], func(t *testing.T) {
			var discovery atomic.Int32
			fixture := newFixture(t, 1, func(options *temporal.OptionsV1, limits *resource.Limits, server *rpcServer) {
				options.Lazy, options.MaxActive = true, 2
				limits.Active, limits.Bytes = 2, 4*limits.Bytes
				server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
					if _, ok := request.(*workflowservice.GetSystemInfoRequest); ok {
						discovery.Add(1)
					}
					return next(ctx, request)
				}
			})
			plugin := &controlledClientPlugin{create: func(ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
				if replaceContext {
					ctx = context.Background()
				}
				return next(ctx, options)
			}}
			envelope := fixture.client.RPCReservation()
			selected, err := temporal.SelectFromExisting(temporal.OptionsV1{Name: "derived-plugin", Namespace: "other", MaxActive: 1, RPCs: []string{countMethod}},
				temporal.RuntimeOptions{Plugins: []sdk.Plugin{plugin}}, fixture.client, 2*envelope)
			if err != nil {
				t.Fatal(err)
			}
			selected = resource.WithLimits(selected, resource.Limits{Active: 1, Bytes: 2 * envelope, MaxLeases: 8})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			assembly, err := resource.Assemble(ctx, ctx, "derived-plugin", selected)
			if assembly != nil {
				t.Cleanup(func() {
					if err := assembly.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
			}
			if err != nil || discovery.Load() != 1 {
				t.Fatalf("plugin context lost derived acquisition ownership: error=%v discovery=%d", err, discovery.Load())
			}
			inbox, err := invocation.NewInbox[temporal.RPCResult](1, 1024)
			if err != nil {
				t.Fatal(err)
			}
			client, err := temporal.Bind(assembly, selected, inbox, nil)
			if err != nil {
				t.Fatal(err)
			}
			value, err := client.WorkflowService(fault.Correlation{Call: "derived-context"}).CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "other"})
			if err != nil || value.Count != 11 {
				t.Fatal("derived namespace control failed", err)
			}
			releaseServiceEvidence(t, inbox)
		})
	}
}
