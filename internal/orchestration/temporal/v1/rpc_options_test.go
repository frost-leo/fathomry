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
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func exerciseBoundedServiceOptions(ctx context.Context, service workflowservice.WorkflowServiceClient, transmitted *atomic.Int32) error {
	request := &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"}
	var header, trailer metadata.MD
	var remote peer.Peer
	response, err := service.CountWorkflowExecutions(ctx, request, grpc.StaticMethod(), grpc.Header(&header), grpc.Trailer(&trailer), grpc.Peer(&remote),
		grpc.WaitForReady(false), grpc.MaxCallSendMsgSize(1024), grpc.MaxCallRecvMsgSize(1024))
	if err != nil || response.GetCount() != 7 || header.Get("fixture-header")[0] != "header" || trailer.Get("fixture-trailer")[0] != "trailer" || remote.Addr == nil {
		return fmt.Errorf("safe native options lost: %w", err)
	}
	before := transmitted.Load()
	for _, option := range []grpc.CallOption{grpc.UseCompressor("gzip"), grpc.Header(nil), grpc.Trailer(nil), grpc.Peer(nil), grpc.MaxCallSendMsgSize(1025), grpc.MaxCallRecvMsgSize(1025)} {
		if _, err := service.CountWorkflowExecutions(ctx, request, option); err == nil || transmitted.Load() != before {
			return errors.New("unsafe or invalid option transmitted")
		}
	}
	tooMany := make([]grpc.CallOption, 32)
	for index := range tooMany {
		tooMany[index] = grpc.StaticMethod()
	}
	if _, err := service.CountWorkflowExecutions(ctx, request, tooMany...); !errors.Is(err, temporal.ErrLimit) || transmitted.Load() != before {
		return errors.New("call-option saturation transmitted")
	}
	if _, err := service.CountWorkflowExecutions(ctx, request, grpc.MaxCallSendMsgSize(1), grpc.MaxCallSendMsgSize(1024)); err == nil || transmitted.Load() != before {
		return errors.New("tighter send limit was overwritten")
	}
	if _, err := service.CountWorkflowExecutions(ctx, request, grpc.MaxCallRecvMsgSize(1), grpc.MaxCallRecvMsgSize(1024)); err == nil {
		return errors.New("tighter receive limit was overwritten")
	}
	return nil
}

func boundedServiceResponse(transmitted *atomic.Int32, ctx context.Context, request any, next grpc.UnaryHandler) (any, error) {
	if _, ok := request.(*workflowservice.CountWorkflowExecutionsRequest); ok {
		transmitted.Add(1)
		if err := grpc.SetHeader(ctx, metadata.Pairs("fixture-header", "header")); err != nil {
			return nil, err
		}
		grpc.SetTrailer(ctx, metadata.Pairs("fixture-trailer", "trailer"))
		return &workflowservice.CountWorkflowExecutionsResponse{Count: 7}, nil
	}
	return next(ctx, request)
}

func TestDirectServiceCallOptionBoundsAndOutputs(t *testing.T) {
	var transmitted atomic.Int32
	fixture := newFixture(t, 16, func(_ *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
		server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			return boundedServiceResponse(&transmitted, ctx, request, next)
		}
	})
	if err := exerciseBoundedServiceOptions(context.Background(), fixture.client.WorkflowService(fault.Correlation{Call: "options"}), &transmitted); err != nil {
		t.Fatal(err)
	}
}

func TestCallbackServiceCallOptionBoundsOutputsAndExpiry(t *testing.T) {
	var transmitted atomic.Int32
	borrowed := reviewReleasedCallbackFixture(t, "GetSystemInfo", temporal.RuntimeOptions{}, func(ctx context.Context, client sdk.Client) error {
		return exerciseBoundedServiceOptions(ctx, client.WorkflowService(), &transmitted)
	}, func(ctx context.Context, request any, next grpc.UnaryHandler) (any, error) {
		return boundedServiceResponse(&transmitted, ctx, request, next)
	})
	before := transmitted.Load()
	var header metadata.MD
	_, err := borrowed.WorkflowService().CountWorkflowExecutions(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"}, grpc.Header(&header))
	if !errors.Is(err, temporal.ErrAuthority) || transmitted.Load() != before || header != nil {
		t.Fatal("expired callback service retained output-container authority", err)
	}
}

type mappedServiceError struct{ cause error }

func (*mappedServiceError) Error() string     { return "mapped direct service error" }
func (err *mappedServiceError) Unwrap() error { return err.cause }

func TestDirectServiceErrorMapperDoesNotChangeNativeEvidence(t *testing.T) {
	fixture := newFixture(t, 4)
	var mappings atomic.Int32
	mapped := temporal.WithRPCErrorMapper(fixture.client, func(err error) error {
		mappings.Add(1)
		return &mappedServiceError{cause: err}
	})
	service := mapped.WorkflowService(fault.Correlation{Call: "mapped-service"})
	if _, err := service.CountWorkflowExecutions(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"}); err != nil || mappings.Load() != 0 {
		t.Fatal("successful RPC was passed to error mapper", err)
	}
	receive(t, fixture)
	_, err := service.SignalWorkflowExecution(context.Background(), &workflowservice.SignalWorkflowExecutionRequest{Namespace: "test"})
	var translated *mappedServiceError
	if !errors.As(err, &translated) || mappings.Load() != 1 {
		t.Fatal("direct RPC error did not pass through mapper", err)
	}
	record := receive(t, fixture)
	if record.Err() == nil || errors.As(record.Err(), &translated) {
		t.Fatal("direct presentation mapper changed independently retained native evidence")
	}
	_, err = fixture.client.WorkflowService(fault.Correlation{Call: "unmapped"}).CountWorkflowExecutions(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: "other"})
	if !errors.Is(err, temporal.ErrAuthority) || errors.As(err, &translated) || mappings.Load() != 1 {
		t.Fatal("mapper mutated the original facade", err)
	}
}

type reentrantServiceTraffic func(context.Context, string, any, any) error

func (hook reentrantServiceTraffic) CheckCallAllowed(ctx context.Context, method string, request, response any) error {
	return hook(ctx, method, request, response)
}

func TestFreshRPCIDsSupportSameViewReentry(t *testing.T) {
	var service workflowservice.WorkflowServiceClient
	traffic := reentrantServiceTraffic(func(ctx context.Context, method string, _, _ any) error {
		if strings.HasSuffix(method, "/CountWorkflowExecutions") {
			_, err := service.GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
			return err
		}
		return nil
	})
	fixture := newRuntimeFixture(t, 4, temporal.RuntimeOptions{TrafficController: traffic}, nil, func(options *temporal.OptionsV1, limits *resource.Limits, _ *rpcServer) {
		options.RPCs = append(options.RPCs, workflowPrefix+"GetSystemInfo")
		limits.Bytes *= 2
	})
	executions, _ := executionBinding(t, fixture)
	_, raw, err := temporal.WithWorkEnvelope(executions, fixture.client, 2*fixture.client.RPCReservation())
	if err != nil {
		t.Fatal(err)
	}
	service = temporal.WithFreshRPCIDs(raw).WorkflowService(fault.Correlation{Call: "view", Owner: "consumer"})
	if _, err := service.CountWorkflowExecutions(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"}); err != nil {
		t.Fatal("same generated view could not reenter its live family", err)
	}
	child, parent := receive(t, fixture), receive(t, fixture)
	if !child.Nested {
		child, parent = parent, child
	}
	if !child.Nested || parent.Nested || child.Context.Correlation.Call == parent.Context.Correlation.Call ||
		child.Context.Correlation.Parent != parent.Context.Correlation.Call || child.Context.Correlation.Owner != "consumer" || parent.Context.Correlation.Owner != "consumer" {
		t.Fatal("fresh raw identities lost inherited parent or explicit owner")
	}
}

func nativePayloadLimitResponse(ctx context.Context, request any, next grpc.UnaryHandler) (any, error) {
	if _, ok := request.(*workflowservice.CountWorkflowExecutionsRequest); ok {
		return &workflowservice.CountWorkflowExecutionsResponse{Count: 7, Groups: []*workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{{GroupValues: []*commonpb.Payload{{Data: make([]byte, 700)}}}}}, nil
	}
	return next(ctx, request)
}

func TestDirectRawRetainsNativeMessageLimit(t *testing.T) {
	fixture := newRuntimeFixture(t, 4, temporal.RuntimeOptions{ConnectionOptions: sdk.ConnectionOptions{MaxPayloadSize: 512}}, nil,
		func(_ *temporal.OptionsV1, _ *resource.Limits, server *rpcServer) {
			server.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
				return nativePayloadLimitResponse(ctx, request, next)
			}
		})
	executions, _ := executionBinding(t, fixture)
	request := &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"}
	if _, err := executions.CountWorkflow(context.Background(), fault.Correlation{Call: "native-limit"}, request); err == nil {
		t.Fatal("native high-level message-limit control did not reject oversized response")
	}
	if _, err := fixture.client.WorkflowService(fault.Correlation{Call: "raw-limit"}).CountWorkflowExecutions(context.Background(), request); err == nil {
		t.Fatal("direct raw view replaced tighter native message limit with source default")
	}
}

func TestCallbackRawRetainsNativeMessageLimit(t *testing.T) {
	reviewReleasedCallbackFixture(t, "GetSystemInfo", temporal.RuntimeOptions{ConnectionOptions: sdk.ConnectionOptions{MaxPayloadSize: 512}},
		func(ctx context.Context, client sdk.Client) error {
			_, err := client.WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
			if err == nil {
				return errors.New("callback raw view replaced tighter native message limit")
			}
			return nil
		}, nativePayloadLimitResponse)
}
