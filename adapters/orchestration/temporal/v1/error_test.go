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

package temporal

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type semanticTestInterceptor struct {
	interceptor.ClientInterceptorBase
	cause error
}

func (extension *semanticTestInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &semanticTestOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}, cause: extension.cause}
}

type semanticTestOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	cause error
}

func (extension *semanticTestOutbound) SignalWorkflow(context.Context, *interceptor.ClientSignalWorkflowInput) error {
	return extension.cause
}

func (extension *semanticTestOutbound) UpdateWorkflow(context.Context, *interceptor.ClientUpdateWorkflowInput) (sdk.WorkflowUpdateHandle, error) {
	return nil, extension.cause
}

func (extension *semanticTestOutbound) UpdateWithStartWorkflow(context.Context, *interceptor.ClientUpdateWithStartWorkflowInput) (sdk.WorkflowUpdateHandle, error) {
	return nil, extension.cause
}

func TestUpdateInterceptorsPreserveExactNativeFailure(t *testing.T) {
	payloads, err := converter.GetDefaultDataConverter().ToPayloads("custom detail")
	if err != nil {
		t.Fatal(err)
	}
	custom := &customLazyState{failure: &failurepb.Failure{Message: "custom failure", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
		ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "custom", NonRetryable: true, Details: payloads}}}}
	for _, test := range []struct {
		name       string
		original   error
		conversion converter.FailureConverter
	}{
		{"nonretryable", sdktemporal.NewNonRetryableApplicationError("native failure", "fixture", nil, "detail"), sdktemporal.GetDefaultFailureConverter()},
		{"canceled", sdktemporal.NewCanceledError("detail"), sdktemporal.GetDefaultFailureConverter()},
		{"timeout", sdktemporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_HEARTBEAT, nil, "detail"), sdktemporal.GetDefaultFailureConverter()},
		{"custom", &cooperatingLazyError{state: custom}, &customFailureConverter{state: custom}},
	} {
		for _, mode := range []string{"signal-control", "update", "update-with-start"} {
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				fixture := newTestFixture(t, NativeOptions{FailureConverter: test.conversion, Interceptors: []interceptor.ClientInterceptor{&semanticTestInterceptor{cause: test.original}}}, nil)
				client := fixture.owner.Client()
				options := sdk.UpdateWorkflowOptions{WorkflowID: "workflow", UpdateName: "update", WaitForStage: sdk.WorkflowUpdateStageCompleted}
				var err error
				switch mode {
				case "signal-control":
					err = client.SignalWorkflow(fixture.ctx, "workflow", "run", "signal", nil)
				case "update":
					_, err = client.UpdateWorkflow(fixture.ctx, options)
				case "update-with-start":
					intent, setup := client.NewWithStartWorkflowOperation(sdk.StartWorkflowOptions{ID: "workflow", TaskQueue: "queue", WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}, "workflow")
					if setup != nil {
						t.Fatal(setup)
					}
					_, err = client.UpdateWithStartWorkflow(fixture.ctx, intent, options)
				}
				native, present := NativeError(err)
				if !present || reflect.TypeOf(native) != reflect.TypeOf(test.original) {
					t.Errorf("interceptor changed exact native error shape: captured=%t type=%T", present, native)
				}
				if !proto.Equal(test.conversion.ErrorToFailure(native), test.conversion.ErrorToFailure(test.original)) {
					t.Error("interceptor changed exact native failure/retryability")
				}
			})
		}
	}
}

func TestNativeErrorHasDirectSafeCoreAndExactSemanticAccess(t *testing.T) {
	original := sdktemporal.NewNonRetryableApplicationError("private native canary", "native-kind", nil, "private detail canary")
	extension := &semanticTestInterceptor{cause: original}
	fixture := newTestFixture(t, NativeOptions{Interceptors: []interceptor.ClientInterceptor{extension}}, nil)
	err := fixture.owner.Client().SignalWorkflow(fixture.ctx, "workflow", "run", "signal", nil)
	core, present := failure.Inspect(err)
	if !present || core == nil || !errors.Is(err, ErrExecution) {
		t.Fatal("native operation lost its directly localizable safe core", err)
	}
	semantic, captured := NativeError(err)
	if _, nativeShape := semantic.(*sdktemporal.ApplicationError); !captured || !nativeShape {
		t.Fatalf("captured native return is missing: captured=%t type=%T", captured, semantic)
	}
	if !errors.Is(semantic, original) {
		t.Fatal("scoped native error lost its intentional original identity")
	}
	converter := sdktemporal.NewDefaultFailureConverter(sdktemporal.DefaultFailureConverterOptions{})
	want := converter.ErrorToFailure(original)
	if !proto.Equal(converter.ErrorToFailure(semantic), want) {
		t.Fatal("deliberate semantic access changed native failure conversion")
	}
	if proto.Equal(converter.ErrorToFailure(err), want) {
		t.Fatal("control incorrectly treats safe wrapping as native-transparent")
	}
	for _, formatted := range []string{err.Error(), fmt.Sprint(err), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err), core.Error()} {
		if strings.Contains(formatted, "private") {
			t.Fatal("ordinary public error presentation leaked native content")
		}
	}
	if _, ok := NativeError(fmt.Errorf("wrapped: %w", err)); ok {
		t.Fatal("semantic accessor searched an arbitrary wrapper")
	}
	if _, ok := NativeError(errors.Join(err, original)); ok {
		t.Fatal("semantic accessor selected a cause from a join")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	delivery, err := fixture.dependencies.Evidence.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, present := snapshot.ValueCopy()
	if !present {
		t.Fatal("handled call failure erased independent evidence")
	}
	if cause, known := result.NativeError(); !known || !proto.Equal(converter.ErrorToFailure(cause), want) {
		t.Fatal("independent result lost exact native failure")
	}
	if _, known := failure.Inspect(snapshot.Primary()); !known {
		t.Fatal("independent failure lacks its direct safe core")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeErrorNeverGuessesAmongJoinedNativeCauses(t *testing.T) {
	first := sdktemporal.NewNonRetryableApplicationError("first private", "first", nil)
	second := sdktemporal.NewApplicationError("second private", "second")
	original := errors.Join(first, second)
	fixture := newTestFixture(t, NativeOptions{Interceptors: []interceptor.ClientInterceptor{&semanticTestInterceptor{cause: original}}}, nil)
	err := fixture.owner.Client().SignalWorkflow(fixture.ctx, "workflow", "run", "signal", nil)
	semantic, captured := NativeError(err)
	if !captured || semantic != original {
		t.Fatal("provider selected one cause instead of the known native return")
	}
	if _, ok := NativeError((*Error)(nil)); ok {
		t.Fatal("typed nil public error invented native evidence")
	}
	if _, ok := failure.Inspect((*Error)(nil)); ok {
		t.Fatal("typed nil public error invented a safe occurrence")
	}
}

type customLazyState struct {
	failure *failurepb.Failure
	decodes atomic.Int32
	scopes  atomic.Int32
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (state *customLazyState) decode(output *string) error {
	state.decodes.Add(1)
	if state.block.CompareAndSwap(true, false) {
		close(state.entered)
		<-state.release
	}
	return converter.GetDefaultDataConverter().FromPayloads(state.failure.GetApplicationFailureInfo().GetDetails(), output)
}

type cooperatingLazyError struct {
	state *customLazyState
	guard func(func() error) error
}

func (*cooperatingLazyError) Error() string { return "custom private native failure" }

func (err *cooperatingLazyError) Details(output *string) error {
	decode := func() error { return err.state.decode(output) }
	if err.guard != nil {
		return err.guard(decode)
	}
	return decode()
}

func (err *cooperatingLazyError) FathomryScopeDecodersV1(guard func(func() error) error) error {
	err.state.scopes.Add(1)
	copy := *err
	copy.guard = guard
	return &copy
}

type opaqueCustomError struct{ state *customLazyState }

func (*opaqueCustomError) Error() string { return "opaque private native failure" }

type customFailureConverter struct {
	state   *customLazyState
	opaque  bool
	decoded atomic.Int32
}

func (conversion *customFailureConverter) FailureToError(value *failurepb.Failure) error {
	conversion.decoded.Add(1)
	if !proto.Equal(value, conversion.state.failure) {
		return errors.New("unexpected custom failure wire value")
	}
	if conversion.opaque {
		return &opaqueCustomError{state: conversion.state}
	}
	return &cooperatingLazyError{state: conversion.state}
}

func (conversion *customFailureConverter) ErrorToFailure(value error) *failurepb.Failure {
	switch value := value.(type) {
	case *sdktemporal.WorkflowExecutionError:
		return conversion.ErrorToFailure(value.Unwrap())
	case *cooperatingLazyError:
		return proto.Clone(value.state.failure).(*failurepb.Failure)
	case *opaqueCustomError:
		return proto.Clone(value.state.failure).(*failurepb.Failure)
	default:
		return sdktemporal.NewDefaultFailureConverter(sdktemporal.DefaultFailureConverterOptions{}).ErrorToFailure(value)
	}
}

type customFailurePeer struct {
	testServer
	failure *failurepb.Failure
}

func (peer *customFailurePeer) GetWorkflowExecutionHistory(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	peer.histories.Add(1)
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{
		EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
			Failure: proto.Clone(peer.failure).(*failurepb.Failure),
		}},
	}}}}, nil
}

func customFailureFixture(t *testing.T, opaque bool) (testFixture, *customFailurePeer, *customFailureConverter) {
	t.Helper()
	payloads, err := converter.GetDefaultDataConverter().ToPayloads("custom private detail")
	if err != nil {
		t.Fatal(err)
	}
	state := &customLazyState{failure: &failurepb.Failure{Message: "custom private wire message", Source: "custom-converter",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "custom-domain", NonRetryable: true, Details: payloads}}},
		entered: make(chan struct{}), release: make(chan struct{})}
	conversion := &customFailureConverter{state: state, opaque: opaque}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	peer := &customFailurePeer{failure: state.failure}
	workflowservice.RegisterWorkflowServiceServer(server, peer)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	fixture := newTestFixture(t, NativeOptions{FailureConverter: conversion, ContextDialer: func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}}, nil)
	return fixture, peer, conversion
}

func workflowCustomCause(t *testing.T, client *Client, conversion *customFailureConverter) error {
	t.Helper()
	run, err := client.GetWorkflow("workflow", "run")
	if err != nil {
		t.Fatal(err)
	}
	err = run.Get(context.Background(), nil)
	if _, present := failure.Inspect(err); !present || strings.Contains(err.Error(), "private") || strings.Contains(fmt.Sprintf("%+v", err), "private") {
		t.Fatal("custom native failure lacks safe direct presentation", err)
	}
	semantic, captured := NativeError(err)
	if !captured || !proto.Equal(conversion.ErrorToFailure(semantic), conversion.state.failure) {
		t.Fatal("custom native converter round-trip changed")
	}
	workflow, ok := semantic.(*sdktemporal.WorkflowExecutionError)
	if !ok {
		t.Fatalf("native workflow error boundary changed: %T", semantic)
	}
	return workflow.Unwrap()
}

func ackCustomFailureRecords(t *testing.T, fixture testFixture, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range count {
		delivery, err := fixture.dependencies.Evidence.NextReleased(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCustomFailureConverterAndCooperatingLazyErrorRetainPublicUse(t *testing.T) {
	fixture, peer, conversion := customFailureFixture(t, false)
	first, err := fixture.owner.Client().Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	second, err := fixture.owner.Client().Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(context.Background())
	firstCause, ok := workflowCustomCause(t, first, conversion).(*cooperatingLazyError)
	if !ok {
		t.Fatal("custom lazy error shape was rewritten")
	}
	secondCause, ok := workflowCustomCause(t, second, conversion).(*cooperatingLazyError)
	if !ok || conversion.decoded.Load() != 2 || conversion.state.scopes.Load() < 2 || peer.histories.Load() != 2 {
		t.Fatal("actual SDK failure conversion or cooperative scope was bypassed")
	}
	ackCustomFailureRecords(t, fixture, 2)
	var initial string
	if err := firstCause.Details(&initial); err != nil || initial != "custom private detail" {
		t.Fatal("live custom decoder positive control failed", err)
	}
	ackCustomFailureRecords(t, fixture, 1)
	conversion.state.block.Store(true)
	unblock := sync.OnceFunc(func() { close(conversion.state.release) })
	defer unblock()
	decoded := make(chan error, 1)
	go func() {
		var output string
		decoded <- firstCause.Details(&output)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-conversion.state.entered:
	case <-ctx.Done():
		t.Fatal("custom decoder did not enter", ctx.Err())
	}
	wait, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	err = first.Close(wait)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) || first.Closed() {
		t.Fatal("canceled close discarded actual custom decoder ownership", err)
	}
	var peerOutput string
	if err := secondCause.Details(&peerOutput); err != nil || peerOutput != "custom private detail" || conversion.state.decodes.Load() != 3 {
		t.Fatal("closing one use revoked a live custom-error peer", err)
	}
	ackCustomFailureRecords(t, fixture, 1)
	unblock()
	if err := <-decoded; err != nil {
		t.Fatal("blocked custom decoder lost its actual result", err)
	}
	if err := first.Close(ctx); err != nil || !first.Closed() {
		t.Fatal("repeat close did not observe actual custom decode completion", err)
	}
	ackCustomFailureRecords(t, fixture, 1)
	if err := firstCause.Details(&initial); err == nil || conversion.state.decodes.Load() != 3 {
		t.Fatal("cooperating custom decoder reopened a closed use", err)
	}
	if err := second.SignalWorkflow(ctx, "workflow", "run", "peer", nil); err != nil || peer.signals.Load() != 1 {
		t.Fatal("closed custom-error use released the peer's native source", err)
	}
	ackCustomFailureRecords(t, fixture, 1)
}

func TestOpaqueCustomFailureShapeIsPreservedWithoutInventedDecoderGuard(t *testing.T) {
	fixture, peer, conversion := customFailureFixture(t, true)
	cause, ok := workflowCustomCause(t, fixture.owner.Client(), conversion).(*opaqueCustomError)
	if !ok || cause.state != conversion.state || conversion.state.scopes.Load() != 0 || peer.histories.Load() != 1 {
		t.Fatal("opaque custom failure was rewritten or falsely treated as cooperative")
	}
	ackCustomFailureRecords(t, fixture, 1)
}
