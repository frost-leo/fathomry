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
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type queryServer struct {
	*testServer
	queries  atomic.Int32
	payloads *commonpb.Payloads
}

type queryTraffic func(context.Context, string, any, any) error

func (hook queryTraffic) CheckCallAllowed(ctx context.Context, method string, request, reply any) error {
	return hook(ctx, method, request, reply)
}

func TestQueryValueRetainsOmittedEffectiveIdentityInDelayedEvidence(t *testing.T) {
	for _, options := range []bool{false, true} {
		name := "direct"
		if options {
			name = "options"
		}
		t.Run(name, func(t *testing.T) {
			hook := queryTraffic(func(_ context.Context, _ string, request, _ any) error {
				if query, ok := request.(*workflowservice.QueryWorkflowRequest); ok {
					query.Execution.WorkflowId = strings.Repeat("x", 1025)
				}
				return nil
			})
			fixture, peer := queryFixture(t, NativeOptions{TrafficController: hook}, nil, 8)
			var value *QueryValue
			var err error
			if options {
				value, _, err = fixture.owner.Client().QueryWorkflowValueWithOptions(fixture.ctx, &sdk.QueryWorkflowWithOptionsRequest{WorkflowID: "intention", RunID: "run", QueryType: "query"})
			} else {
				value, err = fixture.owner.Client().QueryWorkflowValue(fixture.ctx, "intention", "run", "query")
			}
			if !errors.Is(err, ErrLimit) || value == nil || peer.queries.Load() != 1 {
				t.Fatal("effective identity overflow control did not return its native value", err)
			}
			read := func(operation string) {
				t.Helper()
				delivery, err := fixture.dependencies.Evidence.NextReleased(fixture.ctx)
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := delivery.Receipt()
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := receipt.WaitReleased(fixture.ctx)
				if err != nil {
					t.Fatal(err)
				}
				result, present := snapshot.ValueCopy()
				if !present || !result.Execution.IdentityOmitted || result.Execution.WorkflowID != "intention" || result.Execution.Operation != operation {
					t.Error("retained query lost effective-identity uncertainty", operation, result.Execution.IdentityOmitted)
				}
				if err := delivery.Ack(); err != nil {
					t.Fatal(err)
				}
			}
			read("workflow.query")
			var output map[string]string
			if err := value.Get(fixture.ctx, &output); err != nil || output["value"] != "retained" {
				t.Fatal("retained result could not be decoded", err)
			}
			read("workflow.query-result")
			if _, _, err := value.RawPayloads(fixture.ctx); err != nil {
				t.Fatal(err)
			}
			read("workflow.query-payloads")
		})
	}
}

func (server *queryServer) QueryWorkflow(_ context.Context, request *workflowservice.QueryWorkflowRequest) (*workflowservice.QueryWorkflowResponse, error) {
	server.queries.Add(1)
	switch request.GetQuery().GetQueryType() {
	case "absent":
		return &workflowservice.QueryWorkflowResponse{}, nil
	case "rejected":
		return &workflowservice.QueryWorkflowResponse{QueryRejected: &querypb.QueryRejected{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}}, nil
	default:
		return &workflowservice.QueryWorkflowResponse{QueryResult: proto.Clone(server.payloads).(*commonpb.Payloads)}, nil
	}
}

func queryFixture(t *testing.T, options NativeOptions, payloads *commonpb.Payloads, capacity int) (testFixture, *queryServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if payloads == nil {
		payloads, err = converter.GetDefaultDataConverter().ToPayloads(map[string]string{"value": "retained"})
		if err != nil {
			t.Fatal(err)
		}
	}
	peer := &queryServer{testServer: &testServer{}, payloads: payloads}
	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, peer)
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-joined })
	prepared, err := Prepare(Settings{Name: "query", Endpoint: listener.Addr().String(), Namespace: "test", Plaintext: true,
		MaxActive: 2, MaxRequestBytes: 4096, MaxResponseBytes: 4096, InnerEvidenceCapacity: capacity}, options)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := adapters.NewInbox[Result](policy.Evidence)
	workers, _ := adapters.NewInbox[WorkerResult](policy.Workers)
	tasks, _ := adapters.NewInbox[TaskResult](policy.Tasks)
	dependencies := Dependencies{Runtime: runtime, Evidence: evidence, Workers: workers, Tasks: tasks}
	owner, err := prepared.Open(ctx, dependencies)
	if err != nil {
		cancel()
		if owner != nil {
			_ = owner.Close(context.Background())
		}
		_ = runtime.Close(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		wait, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := owner.Close(wait); err != nil {
			t.Error(err)
		}
		if err := runtime.Close(wait); err != nil {
			t.Error(err)
		}
	})
	return testFixture{owner: owner, server: peer.testServer, dependencies: dependencies, ctx: ctx}, peer
}

type queryCapture struct {
	interceptor.ClientInterceptorBase
	value    converter.EncodedValue
	returned converter.EncodedValue
	wrap     func(converter.EncodedValue) converter.EncodedValue
}

func (capture *queryCapture) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &queryCaptureOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}, capture: capture}
}

type queryCaptureOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	capture *queryCapture
}

func (outbound *queryCaptureOutbound) QueryWorkflow(ctx context.Context, input *interceptor.ClientQueryWorkflowInput) (converter.EncodedValue, error) {
	if outbound.capture.returned != nil {
		return outbound.capture.returned, nil
	}
	value, err := outbound.Next.QueryWorkflow(ctx, input)
	outbound.capture.value = value
	if value != nil && outbound.capture.wrap != nil {
		value = outbound.capture.wrap(value)
	}
	return value, err
}

type cooperatingQueryValue struct {
	converter.EncodedValue
	copies *[]converter.EncodedValue
}

func (value *cooperatingQueryValue) HasValue() bool {
	return converter.GetPayloads(value.EncodedValue) != nil
}
func (value *cooperatingQueryValue) Payloads() *commonpb.Payloads {
	return converter.GetPayloads(value.EncodedValue)
}
func (value *cooperatingQueryValue) FathomryMapEncodedValueV1(mapping func(converter.EncodedValue) converter.EncodedValue) converter.EncodedValue {
	copy := *value
	copy.EncodedValue = mapping(value.EncodedValue)
	*value.copies = append(*value.copies, &copy)
	return &copy
}

func TestQueryValueCooperatingWrapperPreservesRetainedAndExpiredScopes(t *testing.T) {
	var original *cooperatingQueryValue
	var copied []converter.EncodedValue
	capture := &queryCapture{wrap: func(value converter.EncodedValue) converter.EncodedValue {
		original = &cooperatingQueryValue{EncodedValue: value, copies: &copied}
		return original
	}}
	fixture, peer := queryFixture(t, NativeOptions{Interceptors: []interceptor.ClientInterceptor{capture}}, nil, 16)
	value, err := fixture.owner.Client().QueryWorkflowValue(fixture.ctx, "workflow", "run", "query")
	if err != nil || !value.HasValue() {
		t.Fatal("wrapped encoded response missing", err)
	}
	var output map[string]string
	if err := original.Get(&output); err == nil {
		t.Fatal("original borrowed wrapper gained retained authority")
	}
	for range 2 {
		if err := value.Get(fixture.ctx, &output); err != nil || output["value"] != "retained" {
			t.Fatal("cooperating wrapper decode failed", err)
		}
	}
	if payloads, supported, err := value.RawPayloads(fixture.ctx); err != nil || !supported || payloads == nil {
		t.Fatal("cooperating raw payload wrapper failed", supported, err)
	}
	if peer.queries.Load() != 1 || len(copied) != 4 || original.EncodedValue != capture.value {
		t.Fatal("wrapper transfer re-queried or mutated original identity")
	}
	for _, snapshot := range copied {
		if err := snapshot.Get(&output); err == nil {
			t.Fatal("consumption-scoped wrapper retained decoder authority")
		}
	}
	if err := original.Get(&output); err == nil {
		t.Fatal("retained consumption reopened original wrapper")
	}
}

type typedNilQueryValue struct{}

func (*typedNilQueryValue) HasValue() bool { panic("typed-nil query presence must not run") }
func (*typedNilQueryValue) Get(any) error  { panic("typed-nil query decode must not run") }

func TestQueryValueRejectsTypedNilBeforeNativePresenceCallback(t *testing.T) {
	var typedNil *typedNilQueryValue
	capture := &queryCapture{returned: typedNil}
	fixture, peer := queryFixture(t, NativeOptions{Interceptors: []interceptor.ClientInterceptor{capture}}, nil, 8)
	client := fixture.owner.Client()
	if value, err := client.QueryWorkflowValue(fixture.ctx, "workflow", "run", "query"); value != nil || !errors.Is(err, ErrExecution) {
		t.Fatal("typed-nil native query response accepted", err)
	}
	if value, rejected, err := client.QueryWorkflowValueWithOptions(fixture.ctx, &sdk.QueryWorkflowWithOptionsRequest{WorkflowID: "workflow", QueryType: "query"}); value != nil || rejected != nil || !errors.Is(err, ErrExecution) {
		t.Fatal("typed-nil native options response accepted", err)
	}
	if peer.queries.Load() != 0 {
		t.Fatal("interceptor-only typed-nil control unexpectedly reached service")
	}
}

func TestQueryValueRetainsOneReplyPresenceAndOriginalUse(t *testing.T) {
	capture := &queryCapture{}
	fixture, peer := queryFixture(t, NativeOptions{Interceptors: []interceptor.ClientInterceptor{capture}}, nil, 32)
	first, err := fixture.owner.Client().Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	other, err := fixture.owner.Client().Borrow(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	value, err := first.QueryWorkflowValue(fixture.ctx, "workflow", "run", "query")
	if err != nil || !value.HasValue() {
		t.Fatal("native query value missing", err)
	}
	var escaped map[string]string
	if err := capture.value.Get(&escaped); err == nil {
		t.Fatal("retained interceptor continuation gained QueryValue authority")
	}
	var firstDecode map[string]string
	if err := value.Get(fixture.ctx, &firstDecode); err != nil || firstDecode["value"] != "retained" {
		t.Fatal("first native decode", err)
	}
	var secondDecode struct{ Value string }
	if err := value.Get(fixture.ctx, &secondDecode); err != nil || secondDecode.Value != "retained" {
		t.Fatal("second native decode of same reply", err)
	}
	payloads, supported, err := value.RawPayloads(fixture.ctx)
	if err != nil || !supported || payloads == nil {
		t.Fatal("native optional raw payload support lost", err)
	}
	payloads.Payloads[0].Data = []byte("caller mutation")
	var afterMutation map[string]string
	if err := value.Get(fixture.ctx, &afterMutation); err != nil || afterMutation["value"] != "retained" || peer.queries.Load() != 1 {
		t.Fatal("decode re-queried or raw payload copy aliased", err)
	}
	if _, err := json.Marshal(value); !errors.Is(err, ErrSerialization) {
		t.Fatal("retained query value serialized", err)
	}
	if err := first.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if !value.HasValue() || value.Get(fixture.ctx, &afterMutation) == nil {
		t.Fatal("closed origin either lost local presence or retained decoder authority")
	}
	if _, _, err := value.RawPayloads(fixture.ctx); err == nil {
		t.Fatal("closed origin retained raw accessor authority")
	}
	peerValue, err := other.QueryWorkflowValue(fixture.ctx, "workflow", "run", "query")
	if err != nil || peerValue.Get(fixture.ctx, &afterMutation) != nil || peer.queries.Load() != 2 {
		t.Fatal("closing first use revoked independent peer", err)
	}
}

func TestQueryValuePreservesAbsentAndRejectedResponses(t *testing.T) {
	fixture, peer := queryFixture(t, NativeOptions{}, nil, 16)
	client := fixture.owner.Client()
	value, err := client.QueryWorkflowValue(fixture.ctx, "workflow", "run", "absent")
	if err != nil || value == nil || value.HasValue() {
		t.Fatal("absent native response became a failure", err)
	}
	var output string
	if err := value.Get(fixture.ctx, &output); !errors.Is(err, sdktemporal.ErrNoData) {
		t.Fatal("absent decode lost native ErrNoData", err)
	}
	if payloads, supported, err := value.RawPayloads(fixture.ctx); err != nil || !supported || payloads != nil {
		t.Fatal("supported nil payload observation changed", supported, err)
	}
	if err := client.QueryWorkflow(fixture.ctx, "workflow", "run", "absent", &output); !errors.Is(err, sdktemporal.ErrNoData) {
		t.Fatal("existing eager convenience changed", err)
	}
	value, rejected, err := client.QueryWorkflowValueWithOptions(fixture.ctx, &sdk.QueryWorkflowWithOptionsRequest{WorkflowID: "workflow", RunID: "run", QueryType: "rejected"})
	if err != nil || value != nil || rejected == nil || rejected.Status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatal("native rejection conflated with result", err)
	}
	value, rejected, err = client.QueryWorkflowValueWithOptions(fixture.ctx, &sdk.QueryWorkflowWithOptionsRequest{WorkflowID: "workflow", RunID: "run", QueryType: "query"})
	var decoded map[string]string
	if err != nil || rejected != nil || !value.HasValue() || value.Get(fixture.ctx, &decoded) != nil || decoded["value"] != "retained" || peer.queries.Load() != 4 {
		t.Fatal("native options result/rejection changed", err)
	}
}

type queryReaderState struct {
	reads    atomic.Int32
	mu       sync.Mutex
	contexts []converter.WorkflowSerializationContext
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

type queryReader struct {
	converter.DataConverter
	state   *queryReaderState
	context converter.WorkflowSerializationContext
}

func (reader *queryReader) WithSerializationContext(ctx converter.SerializationContext) converter.DataConverter {
	copy := *reader
	copy.context, _ = ctx.(converter.WorkflowSerializationContext)
	return &copy
}
func (reader *queryReader) FromPayloads(payloads *commonpb.Payloads, values ...any) error {
	reader.state.reads.Add(1)
	reader.state.mu.Lock()
	reader.state.contexts = append(reader.state.contexts, reader.context)
	reader.state.mu.Unlock()
	if reader.state.entered != nil {
		reader.state.once.Do(func() { close(reader.state.entered) })
		<-reader.state.release
	}
	return reader.DataConverter.FromPayloads(payloads, values...)
}

func TestQueryValueDecodeAdmissionAndSerializationContext(t *testing.T) {
	state := &queryReaderState{}
	reader := &queryReader{DataConverter: converter.GetDefaultDataConverter(), state: state}
	fixture, peer := queryFixture(t, NativeOptions{DataConverter: reader}, nil, 1)
	value, err := fixture.owner.Client().QueryWorkflowValue(fixture.ctx, "query-workflow", "run", "query")
	if err != nil || state.reads.Load() != 0 {
		t.Fatal("query value decoded eagerly", err)
	}
	var output map[string]string
	if err := value.Get(fixture.ctx, &output); !errors.Is(err, adapters.ErrEvidence) || state.reads.Load() != 0 {
		t.Fatal("public evidence refusal entered native decoder", err)
	}
	for range 2 {
		record, err := fixture.dependencies.Evidence.NextReleased(fixture.ctx)
		if err != nil || record.Ack() != nil {
			t.Fatal("query evidence release", err)
		}
		if err := value.Get(fixture.ctx, &output); err != nil {
			t.Fatal(err)
		}
	}
	if state.reads.Load() != 2 || peer.queries.Load() != 1 {
		t.Fatal("repeated decode reissued query")
	}
	for _, ctx := range state.contexts {
		if ctx.Namespace != "test" || ctx.WorkflowID != "query-workflow" {
			t.Fatal("native selected reader serialization context changed", ctx)
		}
	}
}

func TestQueryValueHeldDecoderRetainsUseAfterCanceledCloseWait(t *testing.T) {
	state := &queryReaderState{entered: make(chan struct{}), release: make(chan struct{})}
	reader := &queryReader{DataConverter: converter.GetDefaultDataConverter(), state: state}
	fixture, _ := queryFixture(t, NativeOptions{DataConverter: reader}, nil, 8)
	t.Cleanup(func() {
		select {
		case <-state.release:
		default:
			close(state.release)
		}
	})
	client := fixture.owner.Client()
	value, err := client.QueryWorkflowValue(fixture.ctx, "workflow", "run", "query")
	if err != nil {
		t.Fatal(err)
	}
	decoded := make(chan error, 1)
	go func() { var output map[string]string; decoded <- value.Get(fixture.ctx, &output) }()
	select {
	case <-state.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("retained native decoder not entered")
	}
	wait, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Close(wait); err == nil || client.Closed() {
		t.Fatal("canceled Close wait released active query decoder", err)
	}
	close(state.release)
	<-decoded
	if err := client.Close(fixture.ctx); err != nil || !client.Closed() {
		t.Fatal("actual query decoder exit did not release use", err)
	}
	var output map[string]string
	if err := value.Get(fixture.ctx, &output); err == nil || state.reads.Load() != 1 {
		t.Fatal("closed query decoder revived", err)
	}
}

func TestQueryValueKeepsNativeExternalRetrievalStage(t *testing.T) {
	reference, err := converter.GetDefaultDataConverter().ToPayload(&sdkpb.ExternalStorageReference{DriverName: "description-fixture", ClaimData: map[string]string{"id": "query"}})
	if err != nil {
		t.Fatal(err)
	}
	store := &descriptionStorage{entered: make(chan struct{}), release: make(chan struct{})}
	close(store.release)
	fixture, peer := queryFixture(t, NativeOptions{ExternalStorage: converter.ExternalStorage{Drivers: []converter.StorageDriver{store}}}, &commonpb.Payloads{Payloads: []*commonpb.Payload{reference}}, 8)
	value, err := fixture.owner.Client().QueryWorkflowValue(fixture.ctx, "workflow", "run", "query")
	if err != nil || store.reads.Load() != 1 {
		t.Fatal("native query inbound retrieval stage changed", err)
	}
	for range 2 {
		var output string
		if err := value.Get(fixture.ctx, &output); err != nil || output != "decoded" {
			t.Fatal("retained external result decode", err)
		}
	}
	if store.reads.Load() != 1 || peer.queries.Load() != 1 {
		t.Fatal("retained decode repeated query or external retrieval")
	}
}
