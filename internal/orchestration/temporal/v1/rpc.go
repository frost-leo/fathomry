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
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/google/uuid"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RPCResult is immutable technical evidence, independent of the returned proto.
// Invoked means local transport invocation, not server acceptance. Acknowledged
// means a successful RPC response, not Workflow completion or an external effect.
type RPCResult struct {
	private
	Method        string
	RequestBytes  int
	ResponseBytes int
	// ResponseBytesKnown distinguishes an empty response from unmeasured bytes.
	ResponseBytesKnown bool
	Invoked            bool
	Acknowledged       bool
	nativeError        error
	nativeReturned     bool
}

// NativeCause exposes the exact captured transport return, deliberately separate
// from bounded observation errors and attributed result wrappers.
func (value RPCResult) NativeCause() (error, bool) { return value.nativeError, value.nativeReturned }

type workflowService = workflowservice.WorkflowServiceClient

type operatorService = operatorservice.OperatorServiceClient

type workflowView struct {
	private
	workflowService
}

type operatorView struct {
	private
	operatorService
}

// WorkflowService supplies the complete generated unary service surface without
// raw connection or lifecycle access. Use a distinct correlation for each logical
// call. Requests are borrowed until return; replies become the caller's.
// Each call independently enforces the exact grant, admission and evidence bound.
// Header, Trailer and Peer output containers are borrowed exclusively until the
// call returns; they must not be shared with concurrent calls.
// At most MaxCallOptions options are accepted, including generated StaticMethod.
func (client *Client) WorkflowService(correlation fault.Correlation) workflowservice.WorkflowServiceClient {
	return &workflowView{workflowService: workflowservice.NewWorkflowServiceClient(&rpcConnection{client: client, correlation: correlation})}
}

// OperatorService has the same boundaries as WorkflowService. No operator grant
// is implicit. Granting an RPC does not authenticate a caller to Temporal Server.
func (client *Client) OperatorService(correlation fault.Correlation) operatorservice.OperatorServiceClient {
	return &operatorView{operatorService: operatorservice.NewOperatorServiceClient(&rpcConnection{client: client, correlation: correlation})}
}

type rpcConnection struct {
	client      *Client
	correlation fault.Correlation
}

// WithRPCErrorMapper maps ordinary direct generated-service return errors only.
// It does not change retained native evidence or errors returned to SDK callbacks.
// The mapper must be synchronous and must not reenter the same call.
func WithRPCErrorMapper(client *Client, mapError func(error) error) *Client {
	if client == nil {
		return nil
	}
	copy := *client
	copy.mapError = mapError
	return &copy
}

// WithFreshRPCIDs gives each invocation of a reusable generated view a distinct
// local call identity. Other correlation fields and inherited parent scope remain.
func WithFreshRPCIDs(client *Client) *Client {
	if client == nil {
		return nil
	}
	copy := *client
	copy.freshRPCIDs = true
	return &copy
}

type activeRPCKey struct{}

type opaqueContext struct{ context.Context }

func (opaqueContext) Value(any) any { return nil }

func observeAttempt(ctx context.Context, method string, request, reply any, transport *grpc.ClientConn, next grpc.UnaryInvoker, options ...grpc.CallOption) error {
	if call, ok := ctx.Value(activeRPCKey{}).(interface{ Attempt() (uint64, error) }); ok {
		if _, err := call.Attempt(); err != nil {
			return err
		}
	}
	var identity *observedNativeIdentity
	var sequence uint64
	if authority, ok := ctx.Value(nativeCallKey{}).(*nativeCall); ok {
		identity = authority.identity
		if identity != nil {
			sequence = identity.begin(request)
		}
	}
	err := next(ctx, method, request, reply, transport, options...)
	if err == nil && identity != nil {
		identity.complete(sequence, reply)
	}
	return err
}

func (connection *rpcConnection) Invoke(ctx context.Context, method string, request, reply any, options ...grpc.CallOption) (err error) {
	client := connection.client
	if client != nil && client.mapError != nil {
		defer func() {
			if err != nil {
				err = client.mapError(err)
			}
		}()
	}
	if client == nil || client.owner == nil || ctx == nil {
		return failure(ErrInput, "rpc")
	}
	value := client.owner.settings
	if !slices.Contains(value.RPCs, method) {
		return failure(ErrAuthority, "rpc-grant")
	}
	input, inputOK := request.(proto.Message)
	output, outputOK := reply.(proto.Message)
	if !inputOK || !outputOK || !input.ProtoReflect().IsValid() || !output.ProtoReflect().IsValid() {
		return failure(ErrInput, "rpc-message")
	}
	if field := input.ProtoReflect().Descriptor().Fields().ByName("namespace"); field != nil {
		if field.Kind() != protoreflect.StringKind || input.ProtoReflect().Get(field).String() != value.Namespace {
			return failure(ErrAuthority, "namespace")
		}
	}
	if err := validateCallOptions(value, options); err != nil {
		return err
	}
	name := "rpc." + strings.ToLower(method[strings.LastIndexByte(method, '/')+1:])
	correlation := connection.correlation
	if client.freshRPCIDs {
		correlation.Call = uuid.NewString()
	}
	call, admitted, finish, err := client.begin(ctx, correlation, name)
	if err != nil {
		return err
	}
	defer finish()
	evidence := RPCResult{Method: method}
	returned := false
	defer func() {
		if returned {
			return
		}
		recovered := recover()
		call.Complete(invocation.Outcome[RPCResult]{Value: evidence, Present: true, Primary: failure(ErrRPC, "native-exit")})
		if recovered != nil {
			panic(recovered)
		}
	}()
	_ = call.Execute(admitted, invocation.Budget{Limit: value.RPCTimeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[RPCResult] {
		frame := newAdmissionFrame(admitted, client.access, call)
		defer frame.closed.Store(true)
		requestBytes, err := messageSize(work, input, value.MaxRequestBytes)
		evidence.RequestBytes = requestBytes
		if err != nil {
			return invocation.Outcome[RPCResult]{Value: evidence, Present: true, Primary: err}
		}
		nativeContext := context.WithValue(opaqueContext{work}, activeRPCKey{}, call)
		nativeContext = context.WithValue(nativeContext, transportOwnerKey{}, client.owner)
		nativeContext = context.WithValue(nativeContext, admissionFrameKey{}, frame)
		evidence.Invoked = true
		err = client.owner.transport.Invoke(nativeContext, method, request, reply, boundedCallOptions(value, options)...)
		evidence.nativeError, evidence.nativeReturned = err, true
		evidence.Acknowledged = err == nil
		if err == nil {
			evidence.ResponseBytes, err = messageSize(work, output, value.MaxResponseBytes)
			evidence.ResponseBytesKnown = err == nil
		}
		if err != nil {
			err = failure(ErrRPC, name, err, work.Err(), context.Cause(work))
		}
		return invocation.Outcome[RPCResult]{Value: evidence, Present: true, Primary: err}
	})
	returned = true
	result, present := call.Receipt().Result()
	if !present {
		return failure(ErrRPC, name)
	}
	return semanticFailure(result.Err(), evidence.nativeError, evidence.nativeReturned)
}

// MaxCallOptions includes the StaticMethod marker added by generated methods.
const MaxCallOptions = 32

func validateCallOptions(value settings, options []grpc.CallOption) error {
	if len(options) > MaxCallOptions {
		return failure(ErrLimit, "call-options")
	}
	for _, option := range options {
		switch option := option.(type) {
		case grpc.StaticMethodCallOption, grpc.FailFastCallOption:
		case grpc.HeaderCallOption:
			if option.HeaderAddr == nil {
				return failure(ErrInput, "header-option")
			}
		case grpc.TrailerCallOption:
			if option.TrailerAddr == nil {
				return failure(ErrInput, "trailer-option")
			}
		case grpc.PeerCallOption:
			if option.PeerAddr == nil {
				return failure(ErrInput, "peer-option")
			}
		case grpc.MaxRecvMsgSizeCallOption:
			if option.MaxRecvMsgSize < 1 || option.MaxRecvMsgSize > value.MaxResponseBytes {
				return failure(ErrLimit, "receive-option")
			}
		case grpc.MaxSendMsgSizeCallOption:
			if option.MaxSendMsgSize < 1 || option.MaxSendMsgSize > value.MaxRequestBytes {
				return failure(ErrLimit, "send-option")
			}
		default:
			return failure(ErrAuthority, "rpc-option")
		}
	}
	return nil
}

func boundedCallOptions(value settings, options []grpc.CallOption) []grpc.CallOption {
	send, receive := value.MaxRequestBytes, value.MaxResponseBytes
	for _, option := range options {
		switch option := option.(type) {
		case grpc.MaxRecvMsgSizeCallOption:
			receive = min(receive, option.MaxRecvMsgSize)
		case grpc.MaxSendMsgSizeCallOption:
			send = min(send, option.MaxSendMsgSize)
		}
	}
	return append(slices.Clone(options), grpc.MaxCallSendMsgSize(send), grpc.MaxCallRecvMsgSize(receive))
}

// Inspect before proto.Size/Marshal: cyclic caller-built protos must not recurse
// without a bound. The wire cap also bounds traversal work; depth is capped at 64.
func messageSize(ctx context.Context, message proto.Message, maximum int) (int, error) {
	remaining, nodes := maximum, maximum
	var walk func(protoreflect.Message, int) error
	var visit func(protoreflect.Kind, protoreflect.Value, int) error
	visit = func(kind protoreflect.Kind, value protoreflect.Value, depth int) error {
		nodes--
		if nodes < 0 {
			return failure(ErrLimit, "message-nodes")
		}
		if nodes&255 == 0 && ctx.Err() != nil {
			return failure(ErrRPC, "message", ctx.Err(), context.Cause(ctx))
		}
		switch kind {
		case protoreflect.StringKind:
			remaining -= len(value.String())
		case protoreflect.BytesKind:
			remaining -= len(value.Bytes())
		case protoreflect.MessageKind, protoreflect.GroupKind:
			return walk(value.Message(), depth+1)
		}
		if remaining < 0 {
			return failure(ErrLimit, "message-bytes")
		}
		return nil
	}
	walk = func(message protoreflect.Message, depth int) error {
		if depth > 64 {
			return failure(ErrLimit, "message-depth")
		}
		remaining -= len(message.GetUnknown())
		if remaining < 0 {
			return failure(ErrLimit, "message-bytes")
		}
		var err error
		message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			switch {
			case field.IsMap():
				value.Map().Range(func(key protoreflect.MapKey, item protoreflect.Value) bool {
					err = visit(field.MapKey().Kind(), key.Value(), depth)
					if err == nil {
						err = visit(field.MapValue().Kind(), item, depth)
					}
					return err == nil
				})
			case field.IsList():
				list := value.List()
				for index := 0; index < list.Len() && err == nil; index++ {
					err = visit(field.Kind(), list.Get(index), depth)
				}
			default:
				err = visit(field.Kind(), value, depth)
			}
			return err == nil
		})
		return err
	}
	if err := walk(message.ProtoReflect(), 0); err != nil {
		return 0, err
	}
	size := proto.Size(message)
	if size > maximum {
		return 0, failure(ErrLimit, "message-bytes")
	}
	return size, nil
}

func (*rpcConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("temporal: selected service APIs have no streaming methods")
}

func (client *callbackClient) GetWorkerBuildIdCompatibility(ctx context.Context, options *sdk.GetWorkerBuildIdCompatibilityOptions) (*sdk.WorkerBuildIDVersionSets, error) {
	return callbackGranted(ctx, client, workflowServicePrefix+"GetWorkerBuildIdCompatibility", Execution{Operation: "callback.getworkerbuildidcompatibility"},
		func(work context.Context, evidence *Execution) (*sdk.WorkerBuildIDVersionSets, error) {
			value, err := client.nativeClient.GetWorkerBuildIdCompatibility(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) GetWorkerTaskReachability(ctx context.Context, options *sdk.GetWorkerTaskReachabilityOptions) (*sdk.WorkerTaskReachability, error) {
	return callbackGranted(ctx, client, workflowServicePrefix+"GetWorkerTaskReachability", Execution{Operation: "callback.getworkertaskreachability"},
		func(work context.Context, evidence *Execution) (*sdk.WorkerTaskReachability, error) {
			value, err := client.nativeClient.GetWorkerTaskReachability(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) UpdateWorkerVersioningRules(ctx context.Context, options sdk.UpdateWorkerVersioningRulesOptions) (*sdk.WorkerVersioningRules, error) {
	return callbackGranted(ctx, client, workflowServicePrefix+"UpdateWorkerVersioningRules", Execution{Operation: "callback.updateworkerversioningrules"},
		func(work context.Context, evidence *Execution) (*sdk.WorkerVersioningRules, error) {
			value, err := client.nativeClient.UpdateWorkerVersioningRules(work, options)
			evidence.Accepted = err == nil
			return value, err
		})
}

func (client *callbackClient) GetWorkerVersioningRules(ctx context.Context, options sdk.GetWorkerVersioningOptions) (*sdk.WorkerVersioningRules, error) {
	return callbackGranted(ctx, client, workflowServicePrefix+"GetWorkerVersioningRules", Execution{Operation: "callback.getworkerversioningrules"},
		func(work context.Context, evidence *Execution) (*sdk.WorkerVersioningRules, error) {
			value, err := client.nativeClient.GetWorkerVersioningRules(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackClient) UpdateWorkerBuildIdCompatibility(ctx context.Context, options *sdk.UpdateWorkerBuildIdCompatibilityOptions) error {
	_, err := callbackGranted(ctx, client, workflowServicePrefix+"UpdateWorkerBuildIdCompatibility", Execution{Operation: "callback.worker-build-compatibility"},
		func(work context.Context, evidence *Execution) (struct{}, error) {
			err := client.nativeClient.UpdateWorkerBuildIdCompatibility(work, options)
			evidence.Accepted = err == nil
			return struct{}{}, err
		})
	return err
}

const workflowServicePrefix = "/temporal.api.workflowservice.v1.WorkflowService/"

func (client *callbackClient) CheckHealth(ctx context.Context, request *sdk.CheckHealthRequest) (*sdk.CheckHealthResponse, error) {
	return callbackNative(ctx, client, "", Execution{Operation: "callback.health"}, func(work context.Context, evidence *Execution) (*sdk.CheckHealthResponse, error) {
		value, err := client.nativeClient.CheckHealth(work, request)
		evidence.ResultObtained = err == nil
		return value, err
	})
}

// CheckHealth observes native service health; it does not certify namespace features.
func (client *Executions) CheckHealth(ctx context.Context, correlation fault.Correlation, request *sdk.CheckHealthRequest) (*sdk.CheckHealthResponse, error) {
	return executeNative(ctx, client, correlation, Execution{Operation: "health"}, func(work context.Context, evidence *Execution) (*sdk.CheckHealthResponse, error) {
		value, err := client.owner.native.CheckHealth(work, request)
		evidence.ResultObtained = err == nil
		return value, err
	})
}
