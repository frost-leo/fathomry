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
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Source is an opaque, non-owning assembly capability. Bind grants process RPCs.
type Source struct {
	private
	owner *connection
}

type connection struct {
	settings          settings
	native            sdk.Client
	transport         *grpc.ClientConn
	captureOnce       sync.Once
	closeOnce         sync.Once
	serverVersion     atomic.Pointer[string]
	ready             atomic.Bool
	runtime           RuntimeOptions
	setup             clientSetup
	plugins           []*clientPluginAdapter
	effective         sdk.Options
	transportLifetime transportLifetime
	setupContext      context.Context
	root              *connection
	parentLease       *resource.Lease
	sharedBytes       int64
}

type disabledLogger struct{}

func (disabledLogger) Debug(string, ...any) {}

func (disabledLogger) Info(string, ...any) {}

func (disabledLogger) Warn(string, ...any) {}

func (disabledLogger) Error(string, ...any) {}

// Select freezes explicitly supplied settings. Dialing observes GetSystemInfo
// only: it neither creates a namespace nor certifies task execution readiness.
// Attach resource.WithLimits before assembly. Without a runtime logger, native
// logging is disabled rather than selecting an implicit process output.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	return SelectWithRuntime(options, RuntimeOptions{}, layers...)
}

// SelectWithRuntime binds explicit borrowed runtime extensions, not serialized
// configuration. Their resource dependencies must precede and outlive this source;
// use resource.Borrow for dependencies owned by another assembly.
func SelectWithRuntime(options OptionsV1, runtime RuntimeOptions, layers ...resource.Layer) (resource.Selection[Source], error) {
	return selectSource(options, runtime, nil, 0, layers...)
}

// SelectFromExisting derives a native namespace Client on a bound source's
// transport. It reserves parentBytes until the derived source releases; derived
// resource byte limits must fit that envelope. Transport/authentication and its
// native RPC instrumentation are inherited. Native derivation is always eager.
func SelectFromExisting(options OptionsV1, runtime RuntimeOptions, parent *Client, parentBytes int64, layers ...resource.Layer) (resource.Selection[Source], error) {
	if parent == nil || parent.owner == nil || parent.access == nil || parentBytes < 1 || options.Lazy {
		return resource.Selection[Source]{}, failure(ErrInput, "shared-client")
	}
	if options.Endpoint == "" {
		options.Endpoint = parent.owner.settings.Endpoint
	}
	if options.Plaintext && !parent.owner.settings.Plaintext {
		return resource.Selection[Source]{}, failure(ErrAuthority, "shared-transport")
	}
	options.Plaintext = parent.owner.settings.Plaintext
	if options.MaxRequestBytes == 0 {
		options.MaxRequestBytes = parent.owner.settings.MaxRequestBytes
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = parent.owner.settings.MaxResponseBytes
	}
	return selectSource(options, runtime, parent, parentBytes, layers...)
}

func selectSource(options OptionsV1, runtime RuntimeOptions, parent *Client, parentBytes int64, layers ...resource.Layer) (resource.Selection[Source], error) {
	runtime, err := freezeRuntime(runtime)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	prepared, err := prepare(options, layers)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, value settings) (result resource.Resource[Source], err error) {
		owner := &connection{settings: value, runtime: runtime, sharedBytes: parentBytes}
		result = resource.Resource[Source]{Capability: Source{owner: owner}, Release: owner.close}
		defer func() {
			result.Acquired = owner.native != nil || owner.transport != nil || owner.parentLease != nil
			if recovered := recover(); recovered != nil {
				cause, _ := recovered.(error)
				err = failure(ErrConnect, "client-setup-panic", cause)
			}
		}()
		work, cancel, err := (invocation.Budget{Limit: value.ConnectTimeout}).Context(ctx, invocation.Establish)
		if err != nil {
			return result, err
		}
		defer cancel()
		owner.setupContext = work
		defer func() { owner.setupContext = nil }()
		if parent != nil {
			if value.Lazy || value.Endpoint != parent.owner.settings.Endpoint || value.Plaintext != parent.owner.settings.Plaintext || value.APIKey != "" || value.RootCAPEM != "" || value.CertificatePEM != "" || value.ServerName != "" ||
				value.MaxRequestBytes > parent.owner.settings.MaxRequestBytes || value.MaxResponseBytes > parent.owner.settings.MaxResponseBytes || parentBytes < value.reservation() ||
				runtime.Credentials != nil || runtime.HeadersProvider != nil || runtime.TrafficController != nil || !emptyConnectionOptions(runtime.ConnectionOptions) {
				return result, failure(ErrAuthority, "shared-transport")
			}
			owner.parentLease, err = parent.access.Acquire(work, parentBytes)
			if err != nil {
				return result, err
			}
			owner.root = parent.owner.transportOwner()
			owner.transport = parent.owner.transport
			work = context.WithValue(work, transportOwnerKey{}, owner)
		}
		options, err := nativeOptions(value, owner.capture, runtime)
		if err != nil {
			return result, err
		}
		if parent != nil {
			options.ConnectionOptions = copyClientOptions(parent.owner.effective).ConnectionOptions
			options.ConnectionOptions.MaxPayloadSize = max(value.MaxRequestBytes, value.MaxResponseBytes)
			options.ConnectionOptions.GetSystemInfoTimeout = value.ConnectTimeout
			options.Credentials = parent.owner.effective.Credentials
			options.HeadersProvider = parent.owner.effective.HeadersProvider
			options.TrafficController = parent.owner.effective.TrafficController
		}
		options, err = owner.preparePlugins(options)
		if err != nil {
			return result, err
		}
		err = contained("client-construction", func() error {
			var err error
			if parent != nil {
				owner.native, err = sdk.NewClientFromExistingWithContext(work, parent.owner.native, options)
			} else if value.Lazy {
				owner.native, err = sdk.NewLazyClient(options)
				if err == nil {
					_, captureErr := owner.native.WorkflowService().GetSystemInfo(context.WithValue(work, captureTransportKey{}, owner), &workflowservice.GetSystemInfoRequest{})
					if !errors.Is(captureErr, errTransportCaptured) {
						err = failure(ErrConnect, "lazy-transport", captureErr)
					}
				}
			} else {
				owner.native, err = sdk.DialContext(work, options)
			}
			return err
		})
		result.Acquired = owner.native != nil || owner.transport != nil
		if err != nil {
			return result, failure(ErrConnect, "dial", err, owner.setup.failure())
		}
		if owner.transport == nil {
			return result, failure(ErrConnect, "transport")
		}
		owner.ready.Store(true)
		if err := owner.setup.failure(); err != nil {
			return result, err
		}
		return result, nil
	}), nil
}

type captureTransportKey struct{}

var errTransportCaptured = errors.New("temporal transport captured without invocation")

func (owner *connection) capture(ctx context.Context, method string, request, reply any, transport *grpc.ClientConn, next grpc.UnaryInvoker, options ...grpc.CallOption) (err error) {
	owner.captureOnce.Do(func() { owner.transport = transport })
	if ctx.Value(captureTransportKey{}) == owner && !owner.ready.Load() {
		return errTransportCaptured
	}
	defer func() {
		if method == "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo" && err == nil {
			if response, ok := reply.(*workflowservice.GetSystemInfoResponse); ok {
				version := response.GetServerVersion()
				owner.serverVersion.Store(&version)
			}
		}
	}()
	ctx = metadata.NewOutgoingContext(ctx, nil)
	if binding, ok := ctx.Value(taskBindingKey{}).(*taskBinding); ok {
		bound := binding.worker.client.owner
		if bound.transportOwner() != owner {
			return failure(ErrAuthority, "callback-transport-owner")
		}
		return binding.invoke(ctx, bound, method, request, reply, transport, next, options...)
	}
	if authority, ok := ctx.Value(nativeCallKey{}).(*nativeCall); ok {
		if authority.owner.transportOwner() != owner || authority.closed.Load() {
			return failure(ErrAuthority, "expired-client-call")
		}
		if authority.callback && !ordinaryExecutionRPC(method) && !slices.Contains(authority.owner.settings.RPCs, method) {
			return failure(ErrAuthority, "callback-rpc")
		}
		return boundedNativeRPC(ctx, authority.owner.settings, method, request, reply, transport, next, options...)
	}
	bound := owner
	if selected, ok := ctx.Value(transportOwnerKey{}).(*connection); ok {
		if selected.transportOwner() != owner {
			return failure(ErrAuthority, "transport-owner")
		}
		bound = selected
	}
	if ctx.Value(activeRPCKey{}) == nil {
		if bound.ready.Load() {
			return failure(ErrAuthority, "native-context")
		}
		if method != "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo" {
			err := failure(ErrAuthority, "client-setup-rpc")
			bound.setup.failed(err)
			return err
		}
	}
	if ctx.Value(activeRPCKey{}) == nil {
		err = boundedNativeRPC(ctx, bound.settings, method, request, reply, transport, next, options...)
	} else {
		err = next(ctx, method, request, reply, transport, options...)
	}
	return err
}

func nativeOptions(value settings, rpcInterceptor grpc.UnaryClientInterceptor, runtime RuntimeOptions) (sdk.Options, error) {
	trust, err := transportTLS(value)
	if err != nil {
		return sdk.Options{}, err
	}
	logger := runtime.Logger
	if logger == nil {
		logger = disabledLogger{}
	}
	options := sdk.Options{HostPort: value.Endpoint, Namespace: value.Namespace, Identity: value.Identity, Logger: logger,
		DataConverter: runtime.DataConverter, FailureConverter: runtime.FailureConverter,
		ExternalStorage: runtime.ExternalStorage,
		MetricsHandler:  runtime.MetricsHandler, ContextPropagators: slices.Clone(runtime.ContextPropagators),
		Interceptors:                 slices.Clone(runtime.Interceptors),
		DisableWorkerEnvironmentInfo: !runtime.ReportWorkerEnvironment,
		DisableErrorCodeMetricTags:   runtime.DisableErrorCodeMetricTags,
		WorkerHeartbeatInterval:      runtime.WorkerHeartbeatInterval, SdkName: runtime.SdkName, SdkVersion: runtime.SdkVersion,
		PayloadLimits: runtime.PayloadLimits, Credentials: runtime.Credentials,
		HeadersProvider: runtime.HeadersProvider, TrafficController: runtime.TrafficController,
		ConnectionOptions: runtime.ConnectionOptions}
	if options.ConnectionOptions.TLS == nil {
		options.ConnectionOptions.TLS = trust
	} else {
		if value.Plaintext || value.RootCAPEM != "" || value.CertificatePEM != "" || value.ServerName != "" {
			return sdk.Options{}, failure(ErrInput, "conflicting-tls-options")
		}
		options.ConnectionOptions.TLS = options.ConnectionOptions.TLS.Clone()
	}
	if options.ConnectionOptions.TLSDisabled && !value.Plaintext {
		return sdk.Options{}, failure(ErrInput, "conflicting-tls-options")
	}
	options.ConnectionOptions.TLSDisabled = value.Plaintext
	if options.ConnectionOptions.MaxPayloadSize == 0 {
		options.ConnectionOptions.MaxPayloadSize = max(value.MaxRequestBytes, value.MaxResponseBytes)
	}
	if options.ConnectionOptions.GetSystemInfoTimeout == 0 {
		options.ConnectionOptions.GetSystemInfoTimeout = value.ConnectTimeout
	}
	if err := validateClientAuthority(value, options); err != nil {
		return sdk.Options{}, err
	}
	options.ConnectionOptions.DialOptions = ownedDialOptions(rpcInterceptor)
	if value.APIKey != "" {
		if runtime.Credentials != nil {
			return sdk.Options{}, failure(ErrInput, "conflicting-credentials")
		}
		options.Credentials = sdk.NewAPIKeyStaticCredentials(value.APIKey)
	}
	return options, nil
}

func (owner *connection) close(context.Context) resource.ReleaseResult {
	owner.closeOnce.Do(func() {
		owner.transportLifetime.close(func() {
			if owner.native != nil {
				owner.native.Close()
			} else if owner.transport != nil && owner.parentLease == nil {
				_ = owner.transport.Close()
			}
		})
		if owner.parentLease != nil {
			owner.parentLease.Release()
		}
	})
	return resource.ReleaseResult{Quiescent: true, Released: true}
}

// Client carries one authoritative resource access and independent evidence Inbox.
// Borrowing aliases share native transport and limits; they cannot close it.
type Client struct {
	private
	owner    *connection
	access   *resource.Access
	inbox    *invocation.Inbox[RPCResult]
	observer *invocation.Observer
}

// Bind rejects limits that do not cover the configured per-RPC wire envelope.
// The caller owns Inbox reception; ignored RPC errors do not release evidence.
func Bind(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[RPCResult], observer *invocation.Observer) (*Client, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	value, limits := source.owner.settings, access.Limits()
	if source.owner.sharedBytes > 0 && limits.Bytes > source.owner.sharedBytes || limits.Active > value.MaxActive || limits.Queued > value.QueuedCalls ||
		limits.Bytes < value.reservation() || limits.MaxLeases < 1 ||
		limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

// RPCReservation is a logical protobuf working envelope, not a heap/RSS bound.
func (client *Client) RPCReservation() int64 {
	if client == nil || client.owner == nil {
		return 0
	}
	return client.owner.settings.reservation()
}

// EvidenceBytes reserves only fixed-size technical RPC evidence, not returned
// protobufs or arbitrary native error graphs. Returned messages are caller-owned.
func (*Client) EvidenceBytes() int64 { return 1024 }

// Profile describes the selected process-RPC mode. A server version is an
// observation, not proof of namespace capabilities or service qualification.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value := client.owner.settings
	grants := slices.Clone(value.RPCs)
	slices.Sort(grants)
	profile := compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "temporal-process-rpc-v1",
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "temporal-api-v1"},
		Native:   compatibility.Fact{Kind: compatibility.NotApplicable},
		Options: []compatibility.Option{
			{Name: "shared-transport", Value: strconv.FormatBool(client.owner.parentLease != nil)},
			{Name: "lazy", Value: strconv.FormatBool(value.Lazy)},
			{Name: "plaintext", Value: strconv.FormatBool(value.Plaintext)},
			{Name: "rpc-grants", Value: fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(grants, "\n"))))},
			{Name: "request-bytes", Value: strconv.Itoa(value.MaxRequestBytes)},
			{Name: "response-bytes", Value: strconv.Itoa(value.MaxResponseBytes)},
			{Name: "rpc-timeout-ns", Value: strconv.FormatInt(int64(value.RPCTimeout), 10)},
			{Name: "admission-timeout-ns", Value: strconv.FormatInt(int64(value.AdmissionTimeout), 10)},
			{Name: "proxy", Value: "disabled"},
			{Name: "native-logger", Value: strconv.FormatBool(client.owner.runtime.Logger != nil)},
			{Name: "native-metrics", Value: strconv.FormatBool(client.owner.runtime.MetricsHandler != nil)},
			{Name: "native-interceptors", Value: strconv.Itoa(len(client.owner.runtime.Interceptors))},
			{Name: "context-propagators", Value: strconv.Itoa(len(client.owner.runtime.ContextPropagators))},
		}}
	version := ""
	if observed := client.owner.transportOwner().serverVersion.Load(); observed != nil {
		version = *observed
	}
	if len(version) > 0 && len(version) <= 32 && strings.Trim(version, "0123456789.") == "" {
		profile.ServiceVersion = compatibility.Fact{Kind: compatibility.Observed, Value: version}
	}
	return profile
}

func (client *Client) begin(ctx context.Context, correlation fault.Correlation, name string) (*invocation.Call[RPCResult], error) {
	if client == nil || client.owner == nil {
		return nil, failure(ErrInput, "rpc")
	}
	return invocation.Begin(ctx, client.access, invocation.Request{Name: name, Correlation: correlation,
		Shape: invocation.Finite, Bytes: client.RPCReservation(), EvidenceBytes: client.EvidenceBytes(),
		Admission: invocation.Budget{Limit: client.owner.settings.AdmissionTimeout}}, client.inbox, client.observer)
}

type transportOwnerKey struct{}

func (owner *connection) transportOwner() *connection {
	if owner.root != nil {
		return owner.root
	}
	return owner
}

func emptyConnectionOptions(options sdk.ConnectionOptions) bool {
	return reflect.DeepEqual(options, sdk.ConnectionOptions{})
}

func transportSettings(ctx context.Context, fallback settings) settings {
	if authority, ok := ctx.Value(nativeCallKey{}).(*nativeCall); ok {
		return authority.owner.settings
	}
	if owner, ok := ctx.Value(transportOwnerKey{}).(*connection); ok {
		return owner.settings
	}
	if binding, ok := ctx.Value(taskBindingKey{}).(*taskBinding); ok {
		return binding.worker.client.owner.settings
	}
	return fallback
}
