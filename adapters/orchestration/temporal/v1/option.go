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
	"net"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	nativelog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/resolver"
)

// ProviderID identifies the Adapter contract, independently of the SDK release.
const ProviderID = "orchestration.temporal.v1"

// Validate checks the data-only default native profile, not service readiness.
// Prepare validates settings together with explicitly selected native extensions.
func Validate(value Settings) error { _, err := Prepare(value, NativeOptions{}); return err }

// Settings is configuration format 1. Zero selects Adapter defaults for timeouts,
// active calls and wire sizes; zero queued calls disables queuing. Durations use
// nanoseconds. Endpoint, namespace, TLS and credentials are never discovered.
// Inputs are copied during Prepare; concurrent mutation during Prepare is invalid.
// Explicit JSON contains secrets. Ordinary formatting and slog are redacted.
type Settings struct {
	Name             string        `json:"name" mapstructure:"name"`
	Version          uint32        `json:"format" mapstructure:"format"`
	Endpoint         string        `json:"endpoint" mapstructure:"endpoint"`
	Namespace        string        `json:"namespace" mapstructure:"namespace"`
	Identity         string        `json:"identity" mapstructure:"identity"`
	Lazy             bool          `json:"lazy" mapstructure:"lazy"`
	Plaintext        bool          `json:"plaintext" mapstructure:"plaintext"`
	RootCAPEM        string        `json:"root_ca_pem" mapstructure:"root_ca_pem"`
	CertificatePEM   string        `json:"certificate_pem" mapstructure:"certificate_pem"`
	PrivateKeyPEM    string        `json:"private_key_pem" mapstructure:"private_key_pem"`
	ServerName       string        `json:"server_name" mapstructure:"server_name"`
	APIKey           string        `json:"api_key" mapstructure:"api_key"`
	RPCs             []string      `json:"rpcs" mapstructure:"rpcs"`
	ConnectTimeout   time.Duration `json:"connect_timeout_ns" mapstructure:"connect_timeout_ns"`
	RPCTimeout       time.Duration `json:"rpc_timeout_ns" mapstructure:"rpc_timeout_ns"`
	AdmissionTimeout time.Duration `json:"admission_timeout_ns" mapstructure:"admission_timeout_ns"`
	MaxActive        int           `json:"max_active" mapstructure:"max_active"`
	QueuedCalls      int           `json:"queued_calls" mapstructure:"queued_calls"`
	MaxRequestBytes  int           `json:"max_request_bytes" mapstructure:"max_request_bytes"`
	MaxResponseBytes int           `json:"max_response_bytes" mapstructure:"max_response_bytes"`
	// MaxUses bounds retained clients and independently owned Worker aliases.
	// Zero selects 64. It is not a remote execution limit.
	MaxUses int `json:"max_uses" mapstructure:"max_uses"`
	// InnerEvidenceCapacity bounds each native operation/RPC/Worker/task receiver.
	// Zero selects 64. Public receivers have separate caller-owned capacity.
	InnerEvidenceCapacity int `json:"inner_evidence_capacity" mapstructure:"inner_evidence_capacity"`
	// FamilyLimit bounds all simultaneously retained nodes in one operation or
	// Worker family, including its root. Zero selects 64; minimum is 3.
	FamilyLimit int `json:"family_limit" mapstructure:"family_limit"`
	// WorkerWorkBytes is the declared native Worker envelope. Zero selects the
	// minimum family wire envelope. It excludes arbitrary caller-owned Go heap.
	WorkerWorkBytes int64 `json:"worker_work_bytes" mapstructure:"worker_work_bytes"`
}

// HeadersProvider preserves the native per-call header hook.
type HeadersProvider interface {
	GetHeaders(context.Context) (map[string]string, error)
}

// TrafficController preserves the native test-oriented per-attempt hook.
type TrafficController interface {
	CheckCallAllowed(context.Context, string, any, any) error
}

// ResolverBinding names a per-source resolver without invoking it in Prepare.
type ResolverBinding struct {
	Scheme  string
	Builder resolver.Builder
}

// NativeOptions borrows native callbacks/objects until actual source shutdown.
// Containers are frozen; referenced objects must be concurrent-safe. Typed nil is
// refused. There is no arbitrary DialOptions escape, global provider fallback or
// automatic sanitization/cancellation of caller-owned observers and converters.
type NativeOptions struct {
	private
	Logger                     nativelog.Logger
	MetricsHandler             sdk.MetricsHandler
	Interceptors               []interceptor.ClientInterceptor
	ContextPropagators         []workflow.ContextPropagator
	DataConverter              converter.DataConverter
	FailureConverter           converter.FailureConverter
	ExternalStorage            converter.ExternalStorage
	Plugins                    []sdk.Plugin
	ConnectionOptions          sdk.ConnectionOptions
	Credentials                sdk.Credentials
	HeadersProvider            HeadersProvider
	TrafficController          TrafficController
	DisableErrorCodeMetricTags bool
	WorkerHeartbeatInterval    time.Duration
	SdkName, SdkVersion        string
	ReportWorkerEnvironment    bool
	PayloadLimits              sdk.PayloadLimitOptions
	ResolverBuilders           []ResolverBinding
	ContextDialer              func(context.Context, string) (net.Conn, error)
	UserAgent                  string
}

// Dependencies are explicit borrowed authority. Separate receivers prevent a
// live Worker record from occupying callback/operation evidence capacity.
type Dependencies struct {
	private
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Workers  *adapters.Inbox[WorkerResult]
	Tasks    *adapters.Inbox[TaskResult]
	Observer *adapters.Observer
	// Native is used by Open(Settings). Prepared.Open already owns its frozen
	// native selection and does not reinterpret this field.
	Native NativeOptions
}

func (value Settings) native() native.OptionsV1 {
	return native.OptionsV1{Name: value.Name, Version: value.Version, Endpoint: value.Endpoint,
		Namespace: value.Namespace, Identity: value.Identity, Lazy: value.Lazy, Plaintext: value.Plaintext,
		RootCAPEM: value.RootCAPEM, CertificatePEM: value.CertificatePEM, PrivateKeyPEM: value.PrivateKeyPEM,
		ServerName: value.ServerName, APIKey: value.APIKey, RPCs: value.RPCs,
		ConnectTimeout: value.ConnectTimeout, RPCTimeout: value.RPCTimeout, AdmissionTimeout: value.AdmissionTimeout,
		MaxActive: value.MaxActive, QueuedCalls: value.QueuedCalls, MaxRequestBytes: value.MaxRequestBytes, MaxResponseBytes: value.MaxResponseBytes}
}

func (value NativeOptions) native() native.RuntimeOptions {
	resolvers := make([]native.ResolverBinding, len(value.ResolverBuilders))
	for index, binding := range value.ResolverBuilders {
		resolvers[index] = native.ResolverBinding{Scheme: binding.Scheme, Builder: binding.Builder}
	}
	return native.RuntimeOptions{Logger: value.Logger, MetricsHandler: value.MetricsHandler,
		Interceptors: value.Interceptors, ContextPropagators: value.ContextPropagators,
		DataConverter: value.DataConverter, FailureConverter: value.FailureConverter, ExternalStorage: value.ExternalStorage,
		Plugins: value.Plugins, ConnectionOptions: value.ConnectionOptions, Credentials: value.Credentials,
		HeadersProvider: value.HeadersProvider, TrafficController: value.TrafficController,
		DisableErrorCodeMetricTags: value.DisableErrorCodeMetricTags, WorkerHeartbeatInterval: value.WorkerHeartbeatInterval,
		SdkName: value.SdkName, SdkVersion: value.SdkVersion, ReportWorkerEnvironment: value.ReportWorkerEnvironment,
		PayloadLimits: value.PayloadLimits, ResolverBuilders: resolvers, ContextDialer: value.ContextDialer, UserAgent: value.UserAgent}
}
