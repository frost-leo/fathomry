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
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"net"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/resource"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	nativelog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// OptionsV1 is borrowed during Select only; do not mutate it concurrently.
// Version zero selects format 1. No endpoint, namespace, credentials, proxy or
// configuration files are discovered from the environment. Native extension
// objects are not serializable configuration and are not accepted by this type.
type OptionsV1 struct {
	private
	Name    string
	Version uint32
	// Endpoint accepts host:port and native dns:///host:port or
	// passthrough:///host:port targets. DNS may include an explicit resolver
	// authority; its omitted port uses native port 53. The service port is required.
	Endpoint  string
	Namespace string
	Identity  string
	// Lazy constructs an owned native Client without service discovery. The
	// first admitted native operation performs discovery when required by SDK.
	Lazy bool
	// Plaintext must be explicit. Otherwise TLS uses system roots unless
	// RootCAPEM supplies an exclusive trust set. A client certificate and key
	// must be supplied together. APIKey is forbidden over plaintext.
	Plaintext      bool
	RootCAPEM      string
	CertificatePEM string
	PrivateKeyPEM  string
	ServerName     string
	APIKey         string
	// RPCs contains exact full method paths from KnownRPCs. Empty grants none.
	// A grant authorizes the entire native method, including its operational
	// effects. It is not a replacement for server-side authentication.
	RPCs []string
	// Defaults: 10 s per connection, 30 s per RPC and 1 s for admission.
	// Durations are nanoseconds in configuration layers.
	ConnectTimeout   time.Duration
	RPCTimeout       time.Duration
	AdmissionTimeout time.Duration
	// Defaults: 8 admitted RPCs, no queued RPCs, 4 MiB per request/response.
	// Byte bounds cover protobuf wire messages, not caller data or process RSS.
	MaxActive        int
	QueuedCalls      int
	MaxRequestBytes  int
	MaxResponseBytes int
}

type settings struct {
	Endpoint         string        `json:"endpoint"`
	Namespace        string        `json:"namespace"`
	Identity         string        `json:"identity"`
	Lazy             bool          `json:"lazy"`
	Plaintext        bool          `json:"plaintext"`
	RootCAPEM        string        `json:"root_ca_pem"`
	CertificatePEM   string        `json:"certificate_pem"`
	PrivateKeyPEM    string        `json:"private_key_pem"`
	ServerName       string        `json:"server_name"`
	APIKey           string        `json:"api_key"`
	RPCs             []string      `json:"rpcs"`
	ConnectTimeout   time.Duration `json:"connect_timeout_ns"`
	RPCTimeout       time.Duration `json:"rpc_timeout_ns"`
	AdmissionTimeout time.Duration `json:"admission_timeout_ns"`
	MaxActive        int           `json:"max_active"`
	QueuedCalls      int           `json:"queued_calls"`
	MaxRequestBytes  int           `json:"max_request_bytes"`
	MaxResponseBytes int           `json:"max_response_bytes"`
}

func defaults(input OptionsV1) settings {
	value := settings{Endpoint: input.Endpoint, Namespace: input.Namespace, Identity: input.Identity, Lazy: input.Lazy,
		Plaintext: input.Plaintext, RootCAPEM: input.RootCAPEM, CertificatePEM: input.CertificatePEM, PrivateKeyPEM: input.PrivateKeyPEM,
		ServerName: input.ServerName, APIKey: input.APIKey, RPCs: input.RPCs, ConnectTimeout: input.ConnectTimeout,
		RPCTimeout: input.RPCTimeout, AdmissionTimeout: input.AdmissionTimeout, MaxActive: input.MaxActive,
		QueuedCalls: input.QueuedCalls, MaxRequestBytes: input.MaxRequestBytes, MaxResponseBytes: input.MaxResponseBytes}
	if value.Identity == "" {
		value.Identity = "fathomry"
	}
	if value.ConnectTimeout == 0 {
		value.ConnectTimeout = 10 * time.Second
	}
	if value.RPCTimeout == 0 {
		value.RPCTimeout = 30 * time.Second
	}
	if value.AdmissionTimeout == 0 {
		value.AdmissionTimeout = time.Second
	}
	if value.MaxActive == 0 {
		value.MaxActive = 8
	}
	if value.MaxRequestBytes == 0 {
		value.MaxRequestBytes = 4 << 20
	}
	if value.MaxResponseBytes == 0 {
		value.MaxResponseBytes = 4 << 20
	}
	return value
}

func validText(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}

func validNetworkAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	portNumber, portError := strconv.Atoi(port)
	return err == nil && host != "" && !strings.ContainsAny(host, "/\\ \t\r\n\x00") &&
		portError == nil && portNumber >= 1 && portNumber <= 65535
}

func validEndpoint(endpoint string) bool {
	if !validText(endpoint, 512) {
		return false
	}
	if !strings.Contains(endpoint, "://") {
		return validNetworkAddress(endpoint)
	}
	target, err := url.Parse(endpoint)
	if err != nil || target.User != nil || strings.Contains(target.RawPath, "%") || target.RawQuery != "" || target.ForceQuery ||
		target.Fragment != "" || target.Opaque != "" || !strings.HasPrefix(target.Path, "/") {
		return false
	}
	switch target.Scheme {
	case "dns":
		if authority := target.Host; authority != "" {
			if target.Port() == "" {
				if strings.Contains(authority, ":") && (!strings.HasPrefix(authority, "[") || !strings.HasSuffix(authority, "]")) {
					return false
				}
				authority = net.JoinHostPort(target.Hostname(), "53")
			}
			if !validNetworkAddress(authority) {
				return false
			}
		}
	case "passthrough":
		if target.Host != "" {
			return false
		}
	default:
		return false
	}
	return validNetworkAddress(strings.TrimPrefix(target.Path, "/"))
}

func validate(value settings) error {
	if !validEndpoint(value.Endpoint) ||
		value.ServerName != "" && !validText(value.ServerName, 255) || strings.ContainsAny(value.APIKey, "\x00\r\n") ||
		!validText(value.Namespace, 255) || !validText(value.Identity, 128) ||
		value.ConnectTimeout <= 0 || value.ConnectTimeout > time.Minute ||
		value.RPCTimeout <= 0 || value.RPCTimeout > 24*time.Hour ||
		value.AdmissionTimeout <= 0 || value.AdmissionTimeout > time.Minute ||
		value.MaxActive < 1 || value.MaxActive > 1024 || value.QueuedCalls < 0 || value.QueuedCalls > 1024 ||
		value.MaxRequestBytes < 1024 || value.MaxRequestBytes > 64<<20 ||
		value.MaxResponseBytes < 1024 || value.MaxResponseBytes > 64<<20 ||
		len(value.RPCs) > 135 || len(value.APIKey) > 8192 || len(value.RootCAPEM) > 256<<10 ||
		len(value.CertificatePEM) > 64<<10 || len(value.PrivateKeyPEM) > 64<<10 {
		return failure(ErrInput, "configuration")
	}
	if value.Plaintext && (value.APIKey != "" || value.RootCAPEM != "" || value.CertificatePEM != "" || value.PrivateKeyPEM != "" || value.ServerName != "") {
		return failure(ErrInput, "plaintext")
	}
	if (value.CertificatePEM == "") != (value.PrivateKeyPEM == "") {
		return failure(ErrInput, "certificate")
	}
	methods := KnownRPCs()
	seen := make(map[string]bool, len(value.RPCs))
	for _, method := range value.RPCs {
		if !slices.Contains(methods, method) || seen[method] {
			return failure(ErrInput, "rpc-grant")
		}
		seen[method] = true
	}
	_, err := transportTLS(value)
	return err
}

func transportTLS(value settings) (*tls.Config, error) {
	if value.Plaintext {
		return nil, nil
	}
	trust := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: value.ServerName}
	if value.RootCAPEM != "" {
		trust.RootCAs = x509.NewCertPool()
		if !trust.RootCAs.AppendCertsFromPEM([]byte(value.RootCAPEM)) {
			return nil, failure(ErrInput, "tls-roots")
		}
	}
	if value.CertificatePEM != "" {
		certificate, err := tls.X509KeyPair([]byte(value.CertificatePEM), []byte(value.PrivateKeyPEM))
		if err != nil {
			return nil, failure(ErrInput, "certificate", err)
		}
		trust.Certificates = []tls.Certificate{certificate}
	}
	return trust, nil
}

func (value settings) reservation() int64 {
	return int64(value.MaxRequestBytes) + int64(value.MaxResponseBytes) + 16<<10
}

// KnownRPCs returns a new sorted inventory of the selected protobuf service
// methods. Presence here does not imply server implementation or authorization.
func KnownRPCs() []string {
	var methods []string
	for _, file := range []protoreflect.FileDescriptor{
		workflowservice.File_temporal_api_workflowservice_v1_service_proto,
		operatorservice.File_temporal_api_operatorservice_v1_service_proto,
	} {
		services := file.Services()
		for index := 0; index < services.Len(); index++ {
			service := services.Get(index)
			for method := 0; method < service.Methods().Len(); method++ {
				methods = append(methods, "/"+string(service.FullName())+"/"+string(service.Methods().Get(method).Name()))
			}
		}
	}
	slices.Sort(methods)
	return methods
}

func prepare(options OptionsV1, layers []resource.Layer) (resource.Prepared[settings], error) {
	format := options.Version
	if format == 0 {
		format = 1
	}
	return resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: format, Layers: layers})
}

// RuntimeOptions contains explicit, borrowed native extensions. Unlike OptionsV1,
// it is not configuration data and cannot be reconstructed by configuration layers.
// Values are copied by SelectWithRuntime; the referenced objects remain borrowed.
type RuntimeOptions struct {
	private
	// Logger is the native SDK logger, shared by the Client and its managed
	// Workers. Nil disables native logging; typed nil is rejected. The native
	// optional WithLogger and WithSkipCallers contracts remain unchanged.
	//
	// The logger must be concurrency-safe and outlive this source and all its
	// Workers. Composition declares any resource dependencies before this source,
	// using resource.Borrow across assemblies. This integration never closes or
	// synchronizes the logger and creates no logging queue, adapter or exporter.
	//
	// Formatting, filtering, buffering, bounds, privacy and failure reporting
	// belong to the upper adapter. Native logging has no error return or caller
	// context. A blocking callback can therefore delay SDK execution and cleanup;
	// a timeout is not proof that callback use ended. Application-owned background
	// logging work must be drained/joined by that adapter's owner, not Worker.Stop.
	Logger nativelog.Logger
	// MetricsHandler preserves native counters, gauges, timers, tags and replay
	// suppression. Nil selects the SDK no-op handler. The upper adapter owns
	// cardinality, export, callback concurrency and any background work.
	MetricsHandler sdk.MetricsHandler
	// Interceptors are native client interceptors, in native wrapping order.
	// Combined client/worker interceptors (including tracing) also wrap managed
	// worker callbacks, inside the mandatory task admission/ownership boundary.
	// Do not register the same interceptor again in WorkerSpec.Options.
	// Factories and returned interceptors are borrowed, not cloned. No concrete
	// tracing provider is selected or closed here. At most 32 entries are allowed.
	Interceptors []interceptor.ClientInterceptor
	// ContextPropagators use native process and deterministic Workflow contexts.
	// Workflow methods must obey native replay/determinism rules, not call process
	// resource/invocation machinery. At most 32 entries are allowed.
	ContextPropagators []workflow.ContextPropagator
	// DataConverter and FailureConverter preserve the native encoding, codec,
	// serialization-context and error contracts. Nil selects the SDK defaults;
	// typed nil is rejected. They are borrowed for the source/Worker lifetime.
	// Workflow-side methods must remain replay-compatible and deterministic.
	DataConverter    converter.DataConverter
	FailureConverter converter.FailureConverter
	// ExternalStorage is the native experimental payload-storage contract. Driver
	// and selector objects are borrowed; composition owns their connections,
	// bounded retrieval, claim authorization, integrity and history retention.
	// Wire limits apply to references, not to allocations inside a driver.
	ExternalStorage converter.ExternalStorage
	// Plugins retain native ConfigureClient/NewClient and combined Worker plugin
	// order. Option containers and continuations are scoped; extension objects
	// remain borrowed. At most 32 plugins may be supplied explicitly.
	Plugins []sdk.Plugin
	// ConnectionOptions preserves native TLS, keepalive and compression options.
	// Namespace/endpoint, wire bounds and the owned dial chain cannot be changed
	// through it; opaque DialOptions are refused. TLS material/callbacks are borrowed.
	ConnectionOptions          sdk.ConnectionOptions
	Credentials                sdk.Credentials
	HeadersProvider            HeadersProvider
	TrafficController          TrafficController
	DisableErrorCodeMetricTags bool
	WorkerHeartbeatInterval    time.Duration
	SdkName, SdkVersion        string
	// ReportWorkerEnvironment explicitly enables native environment reporting.
	ReportWorkerEnvironment bool
	PayloadLimits           sdk.PayloadLimitOptions
}

// HeadersProvider matches the native client.Options hook, whose interface type
// is not separately exported by the selected SDK's public client package.
type HeadersProvider interface {
	GetHeaders(context.Context) (map[string]string, error)
}

// TrafficController is the native test-oriented per-attempt admission hook.
// Changes to request data remain subject to source namespace and message bounds.
type TrafficController interface {
	CheckCallAllowed(context.Context, string, any, any) error
}

func (*RuntimeOptions) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

func nilRuntime(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	}
	return false
}

func freezeRuntime(value RuntimeOptions) (RuntimeOptions, error) {
	if value.Logger != nil && nilRuntime(value.Logger) || value.MetricsHandler != nil && nilRuntime(value.MetricsHandler) ||
		value.DataConverter != nil && nilRuntime(value.DataConverter) || value.FailureConverter != nil && nilRuntime(value.FailureConverter) ||
		len(value.Interceptors) > 32 || len(value.ContextPropagators) > 32 || len(value.ExternalStorage.Drivers) > 32 || len(value.Plugins) > 32 ||
		value.ExternalStorage.PayloadSizeThreshold < 0 || value.ExternalStorage.DriverSelector != nil && nilRuntime(value.ExternalStorage.DriverSelector) {
		return RuntimeOptions{}, failure(ErrInput, "runtime-binding")
	}
	if len(value.ConnectionOptions.DialOptions) != 0 || value.PayloadLimits.PayloadSizeWarning < 0 || value.PayloadLimits.MemoSizeWarning < 0 ||
		value.WorkerHeartbeatInterval > 0 && (value.WorkerHeartbeatInterval < time.Second || value.WorkerHeartbeatInterval > time.Minute) ||
		value.SdkName != "" && !validText(value.SdkName, 128) || value.SdkVersion != "" && !validText(value.SdkVersion, 128) {
		return RuntimeOptions{}, failure(ErrInput, "native-options")
	}
	for _, extension := range []any{value.Credentials, value.HeadersProvider, value.TrafficController, value.ConnectionOptions.GrpcCompression} {
		if extension != nil && nilRuntime(extension) {
			return RuntimeOptions{}, failure(ErrInput, "native-extension")
		}
	}
	for _, plugin := range value.Plugins {
		if nilRuntime(plugin) {
			return RuntimeOptions{}, failure(ErrInput, "client-plugin")
		}
	}
	for _, extension := range value.Interceptors {
		if nilRuntime(extension) {
			return RuntimeOptions{}, failure(ErrInput, "client-interceptor")
		}
	}
	for _, extension := range value.ContextPropagators {
		if nilRuntime(extension) {
			return RuntimeOptions{}, failure(ErrInput, "context-propagator")
		}
	}
	for _, driver := range value.ExternalStorage.Drivers {
		if nilRuntime(driver) {
			return RuntimeOptions{}, failure(ErrInput, "storage-driver")
		}
	}
	value.ExternalStorage.Drivers = slices.Clone(value.ExternalStorage.Drivers)
	value.Interceptors = slices.Clone(value.Interceptors)
	value.ContextPropagators = slices.Clone(value.ContextPropagators)
	value.Plugins = slices.Clone(value.Plugins)
	if value.ConnectionOptions.TLS != nil {
		value.ConnectionOptions.TLS = value.ConnectionOptions.TLS.Clone()
	}
	return value, nil
}
