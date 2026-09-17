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
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	nativeworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
)

type clientPluginAdapter struct {
	sdk.PluginBase
	owner       *connection
	native      sdk.Plugin
	name        string
	first, last bool
}

type clientPluginName struct {
	sdk.PluginBase
	name string
}

func (plugin *clientPluginName) Name() string { return plugin.name }

func (plugin *clientPluginAdapter) Name() string { return plugin.name }

func copyClientOptions(options sdk.Options) sdk.Options {
	options.Interceptors = slices.Clone(options.Interceptors)
	options.ContextPropagators = slices.Clone(options.ContextPropagators)
	options.ExternalStorage.Drivers = slices.Clone(options.ExternalStorage.Drivers)
	options.Plugins = slices.Clone(options.Plugins)
	options.ConnectionOptions.DialOptions = slices.Clone(options.ConnectionOptions.DialOptions)
	if options.ConnectionOptions.TLS != nil {
		options.ConnectionOptions.TLS = options.ConnectionOptions.TLS.Clone()
	}
	return options
}

func validateClientAuthority(value settings, options sdk.Options) error {
	connection := options.ConnectionOptions
	if options.HostPort != value.Endpoint || options.Namespace != value.Namespace || options.Identity != value.Identity ||
		connection.TLSDisabled != value.Plaintext || value.Plaintext && (connection.TLS != nil || options.Credentials != nil) ||
		!value.Plaintext && connection.TLS == nil || connection.MaxPayloadSize < 1 ||
		connection.MaxPayloadSize > max(value.MaxRequestBytes, value.MaxResponseBytes) ||
		connection.GetSystemInfoTimeout <= 0 || connection.GetSystemInfoTimeout > value.ConnectTimeout ||
		connection.KeepAliveTime < 0 || connection.KeepAliveTimeout < 0 ||
		connection.Authority != "" && !validText(connection.Authority, 255) {
		return failure(ErrAuthority, "client-plugin-authority")
	}
	return nil
}

func runtimeFromClientOptions(original RuntimeOptions, options sdk.Options) (RuntimeOptions, error) {
	original.Logger, original.MetricsHandler = options.Logger, options.MetricsHandler
	if _, disabled := original.Logger.(disabledLogger); disabled {
		original.Logger = nil
	}
	original.Interceptors, original.ContextPropagators = options.Interceptors, options.ContextPropagators
	original.DataConverter, original.FailureConverter = options.DataConverter, options.FailureConverter
	original.ExternalStorage = options.ExternalStorage
	original.ConnectionOptions = options.ConnectionOptions
	original.ConnectionOptions.DialOptions = nil
	original.Credentials, original.HeadersProvider, original.TrafficController = options.Credentials, options.HeadersProvider, options.TrafficController
	original.DisableErrorCodeMetricTags = options.DisableErrorCodeMetricTags
	original.WorkerHeartbeatInterval, original.SdkName, original.SdkVersion = options.WorkerHeartbeatInterval, options.SdkName, options.SdkVersion
	original.ReportWorkerEnvironment = !options.DisableWorkerEnvironmentInfo
	original.PayloadLimits = options.PayloadLimits
	return freezeRuntime(original)
}

func (owner *connection) preparePlugins(options sdk.Options) (sdk.Options, error) {
	for index, plugin := range owner.runtime.Plugins {
		var name string
		if err := contained("client-plugin-name", func() error { name = plugin.Name(); return nil }); err != nil {
			return options, err
		}
		if !validText(name, 255) {
			return options, failure(ErrInput, "client-plugin-name")
		}
		adapter := &clientPluginAdapter{owner: owner, native: plugin, name: name, first: index == 0, last: index == len(owner.runtime.Plugins)-1}
		owner.plugins = append(owner.plugins, adapter)
		options.Plugins = append(options.Plugins, adapter)
	}
	if len(owner.plugins) == 0 {
		return owner.finalizeClientOptions(options)
	}
	return options, nil
}

func (adapter *clientPluginAdapter) ConfigureClient(ctx context.Context, supplied sdk.PluginConfigureClientOptions) error {
	if adapter.owner.setupContext != nil {
		var release func()
		ctx, release = mergeActivityContext(ctx, adapter.owner.setupContext)
		defer release()
	}
	options := copyClientOptions(*supplied.ClientOptions)
	options.Plugins = nil
	options.ConnectionOptions.DialOptions = nil
	shared := adapter.owner.parentLease != nil
	if shared {
		options.ConnectionOptions = sdk.ConnectionOptions{}
		options.Credentials, options.HeadersProvider, options.TrafficController = nil, nil, nil
	}
	if err := contained("client-plugin-configure", func() error {
		return adapter.native.ConfigureClient(ctx, sdk.PluginConfigureClientOptions{ClientOptions: &options})
	}); err != nil {
		return err
	}
	if len(options.Plugins) != 0 || len(options.ConnectionOptions.DialOptions) != 0 {
		return failure(ErrAuthority, "client-plugin-owned-chain")
	}
	if shared {
		if !emptyConnectionOptions(options.ConnectionOptions) || options.Credentials != nil || options.HeadersProvider != nil || options.TrafficController != nil {
			return failure(ErrAuthority, "shared-plugin-transport")
		}
		transport := copyClientOptions(*supplied.ClientOptions)
		options.ConnectionOptions = transport.ConnectionOptions
		options.ConnectionOptions.DialOptions = nil
		options.Credentials, options.HeadersProvider, options.TrafficController = transport.Credentials, transport.HeadersProvider, transport.TrafficController
	}
	if err := validateClientAuthority(adapter.owner.settings, options); err != nil {
		return err
	}
	if _, err := runtimeFromClientOptions(adapter.owner.runtime, options); err != nil {
		return err
	}
	accepted := copyClientOptions(options)
	accepted.Plugins = slices.Clone(supplied.ClientOptions.Plugins)
	accepted.ConnectionOptions.DialOptions = slices.Clone(supplied.ClientOptions.ConnectionOptions.DialOptions)
	if adapter.last {
		var err error
		accepted, err = adapter.owner.finalizeClientOptions(accepted)
		if err != nil {
			return err
		}
	}
	*supplied.ClientOptions = accepted
	return nil
}

func (owner *connection) finalizeClientOptions(options sdk.Options) (sdk.Options, error) {
	if options.Logger == nil {
		options.Logger = disabledLogger{}
	}
	if err := validateClientAuthority(owner.settings, options); err != nil {
		return options, err
	}
	effective, err := runtimeFromClientOptions(owner.runtime, options)
	if err != nil {
		return options, err
	}
	owner.runtime = effective
	owner.effective = copyClientOptions(options)
	owner.effective.Plugins = nil
	owner.effective.ConnectionOptions.DialOptions = nil
	if options.ConnectionOptions.TLS != nil {
		options.ConnectionOptions.DialOptions = owner.transportLifetime.secureDialOptions(options.ConnectionOptions.DialOptions, options.ConnectionOptions.TLS)
	}
	options.Interceptors = nil
	for _, extension := range effective.Interceptors {
		options.Interceptors = append(options.Interceptors, &clientFactory{native: extension, setup: &owner.setup, owner: owner})
	}
	options.HeadersProvider = controlledHeaders(options.HeadersProvider, owner.settings.Namespace)
	if options.TrafficController != nil {
		options.TrafficController = &trafficBoundary{native: options.TrafficController, settings: owner.settings}
	}
	return options, nil
}

func (owner *connection) pollingOptions(transport grpc.UnaryClientInterceptor, lifetime *transportLifetime) sdk.Options {
	options := copyClientOptions(owner.effective)
	options.Interceptors = nil
	options.HeadersProvider = controlledHeaders(options.HeadersProvider, owner.settings.Namespace)
	if options.TrafficController != nil {
		options.TrafficController = &trafficBoundary{native: options.TrafficController, settings: owner.settings}
	}
	for _, plugin := range owner.plugins {
		options.Plugins = append(options.Plugins, &clientPluginName{name: plugin.name})
	}
	options.ConnectionOptions.DialOptions = ownedDialOptions(transport)
	if options.ConnectionOptions.TLS != nil {
		options.ConnectionOptions.DialOptions = lifetime.secureDialOptions(options.ConnectionOptions.DialOptions, options.ConnectionOptions.TLS)
	}
	return options
}

func ownedDialOptions(transport grpc.UnaryClientInterceptor) []grpc.DialOption {
	return []grpc.DialOption{grpc.WithNoProxy(), grpc.WithDisableServiceConfig(), grpc.WithDisableRetry(),
		grpc.WithMaxHeaderListSize(16 << 10), grpc.WithUnaryInterceptor(transport), grpc.WithChainUnaryInterceptor(observeAttempt)}
}

// One native adapter runs the entire user NewClient chain. Returning a post-dial
// plugin error to the SDK would discard its already-created Client. Instead keep
// that error independently and let the SDK return the handle for assembly rollback.
func (adapter *clientPluginAdapter) NewClient(ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
	if !adapter.first {
		return next(ctx, options)
	}
	if adapter.owner.setupContext != nil {
		var release func()
		ctx, release = mergeActivityContext(ctx, adapter.owner.setupContext)
		defer release()
	}
	exposed := options
	exposed.ClientOptions = copyClientOptions(adapter.owner.effective)
	exposed.ClientOptions.MetricsHandler = options.ClientOptions.MetricsHandler
	exposed.ClientOptions.Plugins = nil
	exposed.ClientOptions.ConnectionOptions.DialOptions = nil
	if options.FromExisting != nil {
		exposed.FromExisting = &callbackClient{nativeClient: options.FromExisting}
	}
	var rootCalled, rootReturned bool
	var rootError error
	err := adapter.owner.newClientPlugin(0, ctx, exposed, func(work context.Context) error {
		rootCalled = true
		work = context.WithValue(work, transportOwnerKey{}, adapter.owner)
		rootError = next(work, options)
		rootReturned = true
		return rootError
	})
	if err != nil {
		adapter.owner.setup.failed(err)
	}
	if rootCalled && rootReturned {
		return rootError
	}
	if err == nil {
		err = failure(ErrConnect, "client-plugin-next-omitted")
	}
	return err
}

func (owner *connection) newClientPlugin(index int, ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context) error) error {
	if index == len(owner.plugins) {
		return next(ctx)
	}
	frame := &continuationFrame{}
	var called atomic.Bool
	var observed clientSetup
	exposed := options
	exposed.ClientOptions = copyClientOptions(options.ClientOptions)
	proceed := func(work context.Context, supplied sdk.PluginNewClientOptions) error {
		leave, err := frame.enter()
		if err != nil {
			observed.failed(err)
			return err
		}
		defer leave()
		if work == nil || supplied.FromExisting != options.FromExisting || supplied.Lazy != options.Lazy ||
			supplied.ClientOptions.HostPort != options.ClientOptions.HostPort || supplied.ClientOptions.Namespace != options.ClientOptions.Namespace ||
			supplied.ClientOptions.Identity != options.ClientOptions.Identity {
			err = failure(ErrAuthority, "client-plugin-next-options")
		} else if !called.CompareAndSwap(false, true) {
			err = failure(ErrAuthority, "client-plugin-next-duplicate")
		} else {
			bounded, release := mergeActivityContext(work, ctx)
			defer release()
			err = owner.newClientPlugin(index+1, bounded, options, next)
		}
		if err != nil {
			observed.failed(err)
		}
		return err
	}
	err := contained("client-plugin-new", func() error { return owner.plugins[index].native.NewClient(ctx, exposed, proceed) })
	frame.close()
	if !called.Load() && err == nil {
		err = failure(ErrAuthority, "client-plugin-next-omitted")
	}
	return errors.Join(err, observed.failure())
}

func (owner *connection) combinedWorkerPlugins() []nativeworker.Plugin {
	var result []nativeworker.Plugin
	for _, plugin := range owner.runtime.Plugins {
		if combined, ok := plugin.(nativeworker.Plugin); ok {
			result = append(result, combined)
		}
	}
	return result
}

type headerBoundary struct {
	native    HeadersProvider
	namespace string
}

func controlledHeaders(native HeadersProvider, namespace string) HeadersProvider {
	if native == nil {
		return nil
	}
	return &headerBoundary{native: native, namespace: namespace}
}

func (boundary *headerBoundary) GetHeaders(ctx context.Context) (map[string]string, error) {
	headers, err := boundary.native.GetHeaders(ctx)
	if err != nil {
		return nil, err
	}
	if len(headers) > 64 {
		return nil, failure(ErrLimit, "native-headers")
	}
	result := make(map[string]string, len(headers))
	size := 0
	for name, value := range headers {
		if strings.EqualFold(name, "temporal-namespace") && value != transportSettings(ctx, settings{Namespace: boundary.namespace}).Namespace {
			return nil, failure(ErrAuthority, "native-header-namespace")
		}
		size += len(name) + len(value)
		if size > 16<<10 || !validText(name, 256) || len(value) > 16<<10 {
			return nil, failure(ErrLimit, "native-headers")
		}
		result[name] = value
	}
	return result, nil
}

type trafficBoundary struct {
	native   TrafficController
	settings settings
}

func (boundary *trafficBoundary) CheckCallAllowed(ctx context.Context, method string, request, reply any) error {
	if err := boundary.native.CheckCallAllowed(ctx, method, request, reply); err != nil {
		return err
	}
	return validateNativeRequest(ctx, transportSettings(ctx, boundary.settings), request)
}

func copyWorkerOptions(options nativeworker.Options) nativeworker.Options {
	options.Interceptors = slices.Clone(options.Interceptors)
	options.Plugins = slices.Clone(options.Plugins)
	if options.MaxEagerActivityReservationsPerWorkflowTask != nil {
		value := *options.MaxEagerActivityReservationsPerWorkflowTask
		options.MaxEagerActivityReservationsPerWorkflowTask = &value
	}
	return options
}

func validateWorkerOptions(options nativeworker.Options) error {
	if len(options.Interceptors) > 32 || len(options.Plugins) > 32 ||
		options.EnableSessionWorker && (options.DeploymentOptions.UseVersioning || options.UseBuildIDForVersioning) {
		return failure(ErrInput, "worker-options")
	}
	for _, extension := range options.Interceptors {
		if nilRuntime(extension) {
			return failure(ErrInput, "worker-interceptor")
		}
	}
	for _, extension := range options.Plugins {
		if nilRuntime(extension) {
			return failure(ErrInput, "worker-plugin")
		}
	}
	for _, extension := range []any{options.BackgroundActivityContext, options.Tuner, options.SysInfoProvider,
		options.WorkflowTaskPollerBehavior, options.ActivityTaskPollerBehavior, options.NexusTaskPollerBehavior} {
		if extension != nil && nilRuntime(extension) {
			return failure(ErrInput, "worker-extension")
		}
	}
	return nil
}

type workerPlugins struct {
	worker        *Worker
	entries       []*workerPluginAdapter
	key           string
	hooks         nativeworker.PluginConfigureWorkerRegistryOptions
	release       func()
	mu            sync.Mutex
	registrations int
	services      int
}

type workerPluginAdapter struct {
	nativeworker.PluginBase
	owner  *workerPlugins
	native nativeworker.Plugin
	name   string
	last   bool
}

func (adapter *workerPluginAdapter) Name() string { return adapter.name }

// Native adapters configure options but never run user lifecycle hooks. The
// managed Stop continuation includes the native join, which cannot run inside
// SDK Stop because the SDK closes its stop-returned signal only after it returns.
func (owner *workerPlugins) prepare(options nativeworker.Options) (nativeworker.Options, error) {
	options.FathomryLifecycleV1 = true
	plugins := options.Plugins
	options.Plugins = nil
	for index, plugin := range plugins {
		var name string
		if err := contained("worker-plugin-name", func() error {
			name = plugin.Name()
			return nil
		}); err != nil {
			return options, err
		}
		if !validText(name, 255) {
			return options, failure(ErrInput, "worker-plugin-name")
		}
		adapter := &workerPluginAdapter{owner: owner, native: plugin, name: name, last: index == len(plugins)-1}
		owner.entries = append(owner.entries, adapter)
		options.Plugins = append(options.Plugins, adapter)
	}
	if len(plugins) == 0 {
		return owner.finalize(options)
	}
	return options, nil
}

func (adapter *workerPluginAdapter) ConfigureWorker(ctx context.Context, supplied nativeworker.PluginConfigureWorkerOptions) error {
	owner := adapter.owner
	owner.key = supplied.WorkerInstanceKey
	options := copyWorkerOptions(*supplied.WorkerOptions)
	options.Plugins = nil
	hooks := *supplied.WorkerRegistryOptions
	detached := supplied
	detached.WorkerOptions, detached.WorkerRegistryOptions = &options, &hooks
	if err := contained("worker-plugin-configure", func() error { return adapter.native.ConfigureWorker(ctx, detached) }); err != nil {
		return err
	}
	if len(options.Plugins) != 0 {
		return failure(ErrAuthority, "worker-plugin-list")
	}
	if err := validateWorkerOptions(options); err != nil {
		return err
	}
	accepted := copyWorkerOptions(options)
	accepted.Plugins = slices.Clone(supplied.WorkerOptions.Plugins)
	if adapter.last {
		owner.hooks = hooks
		hooks = nativeworker.PluginConfigureWorkerRegistryOptions{}
		var err error
		accepted, err = owner.finalize(accepted)
		if err != nil {
			return err
		}
	}
	*supplied.WorkerOptions, *supplied.WorkerRegistryOptions = accepted, hooks
	return nil
}

func (owner *workerPlugins) finalize(options nativeworker.Options) (nativeworker.Options, error) {
	if err := validateWorkerOptions(options); err != nil {
		return options, err
	}
	worker := owner.worker
	options.FathomryLifecycleV1 = true
	options.BackgroundActivityContext, owner.release = mergeActivityContext(options.BackgroundActivityContext, worker.lifetime)
	chain := []interceptor.WorkerInterceptor{&taskInterceptor{worker: worker}}
	for _, extension := range worker.client.owner.runtime.Interceptors {
		if native, ok := extension.(interceptor.WorkerInterceptor); ok {
			chain = append(chain, &workerFactory{native: native, worker: worker})
		}
	}
	for _, extension := range options.Interceptors {
		chain = append(chain, &workerFactory{native: extension, worker: worker})
	}
	options.Interceptors = chain
	onFatal := options.OnFatalError
	options.OnFatalError = func(err error) {
		worker.mu.Lock()
		if worker.fatalError == nil {
			worker.fatalError = err
		}
		worker.mu.Unlock()
		worker.requestStop()
		if onFatal != nil {
			callbackErr := contained("worker-fatal-callback", func() error { onFatal(err); return nil })
			worker.mu.Lock()
			worker.cleanupError = errors.Join(worker.cleanupError, callbackErr)
			worker.mu.Unlock()
		}
	}
	return options, nil
}

type activityDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (ctx activityDeadlineContext) Deadline() (time.Time, bool) { return ctx.deadline, true }

func mergeActivityContext(configured, lifetime context.Context) (context.Context, func()) {
	if configured == nil {
		return lifetime, func() {}
	}
	merged, cancel := context.WithCancelCause(configured)
	var result context.Context = merged
	if deadline, ok := lifetime.Deadline(); ok {
		if ownDeadline, ownOK := configured.Deadline(); !ownOK || deadline.Before(ownDeadline) {
			// A second deadline timer could win cancellation and erase the
			// lifetime's custom cause. Its owned callback is the sole actuator.
			result = activityDeadlineContext{Context: merged, deadline: deadline}
		}
	}
	done := make(chan struct{})
	stop := context.AfterFunc(lifetime, func() {
		defer close(done)
		cancel(context.Cause(lifetime))
	})
	return result, func() {
		if !stop() {
			<-done
		}
		cancel(context.Canceled)
	}
}

func (owner *workerPlugins) start(index int, ctx context.Context, native nativeworker.Worker) error {
	if index == len(owner.entries) {
		err := native.Start()
		owner.worker.mu.Lock()
		owner.worker.status.Started = err == nil
		owner.worker.mu.Unlock()
		return err
	}
	registry := &workerRegistry{owner: owner, native: native}
	options := nativeworker.PluginStartWorkerOptions{WorkerInstanceKey: owner.key, WorkerRegistry: registry}
	frame := &continuationFrame{}
	var called atomic.Bool
	var observed clientSetup
	next := func(nextContext context.Context, supplied nativeworker.PluginStartWorkerOptions) error {
		leave, err := frame.enter()
		if err != nil {
			observed.failed(err)
			return err
		}
		defer leave()
		sameRegistry, ok := supplied.WorkerRegistry.(*workerRegistry)
		if nextContext == nil || supplied.WorkerInstanceKey != options.WorkerInstanceKey || !ok || sameRegistry != registry {
			err = failure(ErrAuthority, "worker-plugin-start-options")
		} else if !called.CompareAndSwap(false, true) {
			err = failure(ErrAuthority, "worker-plugin-start-duplicate")
		} else {
			err = owner.start(index+1, nextContext, native)
		}
		if err != nil {
			observed.failed(err)
		}
		return err
	}
	err := contained("worker-plugin-start", func() error { return owner.entries[index].native.StartWorker(ctx, options, next) })
	active := frame.seal()
	registry.seal()
	if active != nil {
		<-active
	}
	registry.active.Wait()
	if !called.Load() && err == nil {
		err = failure(ErrAuthority, "worker-plugin-start-omitted")
	}
	return errors.Join(err, observed.failure(), registry.observed.failure())
}

func (owner *workerPlugins) stop(index int, ctx context.Context, stopNative func()) error {
	if index == len(owner.entries) {
		stopNative()
		return nil
	}
	options := nativeworker.PluginStopWorkerOptions{WorkerInstanceKey: owner.key}
	frame := &continuationFrame{}
	var called atomic.Bool
	var observed clientSetup
	next := func(nextContext context.Context, supplied nativeworker.PluginStopWorkerOptions) {
		leave, err := frame.enter()
		if err != nil {
			observed.failed(err)
			panic(err)
		}
		defer leave()
		if nextContext == nil || supplied.WorkerInstanceKey != options.WorkerInstanceKey {
			err = failure(ErrAuthority, "worker-plugin-stop-options")
		} else if !called.CompareAndSwap(false, true) {
			err = failure(ErrAuthority, "worker-plugin-stop-duplicate")
		}
		if err != nil {
			observed.failed(err)
			panic(err)
		}
		if err := owner.stop(index+1, nextContext, stopNative); err != nil {
			observed.failed(err)
		}
	}
	err := contained("worker-plugin-stop", func() error { owner.entries[index].native.StopWorker(ctx, options, next); return nil })
	frame.close()
	if !called.Load() {
		observed.failed(failure(ErrAuthority, "worker-plugin-stop-omitted"))
		err = errors.Join(err, owner.stop(index+1, ctx, stopNative))
	}
	return errors.Join(err, observed.failure())
}

// The private field deliberately does not promote Start, Stop, Run or the native
// client's interfaces. Each plugin receives a separately expiring registry view.
type workerRegistry struct {
	owner    *workerPlugins
	native   nativeworker.Worker
	mu       sync.Mutex
	closed   bool
	active   sync.WaitGroup
	observed clientSetup
}

func (registry *workerRegistry) enter(service bool) func() {
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		err := failure(ErrAuthority, "worker-registry-expired")
		registry.observed.failed(err)
		panic(err)
	}
	registry.active.Add(1)
	registry.mu.Unlock()
	owner := registry.owner
	owner.mu.Lock()
	refused := owner.registrations >= 256
	if service {
		refused = owner.services >= 32
		if !refused {
			owner.services++
		}
	} else if !refused {
		owner.registrations++
	}
	owner.mu.Unlock()
	if refused {
		registry.active.Done()
		err := failure(ErrLimit, "worker-registrations")
		registry.observed.failed(err)
		panic(err)
	}
	return registry.active.Done
}

func (registry *workerRegistry) register(service bool, invoke func()) {
	leave := registry.enter(service)
	defer leave()
	returned := false
	defer func() {
		value := recover()
		if !returned {
			cause, _ := value.(error)
			registry.observed.failed(failure(ErrWorker, "worker-registration", cause))
		}
		if value != nil {
			panic(value)
		}
	}()
	invoke()
	returned = true
}

func (registry *workerRegistry) seal() {
	registry.mu.Lock()
	registry.closed = true
	registry.mu.Unlock()
}

func (registry *workerRegistry) close() {
	registry.seal()
	registry.active.Wait()
}

func (registry *workerRegistry) RegisterWorkflowWithOptions(definition any, options workflow.RegisterOptions) {
	registry.register(false, func() {
		if hook := registry.owner.hooks.OnRegisterWorkflow; hook != nil {
			hook(definition, options)
		}
		registry.native.RegisterWorkflowWithOptions(definition, options)
	})
}

func (registry *workerRegistry) RegisterDynamicWorkflow(definition any, options workflow.DynamicRegisterOptions) {
	registry.register(false, func() {
		if hook := registry.owner.hooks.OnRegisterDynamicWorkflow; hook != nil {
			hook(definition, options)
		}
		registry.native.RegisterDynamicWorkflow(definition, options)
	})
}

func (registry *workerRegistry) RegisterActivityWithOptions(definition any, options activity.RegisterOptions) {
	registry.register(false, func() {
		if hook := registry.owner.hooks.OnRegisterActivity; hook != nil {
			hook(definition, options)
		}
		registry.native.RegisterActivityWithOptions(definition, options)
	})
}

func (registry *workerRegistry) RegisterDynamicActivity(definition any, options activity.DynamicRegisterOptions) {
	registry.register(false, func() {
		if hook := registry.owner.hooks.OnRegisterDynamicActivity; hook != nil {
			hook(definition, options)
		}
		registry.native.RegisterDynamicActivity(definition, options)
	})
}

func (registry *workerRegistry) RegisterNexusService(service *nexus.Service) {
	registry.register(true, func() {
		if hook := registry.owner.hooks.OnRegisterNexusService; hook != nil {
			hook(service)
		}
		registry.native.RegisterNexusService(service)
	})
}
