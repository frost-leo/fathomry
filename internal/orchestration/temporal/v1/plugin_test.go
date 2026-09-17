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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type controlledClientPlugin struct {
	sdk.PluginBase
	configure func(context.Context, sdk.PluginConfigureClientOptions) error
	create    func(context.Context, sdk.PluginNewClientOptions, func(context.Context, sdk.PluginNewClientOptions) error) error
}

func (*controlledClientPlugin) Name() string { return "controlled-client-plugin" }

func (plugin *controlledClientPlugin) ConfigureClient(ctx context.Context, options sdk.PluginConfigureClientOptions) error {
	if plugin.configure != nil {
		return plugin.configure(ctx, options)
	}
	return nil
}

func (plugin *controlledClientPlugin) NewClient(ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
	if plugin.create != nil {
		return plugin.create(ctx, options, next)
	}
	return next(ctx, options)
}

func TestClientPluginInitializationFailuresRetainRollback(t *testing.T) {
	fixture := newFixture(t, 1)
	marker := errors.New("plugin failure marker")
	for _, mode := range []string{"configure-error", "configure-panic", "configure-goexit", "omit", "duplicate", "after-dial-error", "after-dial-panic", "after-dial-goexit", "namespace", "dial-chain"} {
		t.Run(mode, func(t *testing.T) {
			plugin := &controlledClientPlugin{
				configure: func(_ context.Context, options sdk.PluginConfigureClientOptions) error {
					switch mode {
					case "configure-error":
						return marker
					case "configure-panic":
						panic(marker)
					case "configure-goexit":
						runtime.Goexit()
					case "namespace":
						options.ClientOptions.Namespace = "another"
					case "dial-chain":
						options.ClientOptions.ConnectionOptions.DialOptions = []grpc.DialOption{grpc.WithUnaryInterceptor(func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
							return nil
						})}
					}
					return nil
				},
				create: func(ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
					if mode == "omit" {
						return nil
					}
					if err := next(ctx, options); err != nil {
						return err
					}
					switch mode {
					case "duplicate":
						_ = next(ctx, options)
					case "after-dial-error":
						return marker
					case "after-dial-panic":
						panic(marker)
					case "after-dial-goexit":
						runtime.Goexit()
					}
					return nil
				},
			}
			selected, err := temporal.SelectWithRuntime(temporal.OptionsV1{Name: "client-plugin-failure", Endpoint: fixture.endpoint, Namespace: "test", Plaintext: true},
				temporal.RuntimeOptions{Plugins: []sdk.Plugin{plugin}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			assembly, err := resource.Assemble(ctx, ctx, "client-plugin-failure", selected)
			if err == nil || assembly == nil {
				t.Fatal("plugin failure disappeared", err)
			}
			if mode == "configure-error" || mode == "configure-panic" || mode == "after-dial-error" || mode == "after-dial-panic" {
				if !errors.Is(err, marker) {
					t.Fatal("plugin error identity lost", err)
				}
			}
			for _, source := range assembly.Snapshot().Sources {
				if !source.Quiescent || !source.Released {
					t.Fatal("plugin failure abandoned acquired resources")
				}
			}
			if err := assembly.Close(ctx); err != nil {
				t.Fatal("rollback did not finish", err)
			}
		})
	}
}

func TestClientPluginReadOnlyViewsAndLateNextCannotReopenClient(t *testing.T) {
	var configured *sdk.Options
	var retained sdk.PluginNewClientOptions
	var proceed func(context.Context, sdk.PluginNewClientOptions) error
	plugin := &controlledClientPlugin{
		configure: func(_ context.Context, options sdk.PluginConfigureClientOptions) error {
			configured = options.ClientOptions
			return nil
		},
		create: func(ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
			retained, proceed = options, next
			return next(ctx, options)
		},
	}
	fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{Plugins: []sdk.Plugin{plugin}}, nil)
	if len(configured.Plugins) != 0 || len(configured.ConnectionOptions.DialOptions) != 0 || len(configured.Interceptors) != 0 {
		t.Fatal("Configure pointer acquired private guards")
	}
	if len(retained.ClientOptions.Plugins) != 0 || len(retained.ClientOptions.ConnectionOptions.DialOptions) != 0 {
		t.Fatal("NewClient options exposed private chain")
	}
	configured.Namespace = "changed-after-configure"
	executions, inbox := executionBinding(t, fixture)
	if _, err := executions.CountWorkflow(context.Background(), fault.Correlation{Call: "plugin-live"}, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"}); err != nil {
		t.Fatal(err)
	}
	releaseServiceEvidence(t, inbox)
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := proceed(context.Background(), retained); !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("retained dial continuation did not expire", err)
	}
}

type pluginHeaders struct{}

type metadataInjectionInterceptor struct {
	interceptor.ClientInterceptorBase
}

type metadataInjectionOutbound struct {
	interceptor.ClientOutboundInterceptorBase
}

func (*metadataInjectionInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &metadataInjectionOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}}
}

func (outbound *metadataInjectionOutbound) ExecuteWorkflow(ctx context.Context, input *interceptor.ClientExecuteWorkflowInput) (sdk.WorkflowRun, error) {
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "synthetic-injected", "temporal-namespace", "foreign"))
	return outbound.Next.ExecuteWorkflow(ctx, input)
}

func TestClientInterceptorCannotInjectTransportAuthority(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{Interceptors: []interceptor.ClientInterceptor{&metadataInjectionInterceptor{}}}, nil,
		func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
			peer.start = func(ctx context.Context, _ *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
				values, _ := metadata.FromIncomingContext(ctx)
				if len(values.Get("authorization")) != 0 || len(values.Get("temporal-namespace")) != 1 || values.Get("temporal-namespace")[0] != "test" {
					return nil, errors.New("interceptor transport authority escaped")
				}
				return &workflowservice.StartWorkflowExecutionResponse{RunId: "controlled-run"}, nil
			}
		})
	executions, _ := executionBinding(t, fixture)
	if _, err := executions.ExecuteWorkflow(context.Background(), fault.Correlation{Call: "metadata-interceptor"}, sdk.StartWorkflowOptions{ID: "controlled", TaskQueue: "unit"}, "workflow"); err != nil {
		t.Fatal(err)
	}
}

func (pluginHeaders) GetHeaders(context.Context) (map[string]string, error) {
	return map[string]string{"x-fixture": "source-header"}, nil
}

type pluginTraffic struct{ retarget bool }

func (controller *pluginTraffic) CheckCallAllowed(_ context.Context, _ string, request, reply any) error {
	if input, ok := request.(*workflowservice.CountWorkflowExecutionsRequest); ok && controller.retarget {
		input.Namespace = "another"
	}
	return nil
}

func TestNativeHeaderAndTrafficHooksCannotRetargetNamespace(t *testing.T) {
	for _, retarget := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "retarget-refused"}[retarget], func(t *testing.T) {
			controller := &pluginTraffic{retarget: retarget}
			fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{HeadersProvider: pluginHeaders{}, TrafficController: controller}, nil,
				func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
					peer.intercept = func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
						if _, ok := request.(*workflowservice.CountWorkflowExecutionsRequest); ok {
							headers, _ := metadata.FromIncomingContext(ctx)
							if len(headers.Get("x-fixture")) != 1 || headers.Get("x-fixture")[0] != "source-header" {
								return nil, errors.New("native header absent")
							}
						}
						return next(ctx, request)
					}
				})
			executions, inbox := executionBinding(t, fixture)
			value, err := executions.CountWorkflow(context.Background(), fault.Correlation{Call: "native-hooks"}, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
			if !retarget && (err != nil || value.Count != 7) {
				t.Fatal("normal native extension control failed", err)
			}
			if retarget && err == nil {
				t.Fatal("traffic hook retargeted selected namespace")
			}
			result := receiveExecution(t, inbox)
			if retarget && (result.Outcome.Value.Accepted || fixture.server.calls.Load() != 0) {
				t.Fatal("refused hook invented an acknowledgement or reached server")
			}
		})
	}
}

func TestCombinedClientPluginConfiguresWorkerExactlyOnce(t *testing.T) {
	var configured, initialized, workerConfigured, workerStarted, workerStopped atomic.Int32
	var sourceConverter converter.DataConverter = converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), versionCodec{version: "2", legacy: true})
	plugin, err := sdktemporal.NewSimplePlugin(sdktemporal.SimplePluginOptions{Name: "combined-fixture", DataConverter: sourceConverter,
		ConfigureClient: func(context.Context, sdk.PluginConfigureClientOptions) error { configured.Add(1); return nil },
		ConfigureWorker: func(context.Context, worker.PluginConfigureWorkerOptions) error { workerConfigured.Add(1); return nil },
		RunContextBefore: func(context.Context, sdktemporal.SimplePluginRunContextBeforeOptions) error {
			workerStarted.Add(1)
			return nil
		},
		RunContextAfter: func(context.Context, sdktemporal.SimplePluginRunContextAfterOptions) { workerStopped.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &controlledClientPlugin{create: func(ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
		initialized.Add(1)
		if options.ClientOptions.DataConverter != sourceConverter {
			return errors.New("configured converter not visible to NewClient")
		}
		return next(ctx, options)
	}}
	fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{Plugins: []sdk.Plugin{plugin, observer}}, nil)
	executions, _ := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "combined-plugin"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(), Options: worker.Options{LocalActivityWorkerOnly: true},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() { _ = managed.Stop(context.Background()) })
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if configured.Load() != 1 || initialized.Load() != 1 || workerConfigured.Load() != 1 || workerStarted.Load() != 1 || workerStopped.Load() != 1 {
		t.Fatalf("combined plugin callbacks repeated or absent: %d/%d/%d/%d/%d", configured.Load(), initialized.Load(), workerConfigured.Load(), workerStarted.Load(), workerStopped.Load())
	}
}

type controlledWorkerPlugin struct {
	worker.PluginBase
	configure func(context.Context, worker.PluginConfigureWorkerOptions) error
	start     func(context.Context, worker.PluginStartWorkerOptions, func(context.Context, worker.PluginStartWorkerOptions) error) error
	stop      func(context.Context, worker.PluginStopWorkerOptions, func(context.Context, worker.PluginStopWorkerOptions))
}

func (*controlledWorkerPlugin) Name() string { return "controlled-plugin" }

func (plugin *controlledWorkerPlugin) ConfigureWorker(ctx context.Context, options worker.PluginConfigureWorkerOptions) error {
	if plugin.configure != nil {
		return plugin.configure(ctx, options)
	}
	return nil
}

func (plugin *controlledWorkerPlugin) StartWorker(ctx context.Context, options worker.PluginStartWorkerOptions, next func(context.Context, worker.PluginStartWorkerOptions) error) error {
	if plugin.start != nil {
		return plugin.start(ctx, options, next)
	}
	return next(ctx, options)
}

func (plugin *controlledWorkerPlugin) StopWorker(ctx context.Context, options worker.PluginStopWorkerOptions, next func(context.Context, worker.PluginStopWorkerOptions)) {
	if plugin.stop != nil {
		plugin.stop(ctx, options, next)
		return
	}
	next(ctx, options)
}

func TestWorkerPluginStartContinuationsAndRegistryExpire(t *testing.T) {
	fixture := newFixture(t, 1)
	executions, _ := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	var retained worker.PluginStartWorkerOptions
	var retainedNext func(context.Context, worker.PluginStartWorkerOptions) error
	var retainedOptions *worker.Options
	var registrationCalls atomic.Int32
	dynamicOptions := workflow.DynamicRegisterOptions{LoadDynamicRuntimeOptions: func(workflow.LoadDynamicRuntimeOptionsDetails) (workflow.DynamicRuntimeOptions, error) {
		return workflow.DynamicRuntimeOptions{VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade}, nil
	}}
	plugin := &controlledWorkerPlugin{
		configure: func(_ context.Context, options worker.PluginConfigureWorkerOptions) error {
			retainedOptions = options.WorkerOptions
			options.WorkerOptions.FathomryLifecycleV1 = false
			options.WorkerRegistryOptions.OnRegisterDynamicActivity = func(any, activity.DynamicRegisterOptions) { registrationCalls.Add(1) }
			options.WorkerRegistryOptions.OnRegisterDynamicWorkflow = func(_ any, actual workflow.DynamicRegisterOptions) {
				if actual.LoadDynamicRuntimeOptions == nil {
					panic("dynamic workflow options changed")
				}
				loaded, err := actual.LoadDynamicRuntimeOptions(workflow.LoadDynamicRuntimeOptionsDetails{})
				if err != nil || loaded.VersioningBehavior != workflow.VersioningBehaviorAutoUpgrade {
					panic("dynamic workflow options changed")
				}
				registrationCalls.Add(1)
			}
			return nil
		},
		start: func(ctx context.Context, options worker.PluginStartWorkerOptions, next func(context.Context, worker.PluginStartWorkerOptions) error) error {
			retained, retainedNext = options, next
			if _, escaped := options.WorkerRegistry.(worker.Worker); escaped {
				return errors.New("native worker escaped")
			}
			options.WorkerRegistry.RegisterDynamicActivity(func(context.Context, converter.EncodedValues) error { return nil }, activity.DynamicRegisterOptions{})
			options.WorkerRegistry.RegisterDynamicWorkflow(func(workflow.Context, converter.EncodedValues) error { return nil }, dynamicOptions)
			return next(ctx, options)
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "plugin-start"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options: worker.Options{LocalActivityWorkerOnly: true, Plugins: []worker.Plugin{plugin}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() { _ = managed.Stop(context.Background()) })
	}
	if err != nil {
		t.Fatal(err)
	}
	if registrationCalls.Load() != 2 {
		t.Fatal("dynamic registration hooks were not honored")
	}
	if len(retainedOptions.Plugins) != 0 || len(retainedOptions.Interceptors) != 0 || retainedOptions.OnFatalError != nil || retainedOptions.FathomryLifecycleV1 {
		t.Fatal("private finalized options were published through retained ConfigureWorker pointer")
	}
	retainedOptions.Interceptors = nil
	retainedOptions.FathomryLifecycleV1 = false
	if err := retainedNext(ctx, retained); !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("late next was accepted", err)
	}
	var refused any
	func() {
		defer func() { refused = recover() }()
		retained.WorkerRegistry.RegisterActivityWithOptions(func() error { return nil }, activity.RegisterOptions{Name: "late"})
	}()
	if err, ok := refused.(error); !ok || !errors.Is(err, temporal.ErrAuthority) {
		t.Fatal("late registration was accepted")
	}
	if err := managed.Stop(ctx); err != nil || !managed.Status().Joined {
		t.Fatal("plugin bypassed final lifecycle options", err)
	}
}

func TestWorkerPluginStartFailureStillOwnsActualStartup(t *testing.T) {
	for _, mode := range []string{"omit", "duplicate", "after-start", "panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newFixture(t, 1)
			executions, _ := executionBinding(t, fixture)
			workers, tasks := workerInboxes(t)
			plugin := &controlledWorkerPlugin{start: func(ctx context.Context, options worker.PluginStartWorkerOptions, next func(context.Context, worker.PluginStartWorkerOptions) error) error {
				switch mode {
				case "omit":
					return nil
				case "panic":
					panic("start failure")
				case "goexit":
					runtime.Goexit()
				}
				if err := next(ctx, options); err != nil {
					return err
				}
				if mode == "duplicate" {
					_ = next(ctx, options)
					return nil
				}
				return errors.New("plugin failed after native start")
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			lifetime, stopLifetime := context.WithCancel(context.Background())
			defer stopLifetime()
			managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "plugin-failure"}, temporal.WorkerSpec{
				TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
				Options: worker.Options{LocalActivityWorkerOnly: true, Plugins: []worker.Plugin{plugin}},
			}, workers, tasks)
			if managed == nil || err == nil {
				t.Fatal("plugin failure lost ownership or evidence", err)
			}
			if err := managed.Stop(ctx); err == nil {
				t.Fatal("startup error was erased")
			}
			status := managed.Status()
			if !status.Joined || !status.NativeStopReturned || status.Started != (mode == "duplicate" || mode == "after-start") {
				t.Fatalf("startup/cleanup facts incorrect: %+v", status)
			}
		})
	}
}

func TestWorkerPluginStopFallbackDoesNotNeedNewEvidenceCapacity(t *testing.T) {
	for _, mode := range []string{"normal", "omit", "duplicate", "panic", "goexit", "changed-options"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newFixture(t, 1)
			executions, _ := executionBinding(t, fixture)
			workers, tasks := workerInboxes(t)
			var innerStops atomic.Int32
			inner := &controlledWorkerPlugin{stop: func(ctx context.Context, options worker.PluginStopWorkerOptions, next func(context.Context, worker.PluginStopWorkerOptions)) {
				innerStops.Add(1)
				next(ctx, options)
			}}
			outer := &controlledWorkerPlugin{stop: func(ctx context.Context, options worker.PluginStopWorkerOptions, next func(context.Context, worker.PluginStopWorkerOptions)) {
				switch mode {
				case "omit":
					return
				case "panic":
					panic("stop failure")
				case "goexit":
					runtime.Goexit()
				case "changed-options":
					options.WorkerInstanceKey = "another-worker"
				}
				next(ctx, options)
				if mode == "duplicate" {
					defer func() { _ = recover() }()
					next(ctx, options)
				}
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			lifetime, stopLifetime := context.WithCancel(context.Background())
			defer stopLifetime()
			managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "plugin-stop"}, temporal.WorkerSpec{
				TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
				Options: worker.Options{LocalActivityWorkerOnly: true, Plugins: []worker.Plugin{outer, inner}},
			}, workers, tasks)
			if err != nil {
				t.Fatal(err)
			}
			if err := managed.Stop(ctx); (err != nil) != (mode != "normal") {
				t.Fatal("stop violation evidence differs", err)
			}
			if innerStops.Load() != 1 || !managed.Status().Joined || !managed.Status().NativeStopReturned {
				t.Fatal("native or inner cleanup skipped/duplicated")
			}
			if err := fixture.assembly.Close(ctx); err != nil {
				t.Fatal("plugin leaked worker dependency", err)
			}
		})
	}
}

func TestWorkerPluginTeardownWaitsForActivityTail(t *testing.T) {
	fixture := newFixture(t, 4, func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) { peer.activityName = "plugin-tail" })
	executions, _ := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	entered, release, stopping, tornDown := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	plugin := &controlledWorkerPlugin{stop: func(ctx context.Context, options worker.PluginStopWorkerOptions, next func(context.Context, worker.PluginStopWorkerOptions)) {
		close(stopping)
		next(ctx, options)
		close(tornDown)
	}}
	body := func(context.Context) error {
		close(entered)
		<-release
		select {
		case <-tornDown:
			return errors.New("plugin dependency closed before callback exit")
		default:
			return nil
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "plugin-tail"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1, WorkerStopTimeout: time.Millisecond, Plugins: []worker.Plugin{plugin}},
		Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "plugin-tail"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); _ = managed.Stop(context.Background()) })
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("activity did not enter")
	}
	short, stopWaiting := context.WithTimeout(ctx, 20*time.Millisecond)
	err = managed.Stop(short)
	stopWaiting()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("stop claimed premature completion", err)
	}
	select {
	case <-stopping:
	case <-ctx.Done():
		t.Fatal("plugin Stop did not enter")
	}
	select {
	case <-tornDown:
		t.Fatal("plugin next returned before actual join")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tornDown:
	default:
		t.Fatal("plugin did not finish teardown")
	}
}
