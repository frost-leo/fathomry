<!--
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
-->

# Selected Temporal capability coverage

[Interface](interface.md) / [Documentation](../../../../../README.md)

**Audience:** maintainers and consumers assessing the selected native surface.
**Status:** implemented source/type mapping for core v1.49.0 with maintained
compatibility repairs. Live service, distribution and cost gates remain separate;
method counts alone do not qualify semantics or Server availability.

## Thirty-one selected families

| ID | Family | Public/native route | Boundary |
| --- | --- | --- | --- |
| C01 | Acquisition / transport | Prepare, PrepareFromExisting, Open, Owner/Client/Binding | T01–T06 bounded routes; exact source/namespace and actual cleanup |
| C02 | Workflow / combined starts | Client.ExecuteWorkflow, SignalWithStartWorkflow, WithStartWorkflowOperation | Native Start options and single-use intention; ID on uncertain outcomes |
| C03 | Workflow / Update handles | WorkflowRun and WorkflowUpdate | Identity getters local; retained source; admitted Get |
| C04 | Signal / Query / Update | Client.SignalWorkflow, QueryWorkflow[WithOptions], QueryWorkflowValue[WithOptions], UpdateWorkflow[WithStart] | Native stages/rejection; eager conveniences and retained presence/repeated decoding/raw payload observations |
| C05 | History / visibility / descriptions | history.go; Workflow/Activity/Nexus descriptions | Owned pages/visitors; immutable metadata snapshots; guarded lazy getters |
| C06 | Cancel / terminate / reset / options | workflow.go control methods | Reset ID explicit; reset/options exact operational grants |
| C07 | Schedules | Schedule; Create/Get/WalkSchedules | Complete native handle/options; no CAS claim |
| C08 | Namespace / operator administration | WorkflowService / OperatorService | Exact grants; generated unary routes, no raw connection |
| C09 | Generated extensions | same generated service views | 135 methods share mapped admission/namespace/bounds; T05 safe option profile |
| W01 | Worker registration / lifecycle | WorkerSpec and StartWorker/Worker.Stop/Status | Dynamic/native registrations/plugins preserved; separate use and actual join |
| W02 | Tuners / pollers / eager / heartbeat | WorkerSpec.Options (native worker.Options) | All selected native fields retained with mandatory bounded ownership; no global quota |
| W03 | Current Worker Deployments | WorkerDeployment + Delete/WalkWorkerDeployments | All native handle operations; exact grants; experimental profile |
| W04 | Legacy versioning / deployments | DeploymentClient and five compatibility methods | Complete deprecated family; exact grants and native conversion |
| D01 | Deterministic primitives | Selected native workflow package | Not copied into process admission; main-only LocalVar excluded |
| D02 | Markers / versioning / replay | Selected native workflow/worker APIs | Native determinism/replay; no process admission from Workflow decisions |
| D03 | Children / external interaction / Continue-As-New | Selected native workflow APIs | Native options/helpers; no unselected main features |
| D04 | Native message handlers | Native Workflow definitions registered by WorkerSpec | Native init/validator/update handler policies retained |
| D05 | Memo / search attributes / metadata | Native options plus native workflow helpers and detached metadata | Native index/typed semantics; payload visibility explicit |
| D06 | Workflow Sessions | Selected native workflow Session APIs | Native affinity facility, not retained Client use; versioned restriction remains |
| A01 | Activities / Local Activities / heartbeat / completion | Worker registrations, native scheduling and Client completion/heartbeat methods | Token vs Workflow IDs vs standalone IDs; native async return ends callback authority |
| A02 | Standalone Activities | ActivityRun, ActivityDescription, Walk/CountActivities | Native stable execution plus explicitly granted experimental operations |
| N01 | Workflow Nexus / backed helpers | WorkerSpec.NexusServices plus native temporalnexus/workflow APIs | Native services/links/helpers/cancellation; scoped callback Client |
| N02 | Standalone Nexus | Execute/Get/Walk/CountNexusOperations, NexusRun/Description/Cancellation | Experimental/gated service support; no server feature enabled |
| X01 | Payload / failure conversion / codecs | NativeOptions.DataConverter/FailureConverter and native converter package | Borrowed context-compatible extensions; exact native errors and scoped details |
| X02 | External payload storage | NativeOptions.ExternalStorage | Explicit borrowed driver/selector; raw references and delayed retrieval remain distinct |
| X03 | Native errors / retry | Error, NativeError, independent Result.NativeError; Internal exact captures | Safe presentation separate from exact native semantic path |
| X04 | Plugins / interceptors / propagation | NativeOptions + WorkerSpec.Options | Native client/plugin factories and combined Worker hooks; scoped callback client satisfies 58 methods |
| X05 | Observers / tracing / environment | NativeOptions.Logger/MetricsHandler/Interceptors/Plugins/ReportWorkerEnvironment | Full native injection; no concrete bridge/global fallback/automatic observer sanitization |
| X06 | Testsuite / mocks / replay / workflowcheck | Selected native verification packages; optional workflowcheck below | Test tooling is not service qualification |
| X07 | Globals / unstable internalbindings | Explicitly outside ordinary owning source capability | No global resolver/SDK runtime mutation or raw unstable factory escape |
| X08 | Independent contrib modules | Per-module disposition below | No modules silently installed or claimed qualified |

## Every selected native Client method

Names below identify semantic counterparts, not a requirement to expose an owning
universal native Client. Native convenience calls remain inside their own
validation/defaulting/conversion path. Stable/deprecated/experimental status is
preserved; server availability is not inferred.

| Native Client method | Current counterpart |
| --- | --- |
| `CancelWorkflow` | Client.CancelWorkflow |
| `CancelWorkflowWithOptions` | Client.CancelWorkflow |
| `CheckHealth` | Client.CheckHealth |
| `Close` | Owner.Close / Client.Close revoke their documented physical/use scopes; callback Close never owns the source |
| `CompleteActivity` | Client.CompleteActivity |
| `CompleteActivityByActivityID` | Client.CompleteActivityByActivityID |
| `CompleteActivityByActivityIDWithOptions` | Client.CompleteActivityByActivityID |
| `CompleteActivityByID` | Client.CompleteActivityByID |
| `CompleteActivityByIDWithOptions` | Client.CompleteActivityByID |
| `CompleteActivityWithOptions` | Client.CompleteActivity |
| `CountActivities` | Client.CountActivities |
| `CountNexusOperations` | Client.CountNexusOperations |
| `CountWorkflow` | Client.CountWorkflow |
| `DeploymentClient` | Client.DeploymentClient → Describe, GetReachability, GetCurrent, SetCurrent, Walk |
| `DescribeTaskQueue` | Client.DescribeTaskQueue |
| `DescribeTaskQueueEnhanced` | Client.DescribeTaskQueueEnhanced |
| `DescribeWorkflow` | Client.DescribeWorkflow |
| `DescribeWorkflowExecution` | Client.DescribeWorkflowExecution |
| `ExecuteActivity` | Client.ExecuteActivity |
| `ExecuteWorkflow` | Client.ExecuteWorkflow |
| `GetActivityHandle` | Client.GetActivityHandle |
| `GetNexusOperationHandle` | Client.GetNexusOperationHandle |
| `GetSearchAttributes` | Client.GetSearchAttributes |
| `GetWorkerBuildIdCompatibility` | Client.GetWorkerBuildIdCompatibility → native converted sets; exact grant |
| `GetWorkerTaskReachability` | Client.GetWorkerTaskReachability → native converted reachability; exact grant |
| `GetWorkerVersioningRules` | Client.GetWorkerVersioningRules → native rule response; exact grant |
| `GetWorkflow` | Client.GetWorkflow |
| `GetWorkflowHistory` | Client.WalkHistory → bounded owned iterator/visitor |
| `GetWorkflowUpdateHandle` | Client.GetWorkflowUpdateHandle |
| `ListActivities` | Client.WalkActivities → native lazy sequence consumed under one operation |
| `ListArchivedWorkflow` | Client.ListArchivedWorkflow |
| `ListClosedWorkflow` | Client.ListClosedWorkflow |
| `ListNexusOperations` | Client.WalkNexusOperations → native lazy sequence consumed under one operation |
| `ListOpenWorkflow` | Client.ListOpenWorkflow |
| `ListWorkflow` | Client.ListWorkflow |
| `NewNexusClient` | Client.ExecuteNexusOperation accepts native NexusClientOptions per operation; retained NexusRun |
| `NewWithStartWorkflowOperation` | Client.NewWithStartWorkflowOperation |
| `OperatorService` | Client.OperatorService → exact-grant generated direct view; scoped native callback view |
| `QueryWorkflow` | Client.QueryWorkflow (eager) or QueryWorkflowValue (retained) |
| `QueryWorkflowWithOptions` | Client.QueryWorkflowWithOptions (eager) or QueryWorkflowValueWithOptions (retained, native rejection separate) |
| `RecordActivityHeartbeat` | Client.RecordActivityHeartbeat |
| `RecordActivityHeartbeatByID` | Client.RecordActivityHeartbeatByID |
| `RecordActivityHeartbeatByIDWithOptions` | Client.RecordActivityHeartbeatByID |
| `RecordActivityHeartbeatWithOptions` | Client.RecordActivityHeartbeat |
| `ResetWorkflowExecution` | Client.ResetWorkflowExecution → explicit RequestID + native response; exact grant |
| `ScanWorkflow` | Client.ScanWorkflow → cloned native request/page; deprecated |
| `ScheduleClient` | Client.CreateSchedule/GetSchedule/WalkSchedules and complete Schedule handle |
| `SignalWithStartWorkflow` | Client.SignalWithStartWorkflow |
| `SignalWorkflow` | Client.SignalWorkflow |
| `TerminateWorkflow` | Client.TerminateWorkflow |
| `TerminateWorkflowWithOptions` | Client.TerminateWorkflow |
| `UpdateWithStartWorkflow` | Client.UpdateWithStartWorkflow |
| `UpdateWorkerBuildIdCompatibility` | Client.UpdateWorkerBuildIdCompatibility → native validation/operation; exact grant |
| `UpdateWorkerVersioningRules` | Client.UpdateWorkerVersioningRules → native validation/rules; exact grant |
| `UpdateWorkflow` | Client.UpdateWorkflow |
| `UpdateWorkflowExecutionOptions` | Client.UpdateWorkflowExecutionOptions → native request/result; exact grant |
| `WorkerDeploymentClient` | Client.GetWorkerDeployment/DeleteWorkerDeployment/WalkWorkerDeployments and complete WorkerDeployment handle |
| `WorkflowService` | Client.WorkflowService → exact-grant generated direct view; scoped native callback view |

## Native option/return fidelity

- All 21 native ClientOptions fields remain represented. HostPort, Namespace and
  Identity are selected data; Credentials are explicit runtime or static APIKey.
  Logger, MetricsHandler, DataConverter, FailureConverter, ContextPropagators,
  ConnectionOptions, HeadersProvider, TrafficController, Interceptors, Plugins,
  DisableErrorCodeMetricTags, WorkerHeartbeatInterval, SdkName, SdkVersion,
  ExternalStorage and PayloadLimits are NativeOptions. DisableWorkerEnvironmentInfo
  maps inversely to explicit ReportWorkerEnvironment. No logger/exporter is installed.
- WorkerSpec.Options is the actual selected native worker.Options, not a mirrored
  reduced struct. Workflows/Activities/dynamic registration/NexusServices are
  explicitly forwarded. Internal mandatory lifecycle/admission and validation
  remain in force; plugin/observer/tuner objects are borrowed through actual join.
- Structured connection fields preserve TLS/mTLS, native plaintext-only Authority,
  keepalive, capability timeout, compression and bounded MaxPayloadSize. Opaque
  DialOptions are refused; resolver/dialer/UserAgent have explicit finite routes.
  Copied containers versus borrowed certificate material/callbacks remain distinct.
- Workflow metadata includes all native common fields (Memo, typed attributes,
  parent/root execution, timing/history/status). Activity metadata includes all
  promoted execution info and native description fields, including RawResponse,
  retry/deployment/priority/cancel fields. Nexus metadata includes promoted info,
  RawInfo/failure/token/identity/times and every cancellation data field.
  The three description families expose their native lazy getters only through
  retained admitted methods; presence/identity remain local observations.
- Generated WorkflowService/OperatorService use the same exact-grant connection
  boundary. The representative Workflow tests exercise direct and callback
  Header/Trailer/Peer/FailFast/StaticMethod/bounded message options, tighter-limit
  preservation, refusal before transmission, expiry and output lifetime. Shared
  dispatch/type coverage is not a separate service test for each of 135 methods.

## All nineteen independently versioned contrib modules

These versions and released module manifests were rechecked against the official
Go module proxy on 2026-10-10. None is added as a product dependency by this Adapter. A compatible borrowed
extension route is not cloud/backend/exporter or deployment qualification.

| Module suffix (`go.temporal.io/sdk/contrib/`) | Observed release | Disposition |
| --- | --- | --- |
| `aws/lambdaworker` | `v0.1.1` | Unselected deployment lifecycle; its own dial/start/stop cannot bypass managed source/Worker ownership. |
| `aws/lambdaworker/otel` | `v0.1.1` | Unselected Lambda telemetry lifecycle; exporter/provider flush/join is deployment-owned. |
| `aws/s3driver/awssdkv2` | `v0.3.0` | Optional borrowed StorageDriver implementation; backend credentials, retention and service qualification remain external. |
| `aws/s3driver` | `v0.3.0` | Optional S3 driver abstraction; no AWS infrastructure or credential discovery is selected. |
| `datadog` | `v0.6.0` | Optional native tracing interceptor path; no exporter/backend installed or qualified. |
| `envconfig` | `v1.0.2` | Optional explicit caller-side preparation only; Adapter never discovers files/environment implicitly. |
| `gcp/cloudrun/id` | `v0.1.0` | Unselected experimental metadata identity plugin. Source identity is explicit and fenced; qualify explicit acquisition separately. |
| `gcp/cloudrun/otel` | `v0.1.0` | Unselected Cloud Run telemetry/deployment plugin; no ambient discovery or exporter lifecycle implied. |
| `gcp/gcsdriver/gcssdk` | `v0.2.0` | Optional borrowed StorageDriver implementation; cloud backend/credentials unqualified. |
| `gcp/gcsdriver` | `v0.2.0` | Optional GCS driver abstraction; no backend installed or selected. |
| `googleadk` | `v0.3.0` | Optional AI integration; no ADK runtime, remote provider or application policy selected. |
| `opentelemetry` | `v0.8.1` | Optional native interceptor/MetricsHandler injection; caller owns provider/exporter and dependencies. |
| `opentelemetry/otlpworker` | `v0.1.0` | Unselected opt-in OTLP Worker telemetry lifecycle; no implicit collector/exporter. |
| `opentelemetry-v2` | `v0.1.0` | Optional experimental plugin/replay-safe native provider path; not v1 span-compatible; no main-only fix claimed. |
| `opentracing` | `v0.3.0` | Optional native tracing interceptor; caller-owned tracer/exporter lifecycle. |
| `sysinfo` | `v0.1.1` | Optional native environment reporting/plugin; default remains disabled and explicit identity is fenced. |
| `tally` | `v0.2.0` | Optional native MetricsHandler implementation; caller owns tags/cardinality/reporting/backend lifecycle. |
| `tools/workflowcheck` | `v0.6.0` | Optional developer analysis tool, not a process source or runtime dependency. |
| `workflowstreams` | `v0.1.1` | Optional deterministic/client protocol. No application stream runtime selected or server qualification implied. |

Source authority: the selected [SDK replacement](../../../../../../third_party/temporal-sdk/FATHOMRY.md),
the [v1.49.0 release](https://github.com/temporalio/sdk-go/releases/tag/v1.49.0),
and independently versioned contrib manifests. This is source/type accounting,
not independent human approval or backend/deployment qualification.

## Exact selected option-field register

This register is source-derived from the selected SDK declarations and current
Internal copy/validation/finalize code, not an independent copy of numeric native
defaults. Public NativeOptions / WorkerSpec.Options preserve those native types.
Containers versus referenced runtime objects have different ownership.

### ClientOptions (21 fields)

| Native field | Native type | Selected disposition |
| --- | --- | --- |
| `HostPort` | `string` | Settings.Endpoint; explicit frozen target. Native bare-target canonicalization cannot change selected source identity. |
| `Namespace` | `string` | Settings.Namespace; explicit frozen namespace, validated at transport/native operations. |
| `Credentials` | `Credentials` | NativeOptions.Credentials borrowed sealed native object/callback, or Settings.APIKey converted into native static credentials; conflict refused. |
| `Logger` | `log.Logger` | Borrowed native observer, typed nil refused; nil disables logging, no global fallback. |
| `MetricsHandler` | `metrics.Handler` | Borrowed native observer, typed nil refused; nil keeps native no-op; native root tags are retained. |
| `Identity` | `string` | Settings.Identity; explicit/defaulted immutable source identity; plugin rewrites fenced. |
| `DataConverter` | `converter.DataConverter` | Borrowed native converter/codecs, typed nil refused; native context/replay requirements remain. |
| `FailureConverter` | `converter.FailureConverter` | Borrowed native converter, typed nil refused; native wire/error semantics retained. |
| `ContextPropagators` | `[]ContextPropagator` | Slice copied; native propagator objects borrowed, typed nil refused; Workflow methods stay deterministic. |
| `ConnectionOptions` | `ConnectionOptions` | Native value/TLS option slice and map containers copied, including certificate container slots; byte/key/trust material and callbacks borrowed under the interface's explicit copy-depth contract. Structured fields retained, opaque DialOptions refused. Final post-credential TLS guarded. |
| `HeadersProvider` | `HeadersProvider` | Borrowed native callback; header namespace and byte/cardinality bounds applied. |
| `TrafficController` | `TrafficController` | Borrowed native test-oriented per-attempt callback; request namespace/message bounds rechecked. |
| `Interceptors` | `[]ClientInterceptor` | Slice copied; native objects/factories borrowed, typed nil refused; controlled client/combined Worker scopes. |
| `DisableErrorCodeMetricTags` | `bool` | NativeOptions.DisableErrorCodeMetricTags copied scalar; native meaning/defaulting retained and applicable Internal bounds validated. |
| `Plugins` | `[]ClientPlugin` | Slice copied; native objects borrowed; configuration/continuations scoped; mandatory owner hooks cannot be replaced. |
| `WorkerHeartbeatInterval` | `time.Duration` | NativeOptions.WorkerHeartbeatInterval copied scalar; native meaning/defaulting retained and applicable Internal bounds validated. |
| `SdkName` | `string` | NativeOptions.SdkName copied scalar; native meaning/defaulting retained and applicable Internal bounds validated. |
| `SdkVersion` | `string` | NativeOptions.SdkVersion copied scalar; native meaning/defaulting retained and applicable Internal bounds validated. |
| `DisableWorkerEnvironmentInfo` | `bool` | Inverse of explicit NativeOptions.ReportWorkerEnvironment; no ambient environment discovery by default. |
| `ExternalStorage` | `converter.ExternalStorage` | Native struct copied, Drivers slice copied; driver/selector objects borrowed, typed nil refused; backend lifetime/allocations caller-owned. |
| `PayloadLimits` | `PayloadLimitOptions` | Native value struct copied; warning bounds validated; no claim that payload references bound external storage allocations. |

### WorkerOptions (41 upstream fields plus one reserved local control)

| Native field | Native type | Selected disposition |
| --- | --- | --- |
| `FathomryLifecycleV1` | `bool` | Reserved local ownership control: mandatory true in managed finalize; caller/plugin cannot disable it. |
| `MaxConcurrentActivityExecutionSize` | `int` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `WorkerActivitiesPerSecond` | `float64` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `MaxConcurrentLocalActivityExecutionSize` | `int` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `WorkerLocalActivitiesPerSecond` | `float64` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `TaskQueueActivitiesPerSecond` | `float64` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `MaxConcurrentActivityTaskPollers` | `int` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `MaxConcurrentWorkflowTaskExecutionSize` | `int` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `MaxConcurrentWorkflowTaskPollers` | `int` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `MaxConcurrentNexusTaskExecutionSize` | `int` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `MaxConcurrentNexusTaskPollers` | `int` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `EnableLoggingInReplay` | `bool` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `StickyScheduleToStartTimeout` | `time.Duration` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `BackgroundActivityContext` | `context.Context` | Borrowed runtime context; merged with independently owned Worker lifetime and released after actual join. |
| `WorkflowPanicPolicy` | `WorkflowPanicPolicy` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `WorkerStopTimeout` | `time.Duration` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `EnableSessionWorker` | `bool` | Native scalar copied; combination with deployment/build-ID versioning explicitly refused. |
| `MaxConcurrentSessionExecutionSize` | `int` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `DisableWorkflowWorker` | `bool` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `LocalActivityWorkerOnly` | `bool` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `Identity` | `string` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `DeadlockDetectionTimeout` | `time.Duration` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `MaxHeartbeatThrottleInterval` | `time.Duration` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `DefaultHeartbeatThrottleInterval` | `time.Duration` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `Interceptors` | `[]WorkerInterceptor` | Slice container copied; interceptor/factory objects borrowed, typed nil refused; mandatory task guard remains outermost. |
| `OnFatalError` | `func(error)` | Borrowed callback; mandatory wrapper preserves first native fatal cause, requests stop and retains callback through actual completion. |
| `DisableEagerActivities` | `bool` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `MaxEagerActivityReservationsPerWorkflowTask` | `*int` | Optional integer pointer copied by value into a new pointer; native semantics/defaults retained. |
| `MaxConcurrentEagerActivityExecutionSize` | `int` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `DisableRegistrationAliasing` | `bool` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `BuildID` | `string` | Native copied legacy scalar; deprecated build-ID routing remains classified, not service-certified. |
| `UseBuildIDForVersioning` | `bool` | Native copied legacy scalar; native deprecation and incompatible Sessions restriction retained. |
| `DeploymentOptions` | `WorkerDeploymentOptions` | Native value struct copied; versioning/session incompatibility rejected; routing/data meaning stays native. |
| `PreferredVersionProvider` | `PreferredVersionProvider` | Borrowed native callback/provider; unchanged native versioning contract, not an Adapter deployment policy. |
| `Tuner` | `WorkerTuner` | Borrowed native tuner/slot providers; validated typed nil, actual Worker lifetime; declared work envelope is not tuner heap/RSS. |
| `SysInfoProvider` | `SysInfoProvider` | Borrowed native provider; no implicit process-global replacement or metrics backend selected. |
| `WorkflowTaskPollerBehavior` | `PollerBehavior` | Borrowed native poller behavior object; native fixed/autoscaling semantics retained; typed nil refused. |
| `ActivityTaskPollerBehavior` | `PollerBehavior` | Borrowed native poller behavior object; native fixed/autoscaling semantics retained; typed nil refused. |
| `NexusTaskPollerBehavior` | `PollerBehavior` | Borrowed native poller behavior object; native fixed/autoscaling semantics retained; typed nil refused. |
| `Plugins` | `[]WorkerPlugin` | Slice container copied; plugin objects borrowed, typed nil refused; native Configure/Start/Stop continuations scoped and owned. |
| `MaxConcurrentWorkflowTaskExternalStorageVisits` | `int` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |
| `DisablePayloadErrorLimit` | `bool` | Native value copied unchanged; SDK remains authoritative for defaults/validation and technical semantics. |

`WorkerSpec.Bytes` / public WorkerWorkBytes declare the native work envelope;
advanced tuners, pollers, native caches and arbitrary callback allocations are not
hard RSS bounds. Interceptor/plugin containers and optional eager integer are
explicitly copied by `copyWorkerOptions`; the remaining non-scalar providers and
contexts are borrowed through actual Worker shutdown/join, not merely Stop return.
