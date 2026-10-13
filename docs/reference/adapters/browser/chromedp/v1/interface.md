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

# Managed chromedp interface

[Documentation](../../../../../README.md) / Public Adapter reference

**Audience:** process-local browser automation callers.
**Status:** controlled page automation and rendered-page extraction implemented.
The reviewed cleanup, event-dispatch, readiness, input and helper-custody corrections are published SDK-only.
This is not a complete CDP surface; advanced routes retain their documented
ownership requirements and runtime availability limits.
**Package:** `github.com/frost-leo/fathomry/adapters/browser/chromedp/v1`.

## Call sequence

Prepare explicit Settings and NativeOptions offline. Construct the shared
operation Runtime and evidence Inbox from the exact preparation's Policy.
Open the source, retain a BrowserContext, and obtain its Page.
Run closed Actions and drain each independent operation record.
Close the retained Context, acknowledge its released record, and close the Owner.
The source's record also needs acknowledgement. Closing a Provider never closes
the caller's Runtime or Inbox.

Page.Close releases one page's operations and children using the context's
existing cleanup reservation. Created pages close; attached/borrowed pages detach.
Closing the initial page does not cancel the BrowserContext or independent sibling
pages. Context storage/discovery and NewPage remain available, and a positively
released page slot can be reused. Native destruction of a parent can still affect
its child frames/workers. Do not synchronously close a page from its own active
handler or serializer. A timeout keeps the same cleanup entry, and retry preserves
the first cleanup failure. Page.Closed reports completion of its explicit Close.
Independent context evidence retains individual PagesClosed/PagesDetached counts
and ignored cleanup errors. Whole-context disposal is reported separately.

Run is a fresh-context convenience with a non-owning Scope callback. It retains
actual callback execution through shutdown. Using consumes an existing
Fixed/Follow Handle reference. Each retention keeps its original generation.
WithID changes correlation only, not capacity or resource ownership.

## Implemented native delegation

For page scraping, use `NavigateResponse` to inspect HTTP status, wait for the
specific rendered element or predicate, then use `Text`, `TextContent`, `OuterHTML`
or typed evaluation. `SendKeys`, `Click` and the other native input actions operate
on the same retained page. A load event does not mean asynchronous page data is
ready. `QueryNodes` and `FromNode` support subtree and same-process iframe work;
`NewPage` retains a separate page in the same context. Request headers and
user-agent overrides use the [typed command route](page-commands.md#page-request-configuration);
cookies use the retained context. The caller decides when to fall back from an
HTTP client and whether a business operation may be retried.

The local synthetic flow in
[`page_automation_test.go`](../../../../../../adapters/browser/chromedp/v1/page_automation_test.go)
exercises rendered asynchronous data, input, extraction, scrolling, iframe work,
canceled waits, error recovery and owned/borrowed cleanup. It does not qualify
external sites or supply a crawler workflow.

The [finite page-command route](page-commands.md) accepts native typed CDP data
without exposing a Target or granting authority from raw result IDs. It complements
the native high-level helpers; it does not replace selector/readiness/navigation
semantics with a protocol command.

Closed selector operations call the native selector/readiness implementation.
CSS, CSSAll, ID, Search and JSPath use the native string selector types.
Input, Poll, navigation responses, history, emulation and screenshot constructors
delegate to the selected SDK. No arbitrary native Action, QueryAfter, ByFunc,
Target, Browser, allocator or native context is returned.
SetAttributes maps and WithPollingArgs containers are borrowed through execution,
not copied at construction. Keep them immutable while the action runs. Attribute
copying and native polling argument preparation happen after resource admission.
Native readiness runs at most eight concurrent checks. All started checks join,
and cancellation cannot report unchecked nodes as ready. JSPath owns a separate
temporary group per lookup and does not release independent console references.
Unknown awaited results retain cleanup failure until session/resource teardown.

QueryNodes returns opaque node capabilities and independently retained native
metadata. ByNodes replaces lookup with native NodeIDs selection from those
capabilities, without accepting raw IDs. The string selector becomes a label.
It delegates the SDK's node-ID query/readiness helper, followed by the selected
operation's normal readiness checks. Node containers are copied after admission.
An empty selection retains native empty-result semantics, including AtLeast(0).
FromNode delegates SDK subtree/readiness and same-process iframe
behavior. Nil explicitly selects the root. A zero or foreign node cannot become
root authority. Native document invalidation and wire-order frame generations
revoke old node use; unrelated child-frame navigation does not invalidate a
different frame's nodes. Guards run before selector setup and each native command.
Search/JSPath retain the SDK's FromNode limitations; this is not a DOM-subtree
security sandbox. Node metadata is a frozen protocol snapshot, not the SDK's
mutable tree. No native Target, frame owner or selector callback is exposed.

Populate preserves native depth, piercing and PopulateWait choices. Its wait ends
on cancellation. Dump and DumpTree return bounded native tree text instead of
running a caller writer under the SDK's DOM locks. ScreenshotScale preserves the
native selector/readiness behavior. ScreenshotNodes uses managed node extents and
scale without adding readiness. MouseClickXY, MouseClickNode and KeyEventNode
retain native input options. Node-specific routes refuse foreign or stale grants.
Option containers have a 64-entry bound before copying. Native key preparation
is prepaid per dispatched packet; a suppressed shortcut character reuses that
reservation for the next packet. Partial input releases use reserved cleanup.
A queued release with a lost reply is uncertain and is not replayed.

JavascriptAttribute preserves native named-property conversion, including null
normalization and undefined errors. SetJavascriptAttribute preserves the native
string assignment semantics. Resolved receivers, inherited result references and
exceptions belong to an admitted group. Ordinary typed results release settled
groups; remote results retain Page.Object authority. Concurrent readiness does
not confuse earlier replies with a later unknown producer. Independent helper
cleanup evidence survives decoder panic and Goexit.

WithPollingInFrame delegates native Poll/PollFunction in a managed node's
enclosing frame. Nil resets an earlier choice to the native default world.
Foreign, stale and zero nodes are refused. Wire-order document retirement cancels
active frame polling without waiting for a consumer. Native DOM invalidation
also cancels it. The admitted operation joins its small invalidation watcher
before returning. Cancellation alone does not release unknown remote-result
custody, and the original SDK predicate/readiness implementation is unchanged.

Page.Frames returns a preorder native frame snapshot with document-bound grants.
CreateWorld copies native parameters before filling that frame identity. Explicit
universal-access and CSP choices retain native meanings. Worlds belong to their
frame because CDP has no independent destroy-world command. MaxEvents bounds
cumulative creation attempts on each page, including unknown canceled outcomes.
Creation racing with navigation is not atomic. A stale result is refused without
claiming that creation had no effect. OOPIF metadata does not attach another target.

Page.ExecutionContexts reads the SDK's current bounded identity table, including
initial worlds created before subscriptions exist. The snapshot contains numeric
and system-unique IDs, not inferred names, origin URLs or default-world flags.
ObjectGroup.EvaluateIn preserves typed native evaluation parameters while filling
the owned group and unique execution identity in a copy. Foreign, stale or zero
grants cannot select a default world. Objects, their properties and derived call
results retain the original execution identity and revoke with it. Group ownership
still covers native exceptions, canceled producers and unknown result cleanup.

Emulate and EmulateReset retain native device-profile behavior. Native device
data is read after admission; caller implementations must return cooperatively.
PrintToPDF accepts native finite-output PDF options, including explicit zero
margins. Option callbacks run after admission. The SDK's PDFStream option is
refused here because that convenience buffers the entire file; use Page.PDFStream
to retain the native stream and its independent cleanup responsibility.

PDFStream creates a target-origin IO handle and reads bounded chunks. Positive
EOF, cancellation, close acknowledgement and context disposal remain separate.
Context cleanup also closes ignored stream handles without new admission.
It joins admitted stream creators before taking the final cleanup snapshot.
Successful cleanup retries keep earlier native failures in independent Cleanup
evidence. A non-nil Close error therefore does not imply Closed is false.

Page.LoadResource returns native network success, status, headers and an optional
owned IO stream. It requires an observed Frame, except for the native worker form
with no frame. HTTP failure metadata is not a Go transport failure and can have
no stream. Native success without a stream identity is malformed and keeps
unknown custody charged. Metadata is copied before caller mutation.
Chrome150 buffers the complete fetch in browser memory before returning IO;
this is incremental reading, not incremental network fetching. The native
IncludeCredentials=false setting still includes same-origin credentials.

ReadAt supplies an explicit nonnegative byte offset and changes subsequent Read
position. Response-body streams reject seeking. Repeated and seeked bytes share
the cumulative MaxStreamBytes allowance. At its exact boundary, a one-byte probe
can establish empty EOF. Extra data is withheld, returns a limit error and seals
further reads. Native JSON escaping is included in the bounded reply allowance.
Canceled resource loads can continue in Chrome. Detaching one borrowed session
does not prove unknown IO destruction while another session remains. An owned
child page can be destroyed even when its enclosing context is borrowed.

Evaluate and Poll with `*runtime.RemoteObject` retain their native result shape,
including object identity, null and undefined. They also retain an independent
object-group receipt instead of releasing custody when the synchronous call
returns. `Page.Object` converts only the exact original returned pointer into an
opaque Object. A copied descriptor, substituted ObjectID, another page or a new
source generation is not a grant. Descriptor fields are caller data; do not mutate
them concurrently. Operations and cleanup use the separately frozen identity.

`EvaluateObject` returns the opaque cleanup handle directly, even when a canceled
producer leaves its result unknown. `Page.NewObjectGroup` creates explicit shared
custody for its Evaluate, Poll and PollFunction operations. Object.Call invokes
the native function once on its immutable receiver, with JSON-value arguments;
the returned Object remains in that group. DataCopy returns frozen native metadata.
Explicit RemoteObject JSON values retain empty string, array, object and null,
including nested property/IndexedDB snapshots and independent result evidence.
Absent values remain absent, so undefined and native special values are not
converted to null. The output-only marshaler keeps existing byte limits and does
not change global JSON settings or native parameter encoding.
Object.CallArguments additionally accepts explicit ValueArgument, ObjectArgument,
UndefinedArgument and native UnserializableArgument values. Object arguments can
come from another group on the same page, but not another page, document or source.
Reference checks precede value codecs and are repeated after encoding. The ordinary
command allowance is acquired before argument access or encoding, not after it.
Concurrent release or navigation can still make the native call fail.
Object.CallWithOptions accepts native CallFunctionOn parameters, including
promise, serialization, side-effect, preview and user-gesture choices. Receiver
and group identity remain managed. Leave native Arguments absent and use the
explicit Argument values instead. Native parameters are copied after admission.
Object.Properties delegates Runtime.getProperties, including native accessor,
internal, private and Symbol descriptors. It does not evaluate getters.
The exact returned native reference pointers can be resolved through Page.Object.
Property enumeration reserves the remaining object capacity before dispatch,
because CDP has no result-count limit. Oversized results stop that group rather
than discarding their cleanup responsibility. Parameters are copied and any
conflicting ObjectID is refused.
Closing an implicit root object closes its group and descendants. In an explicit
group, Object.Close releases that object and ObjectGroup.Close releases the rest.
Context/source shutdown also handles ignored groups. Copies share release state.

MaxObjectGroups (default 8) and MaxObjects (default 128) are shared across all
pages of a retained context. Group evidence and native object slots are reserved
before native setup and argument encoding. Object calls use the existing command
and result budgets. Known document changes invalidate receivers, including when
navigation occurs while a call waits for its group. A retained old generation
continues to own its original objects; replacement generations cannot adopt them.

Ordinary value-returning Evaluate, Poll and PollFunction also reserve object-group
and object custody before native setup and custom decoding. A successful value or
terminal named-group evaluation exception releases that short-lived group.
A canceled producer, or an ungrouped Poll error that can hide native references,
keeps its independent receipt until positive session/resource teardown. It can
therefore consume group capacity even when no RemoteObject was returned. Typed
native exception inspection through errors.As remains available after cleanup,
but its raw object ID is not a reusable capability. Decoder/serializer cancellation
does not release an active Go callback. Joined work on an owned resource can
transfer uncertain native custody to source cleanup, without declaring it gone;
this lets source shutdown terminate its process despite a blocked CDP transport.

Canceling an awaiting native evaluation does not prove its browser producer ended.
An early releaseObjectGroup acknowledgement cannot release objects created later.
Such unknown custody remains charged and its group stays incomplete until positive
session detach or owned page/context/process destruction. Successful Evaluate
results use a private native group; successful Poll references are released by ID
without replacing the native polling algorithm. A confirmed borrowed-page detach
releases this session's object custody, not the peer page or its other controllers.
GroupReleased records positive release separately from primary and cleanup history.
Construction, metadata copying and child receipt resolution are joined before the
group's receipt is released. Property enumeration and explicit execution-context
grants preserve this same custody, as described above.

Subscribe accepts a selected native typed event descriptor and creates an
independent bounded native queue immediately. Next reserves output evidence
before reception and native decoding. Cancellation also stops an iterator that
was constructed but never started. Events closes on early break and retains an
executing range body until it actually returns. A blocking range body therefore
prevents completed shutdown. Do not synchronously Close its own subscription from
inside a range body; cancel the constructor context or break instead.
MaxSubscriptions bounds retained queues per BrowserContext, including all pages.
An optional subscription overflow closes that subscription, not unrelated pages.
Primary records the overflow while successful producer cleanup remains separate.
Console uses the same managed subscription lifetime and delegates ordered console,
exception and browser-log conversion to the native SDK's data-only decoder.

Page.Control installs Fetch interception, dialog/file-chooser control and/or screencast after reserving a
finite control ledger and cleanup responsibility. Control.Next yields opaque
Pending replies. ContinueRequest, ContinueResponse, FailRequest, FulfillRequest,
ContinueWithAuth and HandleDialog construct closed native responses. The reply
path uses its existing reservation rather than new Inbox or ordinary action
admission, so it can unblock a parent navigation or promise at saturation.
ScreencastFrameAck binds only the frame represented by its Pending token, without
accepting a raw frame ID. Frame bytes consume the same cumulative control ledger.
Close stops the screencast producer even when no frame was consumed or acknowledged.
This is finite native frame capture, not a video encoder or an unbounded recorder.
`ControlOptions.Recording` instead owns native screen-recording IO; it is mutually
exclusive with Screencast. `StopRecording` waits for the producer's final flush,
not merely the stop request. A timeout ends only the wait; retries join the same
native attempt. `ReadRecording` and `ReadRecordingAt` are available only after a
confirmed stop and share the owned stream's cumulative byte limit. A current file
EOF before producer completion is not recording completion. Close stops an ignored
producer and closes its stream, including when another control's response-body
cleanup fails. Lost stop replies retain custody until stronger destruction
evidence; they do not authorize replay. Chrome 150 rejects these selected methods
with native -32601. Local protocol-surrogate tests qualify ownership/fault handling,
not successful video encoding on that browser.
FileChooserFiles selects explicit browser.HostPath values for an intercepted
input-element chooser. Wire-order frame navigation/detachment and a newer chooser
revoke old tokens before delivery or reply. It never accepts a bare backend-node
ID. Choosers without an input node remain observable, but DOM.setFileInputFiles
cannot fulfill those native events. No remote file-transfer service is implied.
One reply runs per lease. Preflight refusals do not consume a pending capability;
after submission an uncertain effect cannot be retried through the same token.
The lease's cumulative ledger is limited by MaxEvents and MaxResultBytes.
Its pre-reserved receipt retains event bytes, refusals and acknowledgements even
when callers ignore individual return values. It does not retain reply credentials.

Dialog CDP commands address the current dialog of a page, without an atomic dialog
identifier. Known stale observations are rejected before submission. An independent
browser controller can still replace a dialog after that check. The API does not
promise exclusive access or atomic per-dialog execution against such a peer.

Pending.ResponseBody reads one Fetch response-stage pause into a finite native
result. Its positive maxBytes argument bounds encoded protocol output before
decoding. The existing control ledger reserves twice that limit plus metadata
before dispatch, and retains a separate copy of decoded bytes. The call therefore
works at ordinary-command and Inbox saturation. It does not continue the request.
Pending.ResponseBodyStream instead returns target-origin sequential IO with its
own pre-admitted stream receipt. These two acquisition modes are mutually exclusive.
After taking a stream, read through positive EOF and explicitly fulfill or fail
the request. ContinueRequest and ContinueResponse cannot restore the original body.
Control.Close closes ignored native streams and aborts their paused requests.
The public Stream still needs Close, or enclosing Page/Context cleanup, to release
its independent receipt. Cleanup retries preserve their first error separately
from confirmed release. A canceled body producer without an observed reply keeps
cleanup incomplete until positive native resource destruction; it is not a
certificate that disabling interception is safe on a borrowed page.
The source retains unfinished body controls after their local context work exits.
Its exact prepared reservation covers one cleanup ledger per configured page in
each context. Those controls still consume their original context capacity and
prevent KeepOpen until cleanup is confirmed. An owned process can be terminated
despite a blocked CDP writer, after which ignored body streams and controls settle.
If borrowed-page cleanup retains an invocation lease on a locally owned browser,
Owner.Close can cancel that local process independently of assembly release.
The source remains incomplete until actual child/process joins. This fallback
never terminates an external browser or a process whose ownership was transferred.
This is a logical cleanup reservation, not a browser-memory measurement.

ControlOptions.Network adds request-start, response, loading-finished and
loading-failed observations to the same bounded control ledger. The SDK already
enables Network on pages; this option neither changes its buffer configuration
nor disables it on Close. Native request identities are recorded before consumer
delivery. Pending.Request returns an opaque grant only when this controller
observed the request start. A redirect that reuses its native ID revokes the
preceding grant even if its event has not yet been consumed. Raw IDs and mutated
event copies cannot confer authority. Closing the controller cancels and joins
its active request commands without occupying the reserved Fetch reply lane.

RunRequestCommand accepts exact native getResponseBody, getRequestPostData,
searchInResponseBody and replayXHR command types. It copies parameters before
filling the immutable RequestID. Body reads/search require an observed successful
loadingFinished, not a successful HTTP status. Body eviction still produces a
native error. Request post data preserves native multipart-file omissions. An
explicit replayXHR requires an observed XHR and sends another request; receipt
redelivery never repeats it. Closing observation does not cancel HTTP requests
already sent. Request/response payloads remain deliberate sensitive outputs.

BindFunc owns one native page-to-Go registration. It keeps native argument and
promise behavior. MaxBindings limits registrations and MaxBindingCalls limits
actual callback work across the retained context's pages. Each callback reserves
independent evidence before decoding its argument or creating its goroutine.
The handler receives cancellation only, without a native owning context.
The result is encoded once. BindingObservation records handler return, undefined
output and reply acknowledgement separately from callback and cleanup errors.
Page-visible failures use fixed text; deliberate errors.Is/As can inspect causes.

Cancel requests binding shutdown. Close removes its native registration and
document script, revokes captured aliases, and restores a prior public property
only while the binding still owns that property. Existing isolated worlds keep
their public properties. Do not call Close synchronously from its own handler.
An unreturned handler or codec keeps its charge and cleanup handle after timeout.
An uncertain cleanup reply does not authorize deleting a later replacement by
name. Unknown cleanup stays incomplete, and a successful retry keeps the earlier
cleanup error. Context and source shutdown also clean ignored binding handles.
These guarantees do not provide a JavaScript integrity sandbox or exclusive
access against another browser controller.

Context.Cookies, SetCookies and ClearCookies use the exact retained BrowserContext.
Scope offers the same operations during Run without gaining cleanup authority.
Native cookie and partition data remains available. Inputs are borrowed until
return and copied after admission; returned cookie data is separate from receipt
evidence. Cookies deliberately exposes credentials, unlike ordinary Result formatting.
SetCookies acknowledges the native command, not a read-back guarantee of persistence.
Borrowed contexts require ContextOptions.SharedStorage even for reads. Granting it
for an explicitly borrowed default/shared context can affect its other users.
An absent context identity never silently authorizes the default cookie store.

Page.Storage selects an explicit security origin within that page's verified
BrowserContext. RunStorageCommand delegates finite DOMStorage data, cache
discovery, IndexedDB metadata/schema/deletion, and origin usage/clear operations.
Borrowed contexts require SharedStorage. Session storage belongs to the selected
page, not all pages in a context. Native origin data can outlive a document.
Parameters are copied before filling scope; conflicting origins and ungranted
storage keys/buckets are rejected. StoragePartition instead acquires the native
storage key through an explicit Frame grant. Nil preserves native target-only
selection, which the qualified Chrome150 rejects on a page; use Page.Frames for
explicit page selection. Target-only worker acceptance is a separate native gate. A key is not
reconstructed from an origin URL. Acquisition checks document identity before
and after the native call. An acquired partition can outlive navigation, but not
its original Page/Context/source. Scope.StorageKey is deliberate sensitive data,
not a transferable grant. Bucket(name) narrows it to a native named/default
bucket without creating or owning stored data. Bucket grants cannot clear a whole
partition or select siblings. ClearDataForStorageKey and DeleteStorageBucket
remain explicit mutations, never automatic Close behavior.

StorageScope.Cache accepts only the exact
native discovery pointer and produces a frozen, context-bound StorageCache.
RunStorageCacheCommand covers finite entry reads/deletions and cache deletion.
Native cache IDs name mutable stored data, not an exclusive revision. Concurrent
browser controllers can change or recreate a cache; this is not a transaction.

ObjectGroup.ReadIndexedDB preserves native paged records, key ranges, indices,
HasMore and zero PageSize. Negative counts and over-capacity sizes are refused.
Each requested row reserves three remote-value slots before parameter encoding.
Key, primary-key and value pointers retain the group's independent custody and
can be resolved through the original Page.Object. Raw IDs or copied pointers do
not grant access. Groups release their returned references individually rather
than clearing Chromium's shared native indexeddb object group. Closing one group
therefore preserves another group's results and never deletes database records.
Canceled or oversized native results retain uncertain cleanup until positive
session/resource teardown. Pagination is not a database snapshot or transaction.

ControlOptions.ServiceWorkers explicitly enables the native observer for the
page's BrowserContext storage partition. Borrowed contexts require SharedStorage.
The same bounded control queue records registration/version lineage before
delivery, so a queued old event cannot revive a known deleted/redundant identity.
Pending.ServiceWorker(index) returns the corresponding original registration or
version grant. RunServiceWorkerCommand delegates ID-based stop/push/sync commands
using copied parameters and frozen identities. Browser-native URL schemes remain
valid observations, not incorrectly classified as malformed HTTP URLs.

RunServiceWorkerContextCommand provides explicit native first-party ScopeURL
start/skip-wait/update/unregister, partition-wide stop-all and force-update controls.
These URL commands do not claim to address an exact observed registration ID or
an arbitrary partitioned storage key. Native acknowledgement may precede effect
completion or a handler execution. Force-update changes affect the partition;
native Disable resets the setting instead of restoring another controller's
previous value. Control.Close stops observation and its own protocol domain,
without unregistering persistent workers or killing the borrowed browser.

Context.SetPermission and ResetPermissions use that same verified context identity
but a separate SharedPermissions grant for borrowed contexts. Empty native origins
mean all origins. The grant accepts context-wide changes, including native cleanup:
closing the control session resets all permission overrides in each context it
modified, including another controller's overrides. This is not per-controller
permission isolation. See the selected
[Chromium BrowserHandler implementation](https://chromium.googlesource.com/chromium/src/+/refs/tags/150.0.7871.114/content/browser/devtools/protocol/browser_handler.cc).

SetUploadFiles delegates native selector/readiness using explicit browser.HostPath
values. Remote paths belong to the browser host, not automatically the Go host.
No file-transfer service or local path resolution is added. Nil paths are invalid;
an explicit empty slice is forwarded with native semantics. Use Clear to clear a
file input. SendKeys retains its native file-input overload: its value is a
browser-host path when the selected input is a file input. These operations do
not delete caller files or prove a later form upload completed.

ControlOptions.Downloads enables native GUID-named downloads into an explicit
caller-owned directory. LocalDirectory requires the owned local process and an
existing absolute directory. BrowserDirectory is a literal browser.HostPath and
never grants Go-local reads, including a cohost remote fixture. SharedDownloads
is a separate explicit grant for borrowed contexts. The policy affects the whole
context, but begin/progress events and cancellation tokens belong to this page.
Other pages' transfers are not adopted. One controller per native context can
hold this source's download policy. Other browser controllers remain independent.

Pending.Download accepts only a native begin event. CancelDownload uses the
pre-admitted reply lane. Download.Read returns bounded chunks after an observed
native completion, with an independent receipt for each read. It uses the original
GUID filename beneath the retained directory handle, not a suggested name or event
path. Aliases share offsets. Native completion, actual file presence, read bytes,
EOF and cancellation acknowledgement are separate facts. Completed and partial
files remain caller property after Close. No download directory is removed.

Download observation reuses the bounded native control queue and finite ledger.
MaxStreamBytes bounds observed cumulative bytes and local reads, not physical
network/disk writes. Native reporting can arrive after that threshold is exceeded.
Close keeps the identity observer until native event disable is acknowledged,
cancels known unfinished transfers, joins local reads and releases the directory
handle without new Inbox admission. Lost begin identity needs positive context
destruction or owned-process exit, not page detach or a broken connection.
Source-level cleanup retains uncertain custody. An external shared context can
therefore report incomplete cleanup rather than silently losing ownership.

The current public surface does not yet expose complete typed CDP,
screen-recording/general IO, all download modes or all
diagnostic and browser-window controls. These are unfinished #139 requirements, not approved exclusions or
claims that the native capabilities are absent.

## Ownership and data

Owner.Window obtains a non-owning window grant from a Page in that owner's
positively owned local browser. Foreign sources and borrowed browser connections
cannot grant window mutation. RunWindowCommand preserves native bounds and
content-size types, copies nested parameters, and revalidates original target/window
membership before dispatch. The membership check consumes an extra owner CDP
command. A concurrent independent controller can still move the target afterward;
this is not an atomic window lock. Closing the Page/source revokes the grant.
Headless and visible-X11 native bounds were tested; this is not human visual review.

RunOwnerCommand accepts finite native Browser/SystemInfo/target observations only from
an Owner of a local browser process. A borrowed connection Owner cannot use it;
ordinary Page commands never gain this authority. The current set includes browser
version/command-line/histogram and process/GPU/feature observations. Parameters and
results require their exact native types. Returned host/process data is deliberate
sensitive output, not ordinary log text. One source-level command/output allowance
is shared by all Owner copies, separately from retained-context allowances. Source
policy reserves the work before startup/encoding, and cleanup does not consume this
ordinary allowance. This route does not expose raw Browser, Target or process handles.

ControlOptions.Profiler and Performance enable explicit target-session diagnostic
domains. RunDiagnosticCommand preserves native CPU sampling, best-effort/precise
coverage, coverage reset and metrics contracts with bounded typed inputs/outputs.
Console profile and coverage/metrics events share the finite Control.Next ledger.
Precise coverage can disable optimized execution; this is not passive telemetry.
Control.Close stops its ignored CPU/console profiles and coverage through native
Disable without new ordinary admission. It does not claim that discarded final
profiles were collected. The borrowed-peer test keeps another CDP session's
profile running after this controller closes. Browser-native profiling memory
is not bounded by these logical output/queue limits.

`ControlOptions.Debugger` requires `SharedIsolateEffects`, including on owned
pages: JavaScript-isolate interference is not BrowserContext isolation. Paused,
resumed and script events share the finite Control ledger. Resume/step replies
use its reserved lane, even while the ordinary operation that triggered a pause
is waiting or ordinary command/evidence capacity is exhausted. Wire-order resume
and newer-pause observations retire old tokens; native call-frame IDs are not
pause generations. CDP has no atomic expected-pause guard against other sessions.
`RunDiagnosticCommand` supports pause requests and finite debugger configuration;
a pause acknowledgment alone grants no frame authority. `Pending.Script` grants
source/search/breakable-location/blackbox operations through `RunScriptCommand`.
Locations and step skip lists require original observed Script grants; returned
metadata or changed raw IDs are not authority. Native enable cache-size options
are preserved; logical limits do not bound native script-cache memory.

Debugger Close queues one cleanup resume before disabling the domain and
releasing its native `backtrace` group. Disable alone does not release that group
or prove another session's paused execution resumed. A lost resume reply does not
prevent independent disable/group cleanup, but retains incomplete custody; later
Close joins the same attempt rather than issuing another resume. These cleanup
workers start before unrelated producer waits, so a lost recording stop reply
cannot starve debugger disable or reference release.

`Pending.CallFrame` selects an original paused frame. `ObjectGroup.EvaluateFrame`
uses a separately pre-created, nonbusy private group, never the shared native
`backtrace` group. `CallFrame.SetVariable` and `SetReturnValue` accept explicit
managed Arguments; raw native NewValue/ObjectID substitution is refused.
`RestartDebuggerFrame` consumes the pause's reserved reply. Resume/new-pause
observation or reply submission retires frame grants. Separately grouped results
may outlive resume and Debugger.Close. Because frame IDs do not prove execution
origin, any observed execution-context retirement conservatively revokes those
objects and their property/call descendants. That revocation does not prove
native release: an individual stale-ID Close refuses, retaining custody until
private-group cleanup or confirmed resource destruction. Ordinary object calls
revalidate after argument codecs. Busy-group refusal prevents a paused producer
from blocking its own inspection indefinitely.

Nonzero EvaluateOnCallFrame ScopeNumber is conservatively refused for observed
Chrome-family 150 peers: the qualified 150.0.7871.114 handler ignores this selected
optional field. Other versions remain unqualified, not inferred from the SDK
version. Breakpoint, async-stack, Wasm-cursor and HeapProfiler routes remain
unfinished; this diagnostic slice is not a complete B17 claim.

`Control.SetBlackboxContexts` replaces native execution-context blackboxing from
current, same-page execution grants. No arguments clears the set. It copies unique
IDs only after command/count admission; raw IDs, foreign pages and retired worlds
are refused. Native context destruction can later make a rule ineffective. The
Chrome 150 control verifies actual pause suppression and restoration after clear,
not only successful command acknowledgments.

Owner.Trace owns one browser-global producer and browser-origin incremental IO.
Only a positively owned local browser can start it. Native JSON/proto, compression
and trace configuration remain native data. TransferMode must be absent or
ReturnAsStream. The bounded browser event queue is installed at source creation.
Stop uses its pre-reserved control path, observes the native completion and does
not buffer file contents. Read returns bounded chunks with independent evidence.
Close stops an ignored producer and closes its IO without new admission.
TraceObservation keeps completion, data loss, consumed bytes, EOF and stream
cleanup separate. Logical byte limits do not bound Chromium's trace buffers or
RSS. Select native trace-buffer settings explicitly when those costs matter.

Owner.KeepOpen transfers a quiescent local process to a KeptBrowser, with its own
root reservation and lifetime. Retained contexts and producers must be closed
first. An explicitly borrowed default page can remain visible; isolated contexts
retain their normal disposal semantics. Borrowed external processes cannot be
transferred. The old source closes its connection and reports Transferred, not
ProcessExited. Canceling that old source cannot kill the transferred process.
KeptBrowser.Endpoint and ProfileDirectory deliberately disclose sensitive data.
Close the KeptBrowser before closing its shared Runtime. Its lifetime cancellation
also requests cleanup. Actual process exit precedes removal of a temporary profile
or release of a persistent-profile lock; caller directories are never deleted.
Source ShutdownComplete after a transfer is not a process-termination claim.

Context.Targets preserves native target filters and returns only the verified
context's targets. TargetRef retains immutable identity and native metadata,
including available opener/frame/worker attribution. Context.Attach rechecks
membership/type before attachment and reuses an existing attachment's state.
Newly discovered targets are borrowed, not assumed owned. Their sessions detach
on Context cleanup; disposing an owned BrowserContext still disposes its contents.
Worker attachments expose the worker's native Runtime operations, not page DOM
or navigation support. A new source/context cannot adopt an old TargetRef.

A local source owns its process and fresh temporary profile. ProfileDir instead
selects caller property. The Provider uses an exclusive lock file and never
recursively deletes that persistent directory. Environment inheritance is explicit.
A remote source owns only its connection and created children, never the peer.

ContextOptions can explicitly borrow a context or page. Native membership is
checked before attach. Borrowed pages detach; separately created pages close.
Explicit default-context borrowing does not promise exclusive cookies/storage.
Chrome can report a nonempty identity for its default context. Borrowing an existing
default-context page verifies membership using a temporary owned default-context
page, closes that probe, and then attaches. Missing creation identities never
select the default context implicitly. Remote source work cancellation preserves the
cleanup connection until borrowed controls and other children finish cleanup.
Copying handles does not multiply context, page, command or output budgets.
Public Page.Navigate uses the same admitted command and encoded-result budgets
as closed actions, including NavigateResponse. The legacy Internal Session.Navigate
counter exception is not part of this public API. Bootstrap, pre-admitted control
replies and cleanup keep separate reservations so they cannot deadlock on an
exhausted ordinary command allowance. These limits do not bound HTTP traffic or RSS.

Nil positive Settings limits use Internal defaults. Explicit zero limits are
validated as supplied. Durations are nanoseconds. TLS containers are copied;
callbacks and private keys remain cooperative, immutable borrowed dependencies.
Action option callbacks receive native data only and must return cooperatively.

Results retain immutable independent evidence. PayloadCopy and native return
values are deliberately sensitive; ordinary runtime formatting stays restricted.
Callback errors are translated on both direct returns and independent evidence.
Synchronous methods also report failures when cancellation prevents producer dispatch.
Cancellation and shutdown waits do not force arbitrary Go code to terminate.
An incomplete close leaves the same cleanup handle and accounting reachable.

## Versions and verification

The selected source tuple is chromedp v0.21.0 and cdproto v0.157.9 on Go 1.27.
The JSON bridge selects standard-library JSON types. The local maintained SDK
adds producer bounds, failed-trigger cancellation, call limits, binding joins,
TLS configuration, a configurable target cleanup timeout and managed target
dispatch lifetime. Work cancellation does not destroy the IO cleanup route.
The maintained SDK is published as
`github.com/frost-leo/fathomry/third_party/chromedp v0.0.0-20261013094802-1825b9d94794`.
Independent consumers must explicitly replace `github.com/chromedp/chromedp` with
that module; the Framework root's development replacement is not inherited.
The project dependency policy pins its verified checksum. This SDK publication
does not certify completion of the Browser Adapter's remaining capabilities.
Client.Profile reads the current source without starting a browser. It separates
declared settings from observed browser/protocol versions and leaves unavailable
facts unknown. With Follow it inspects the currently selected source, not an older
retained context. Returned option containers and Profile.Clone are detached copies.

## Tests

The category tests check shared observations, dependency boundaries and a standalone
consumer that performs no native I/O. External-package provider tests exercise
preparation, policy bounds, diagnostics, resource texts and source revocation through
exported APIs. Standalone Direct and Framework Fixed/Follow consumers explicitly
select the development SDK and verify that root replacements are not inherited.

Opt-in browser tests use synthetic pages and task-owned profiles. They cover native
helpers, independent action evidence, generation retention, PDF streams and cleanup
faults. Further controls cover independent event queues, real Fetch authentication,
dialog/promise progress with a full Inbox, producer overflow, actual iterator joins,
and cleanup that leaves a borrowed peer usable. Object controls cover immutable
provenance, pre-codec capacity, full-Inbox refusal, stale receivers, constructor and
metadata joins, and late canceled results after confirmed borrowed-session detach.
Missing opt-in is a skip, not live
qualification. These tests do not certify
the unfinished capabilities listed above or remote SDK distribution.

Tests in this package exercise offline preparation and real native selector,
input and Poll operations through public evidence. Browser tests require both
`FATHOMRY_CHROMEDP_INTEGRATION=1` and an explicit
`FATHOMRY_CHROME_EXECUTABLE`. Use only owned profiles and synthetic pages.
The existing Chrome150 results do not certify Chromium157-only capabilities.
