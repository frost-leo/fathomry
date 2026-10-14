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

# chromedp v0 interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** Internal composition and Browser capability maintainers.
**Status:** managed page automation with finite advanced routes. Not a complete CDP surface.
**Package:** `github.com/frost-leo/fathomry/internal/browser/chromedp/v0`.

## Preparation and ownership

PrepareV1 freezes exact effective data and native TLS containers offline. Budget
and Limits describe that same preparation. Select requires an explicit cancellable
source lifetime. Assemble owns the source; Bind attaches an independent Inbox.
Go zero limits default; explicit layer zeros are validated as supplied.

An absolute ExecPath starts an owned process and a loopback WebSocket connection.
RemoteURL names an exact borrowed browser endpoint. The source never signals that
peer or sends Browser.close. Headers and TLS apply to CDP control, not browser HTTP.
The source uses the selected core module's remote package, not the obsolete
standalone remote module.

Fresh profiles are removed only after owned process exit. ProfileDir instead names
caller property. An exclusive `.fathomry-browser.lock` protects concurrent
Fathomry ownership; Chrome retains its own native lock. Cleanup checks the lock's
file identity before removal and never recursively deletes a persistent directory.
Environment inheritance requires InheritEnvironment. Flags cannot disable the sandbox.

Startup remains lazy unless CheckReady is explicit. Readiness observes a CDP
round trip, not HTTP success or page isolation. Failed startup is retained and is
not automatically retried.

## Contexts, pages and calls

Retain admits one BrowserContext before native setup. Context.Page returns its
initial page and NewPage creates another page in the same context. Copies share
the same context/page/command/output accounting. Run uses Retain for its fresh
callback convenience and preserves the separate cleanup context.

SessionOptionsV1 can explicitly borrow an existing context or page. Native context
membership and target membership are checked before attach. A borrowed page is
detached, not closed. Separately created pages are owned and closed. Explicit
default-context borrowing does not promise exclusive cookies or storage.

Closed generic Actions delegate to native selector/readiness, input, JavaScript,
Poll, navigation, capture and emulation helpers. RunAction returns native data
without giving caller code a Target. Query options are closed; raw QueryAfter,
ByFunc, After and owning native contexts are not public extension points.
Native data-only keyboard/mouse/emulation option callbacks remain cooperative.

RunResponse delegates native ordered navigation correlation while reserving the
navigation lane. Its callback receives cancellation and a private response lease,
not a native context. ResponseStep allows a closed navigation action to use the
ordinary lane only during that lease. Stale contexts, copied steps after expiry,
and foreign sessions do not grant navigation reentry. The public bridge runs each
step through normal public admission and remote-value custody. Control replies
keep their separate reserved lane.

Evaluate/Poll remote-object actions reserve one object slot before native setup
or argument encoding. Session.NewObjectGroup reserves bounded custody; the action
route creates an implicit group when no explicit group is supplied. Session.Object
accepts only the exact original pointer returned on that page, with its unmodified
native identity. Copies of metadata and IDs from another page/source are not grants.
Object.Call delegates once with a frozen receiver ID and JSON-value arguments.
DataCopy retains immutable native metadata. Known main-document changes revoke
receivers; the epoch is rechecked after waiting for the group's serialized work.
Output snapshots preserve explicit RemoteObject JSON values, including empty
array/object/string and null. WriteNativeResult applies that correction recursively
inside bounded retained output, not in parameter codecs or global JSON settings.
Properties and IndexedDB references share the same immutable snapshot boundary.

Evaluate uses a private native group. Poll keeps the native algorithm and releases
its successful result by ID. Unobserved/canceled producers retain unknown custody:
an early releaseObjectGroup reply does not prove a later result cannot be retained.
The group remains incomplete until positive session detach or owned resource
destruction. Borrowed-page detach ends only this session's object custody, not the
peer target. Object/group Close bypasses fresh ordinary admission, uses the cleanup
budget, and keeps prior native cleanup failure separately from positive closure.
Context shutdown joins admitted object work before cleanup and retries after
detaching its native sessions. No SDK producer runtime was added for this route.
Object-valued arguments and native property descriptors preserve opaque references,
including accessor/private/Symbol values. Ordinary Evaluate/Poll values also
retain exception/late-result custody. Only joined work on an owned resource can
transfer unresolved native custody into the existing source record. Actual source
cleanup settles those objects after positive resource destruction, so held object
evidence cannot prevent termination of an owned process behind blocked transport.
Explicit execution-context grants use the same custody, as described below.

StorageScope binds an explicit origin to the verified context and selected page.
Finite native DOMStorage/cache/database metadata routes copy parameters and enforce
the existing SharedStorage grant. Cache capabilities derive from exact returned
pointers, never a caller's string ID. Native data deletion is explicit, not Close.
StoragePartition obtains a native key through a checked frame/target query;
Bucket narrows that acquired partition without creating or owning stored data.
Native nil-frame rejection remains visible instead of silently guessing a root.
ObjectGroup.ReadIndexedDB reserves three remote slots per requested row and
retains the exact returned key/primary-key/value objects. Group cleanup releases
individual objects, not Chromium's shared indexeddb group. Unknown producers
remain charged until positive session/resource teardown.

ServiceWorkers is an explicit Control option with context-wide SharedStorage
requirements on borrowed resources. Its native-order bounded lineage observer
creates registration/version grants; raw IDs never grant stop/push/sync authority.
First-party ScopeURL and partition-wide native controls use the separately named
context-command route. Native Disable clears force-update state and stops the
observer, not persistent registrations. Native acknowledgements do not prove
asynchronous effects or user-handler execution. Browser-native schemes are valid
observations, while credentials and malformed scopes remain refused.

Trace owns one browser-global producer and browser-origin IO, separate from a
target-origin PDF stream. Both reuse bounded incremental reads and actual cleanup
joins. Stop/Close use pre-reserved cleanup controls. Borrowed browsers cannot start
global tracing. File-chooser input replies reuse the bounded Control ledger and
wire-order document revocation; no raw backend-node identity is authorized.

KeepOpen transfers only a quiescent positively owned local process. KeptBrowser
then owns process exit and profile cleanup, while the old source releases only its
connection. A mutex arbitrates transfer versus source-lifetime process termination.
Context reservation uses that same transfer decision, so a concurrent Retain cannot
create resources after the process changes owners.
Temporary profiles remain with the new owner until actual exit. Persistent caller
directories are preserved and their lock remains held until then.

AcquireWindow binds a native window to an exact target of the same owned process.
RunWindowCommand copies native bounds/content-size data, then rechecks the target's
window before dispatch. The extra membership query consumes an owner command and
does not provide atomicity against another controller moving the target.

Profiler and Performance Control options own their target-session domains.
RunDiagnosticCommand uses ordinary admission; Close uses pre-reserved cleanup
to disable ignored CPU/console profiles, coverage and metrics. Another native
CDP session's profiler remains independent. Native profiling memory and the
cost of disabling optimization are outside logical output/queue guarantees.

Recording Control reserves both stream custody and bounded lifecycle evidence
before native setup. Stop waits for the single owned native flush call; canceled
waits do not replay it. Reads/seeks require confirmed producer completion. Close
starts stop before unrelated cleanup and joins/closes the same private stream.
The Chrome 150 peer reports recording unavailable (-32601); protocol-surrogate
tests are not video-encoding qualification.

Debugger Control requires shared-isolate consent. Its reserved pause replies
and bounded finite/script commands can progress while Session's ordinary action
is paused. Script and pause authority follows wire-order observations, not raw
IDs or script execution-context metadata. Cleanup queues one resume, disables
Debugger, then releases `backtrace`; uncertain resume retains custody without
blocking independent domain/group cleanup. This is the implemented lifecycle and
script/frame slice, not complete Debugger/HeapProfiler coverage. Frame evaluation
reuses a pre-created private ObjectGroup with nonblocking gate admission. Its
page-owned execution-retirement fence is captured before dispatch and inherited
by derived references; retirement revokes use without refunding native custody.
Private-group release remains required and is independent of Debugger.Close.
Control's native observer reserves shared event count/bytes, including setup and
queued events, before identity decoding; the pump consumes that same reservation.

Native helper cleanup failures are separately retained in operation/context
evidence. The SDK uses a bounded independent cleanup context for node/search/JSPath/drag
cleanup and does not retry those failures as selector readiness. Ordinary native
error inspection remains available. A missing-context release response is treated
as completed node cleanup only with a subsequently observed all-contexts-cleared
generation, not from cancellation or error text alone.
JSPath releases its own per-lookup group, including property references, without
releasing the shared console group. A queued evaluation with no observed reply
retains unknown cleanup even after group release. Readiness uses at most eight
workers and joins started checks; cancellation cannot certify unchecked nodes.
Native state-queue overflow stops the target visibly rather than blocking response
dispatch. Late events for stopped targets do not block browser or sibling calls.

Protocol provides a synchronous, revocable cdp.Session for the same finite page
commands as PageCommand, plus inline-only PDF. Raw subscriptions remain refused.
Command uses that same boundary. Neither route grants authority from raw IDs or
admits object/stream producers, storage mutation, interception or domain lifecycle.
Use the existing scoped Object, Control, Request, Storage and Context operations
for those capabilities. Arbitrary native Action[T] is not accepted.
The static 627-command vocabulary and parameter types are pinned to cdproto
v0.157.9; catalogue membership is not execution authority.
PDFStream is a separate owned, target-scoped IO route. General IO,
complete origin-grant, owner-control and remaining-domain
routes are unfinished #139 work, not claims of native absence.

One navigation may overlap one other action and event reception. This allows
Fetch replies while navigation waits. Same-kind overlap is refused. Context copies
and detached Go contexts cannot renew a closed grant. The current event bridge
owns one bounded native queue and one bounded observation queue per page.
The bridge includes all native target event methods. Unknown protocol methods are
ignored for version compatibility; malformed known events still fail the session.

The Control response-body route retains response-stage Fetch identities. Finite
reads use pre-reserved ledger capacity and retain copied bytes. Incremental reads
reuse Stream ownership and cannot authorize an original-request continuation.
Unknown canceled producers remain incomplete until positive resource teardown.
Native stream cleanup and public receipt completion are separate responsibilities.
Source-owned cleanupControls retain unfinished body/download controllers after
context work exits. Preparation reserves their per-page ledgers; multiple controls
from one retired context consume one context slot, not independent aliases.
Positive process/resource teardown closes ignored streams without waiting for a
responsive protocol writer. KeepOpen refuses this unfinished custody.
ControlOptions.Network records bounded request origins in native event order.
Request grants cannot follow a redirect's reused ID or another target/source.
RunRequestCommand delegates finite body/post-data/search and explicit XHR replay
with copied, typed parameters. Its work joins independently of the control-reply
lane. Closing observation preserves the SDK's Network domain and already-sent HTTP
effects. See the [public contract](../../../../adapters/browser/chromedp/v1/interface.md).

## Shutdown and independent evidence

Close seals page work, cancels it, and waits for actual execution and binding exit.
The retained context lifetime and context-command lane are separate from its
initial page. ClosePage and Context.Close share per-session native cancellation
and actual-join state. Individual cleanup filters that page's children, validates
positive owned-target disappearance or borrowed detach, then refunds its page
slot. Borrowed child targets detach before whole-context cleanup closes their
owned parent pages. Public metadata/serializer joins remain independent.
Page creation records ownership before native I/O and is joined without holding
the context state mutex. Stream cleanup uses the separate cleanup budget, and
context release joins in-flight stream work before returning its reservation.
A canceled wait on a retained Context keeps its cleanup handle and accounting.
Run's synchronous callback cannot be forcibly terminated. Goexit and panic retain
failure evidence instead of silently releasing successful work.

Native target cleanup uses the configured cleanup budget. Owned process termination
does not depend on a responsive CDP writer. The blocked-write regression still
requires the owned process to be killed and joined. A borrowed peer is never killed.

Known failed context disposal is transferred to the same source owner and remains
charged against its native context ceiling. Source cleanup can open a cleanup-only
connection to reconcile its known IDs. A lost create response with no ID remains
unknown; cleanup never guesses from a before/after context list.

Result retains primary and cleanup facts separately. ContextReleased means
positive owned-context disposal. ContextDetached describes released borrowed use,
not disposal. CallbackCompleted only means the callback returned nil before its
deadline. A handled operation error still remains in the required result.
Stream cleanup retains its first native failure across successful retries.
Closed and ContextDetached can be true while the independent Cleanup reports history.
Save copies explicit named evidence; empty and absent data differ.
Receipt redelivery never executes the original operation again.

Commands counts admitted CDP attempts through the controlled action/command route,
not physical HTTP traffic. Initialization, cleanup and the legacy Navigate method
are outside that counter. The closed Navigate action and the public Page.Navigate
route do use it. RequestEvents counts observed requestWillBeSent events,
not exact requests, redirects, cache activity or connections.

## Limits and validation

Defaults are four contexts, four pages per context, no queued calls, 256 command
attempts, 1 MiB command bytes, 2 MiB result bytes, 2 MiB retained evidence and
2 MiB / 256 events per queue. Admission/startup/context/cleanup defaults are
1/15/30/5 seconds. Durations use nanoseconds.
Object groups default to 8 and retained object results to 128 per BrowserContext,
shared across its pages. Their logical metadata and origin-queue reservations are
included in the exact prepared Budget; these do not bound the remote JavaScript heap.

Native queues include pump-held batches in count and byte charges. Overflow is
observable and cancels the owning source rather than silently dropping progress.
Binding limits byte-bound name routing, then admit payload decoding and handler creation.
Its join observes actual handler exit. These SDK controls alone do not complete
the public binding/subscription contract.

Logical reservations do not bound Chrome RSS, native DOM caches, socket traffic,
or every transport allocation. BrowserContext isolation is not a process sandbox.
Callbacks and serializers are cooperative Go code, not untrusted plugins.

ExecutionContexts reads the bounded native identity snapshot without a new
observer or Runtime toggle. ExecutionContext grants fill system-unique IDs in
ObjectGroup.EvaluateIn. Derived references preserve the same execution origin.
Frames and CreateWorld reuse the node/document generation observer. Native
WithPollingInFrame uses managed node provenance and joins its invalidation watcher.
MaxEvents also limits cumulative isolated-world creation attempts per page.

ControlOptions.Downloads adds target-scoped Browser download observations to the
existing control lease. Its context-wide policy requires a separate SharedDownloads
grant for borrowed contexts. GUID naming, explicit browser-host versus local
destinations, bounded local reads and caller-file preservation follow the
[public download contract](../../../../adapters/browser/chromedp/v1/interface.md).
Identity is captured in the bounded native observer before asynchronous delivery.
The source separately owns pending download cleanup and its logical reservation,
so a blocked transport cannot strand a local directory handle after process exit.
Pending source custody still consumes context capacity and prevents KeepOpen.

The selected tuple is chromedp v0.21.0 / cdproto v0.157.9 with Go 1.27.
The JSON bridge uses standard-library JSON types. Build reports the actual two
native modules and their replacements; unrelated consumers retain their own JSON
dependency. Profile keeps browser version, CDP version and declared ownership
separate. LaunchArgumentsCopy deliberately exposes supplied local arguments.

Run the focused package checks first:

```sh
go vet ./internal/browser/chromedp/v0
go test -race -count=1 ./internal/browser/chromedp/v0
FATHOMRY_CHROMEDP_INTEGRATION=1 \
FATHOMRY_CHROME_EXECUTABLE=/absolute/path/to/test-owned-chrome \
go test -race -count=1 -timeout=3m ./internal/browser/chromedp/v0
```

Absent opt-in is a skip, not browser acceptance. Fixtures use synthetic pages,
owned profiles and exact cleanup. Chrome150 qualification does not establish
Chromium157-only or hardware capabilities. The first page of a fresh context gets
a window, matching native v0.21. Later pages honor NewWindow. Partial target-creation
failure retains a separate protocol-peer regression requiring exact context cleanup.

See [public contracts](../../../../adapters/browser/chromedp/v1/interface.md),
[SDK integration](../../../../../architecture/sdk-integration.md), and
[issue #139](https://github.com/frost-leo/fathomry/issues/139).
Historical #52 evidence applies to its old tuple and is not rewritten.
