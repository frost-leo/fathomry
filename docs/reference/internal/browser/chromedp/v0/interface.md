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

**Audience:** internal composition and browser capability maintainers.
**Status:** implemented internal Provider; not a public API, production-site
qualification, hard browser-memory limit or complete HTTP quota implementation.
**Package:** `github.com/frost-leo/fathomry/internal/browser/chromedp/v0`.

## Configuration and call sequence

One externally configured entry creates one named Provider. There is no executable
discovery, business profile, URL allowlist, credential refresh, application retry,
rotation, Redis coordination or shared HTTP client facade.

1. Supply an explicit cancellable resource lifetime and `OptionsV1`. Choose either
   an absolute executable path or an exact borrowed browser WebSocket URL.
2. `Select` snapshots settings without I/O. Optional `resource.Layer` inputs are
   plain configuration. Go zero values use documented technical defaults; explicit
   layer zeros are validated without reapplying defaults.
3. Apply `resource.WithLimits`, assemble, create an independently owned bounded
   `invocation.Inbox[Result]`, and `Bind`. `LimitsV1` describes unoverridden Go
   options; layered settings need matching composition limits.
4. `Run` reserves evidence and authoritative source admission before starting the
   browser or creating a BrowserContext. Supply separate work and cleanup contexts.
   Each callback gets a fresh, isolated BrowserContext and one initial page target.
   `SessionOptionsV1` retains runtime proxy/bypass inputs without mutating the source.
5. Receive every Inbox delivery even when the direct caller handles its error.
   Inspect primary failure, cleanup failure, output and release independently.
   Close borrowing assemblies before the owning assembly.

`CheckReady` explicitly enables an assembly readiness check. Otherwise construction
is lazy. Readiness proves a browser/CDP round trip, not a page load, business access,
or readiness of an independently borrowed browser's other resources. Failed startup
is retained, not automatically retried or replaced by another executable.

The ordinary Go code belongs in Activities or other process-local callers, not
Temporal Workflow logic. No durable DTO or Workflow command changes are supplied.

## Who owns the browser and state?

| Resource | Local executable mode | Borrowed browser mode |
| --- | --- | --- |
| Process | Provider launches, signals and joins exactly its own process | External owner; never signaled or sent Browser.close |
| User-data directory | Fresh private temporary directory; removed after process join | External profile is never selected, copied or removed |
| Allocator / CDP Browser connection | Provider-owned RemoteAllocator and connection to the new process | Provider-owned connection to the explicit URL; external process is borrowed |
| BrowserContext | Provider creates one per Run, with dispose-on-debugger-detach | Same; only Provider-created contexts are disposed |
| Initial Target/tab | Provider-created, attached and canceled independently | Same; no existing target is reused |
| Popup/worker/subresource work | Belongs to the BrowserContext lifetime, not an extra source allowance | Same; unrelated BrowserContexts are not owned |

A remote allocator is intentionally used even for local processes: explicit
process ownership avoids coupling an SDK allocation timeout to process reaping.
The allocator owns a CDP connection, not the locally launched OS process.

Different Go contexts and different tabs do not imply Cookie/storage isolation.
Run creates a genuinely distinct BrowserContext; technical Cookie and storage
operations remain native capabilities inside it. BrowserContexts still share an
OS process, browser services and deployment limits. Their proxy settings do not
certify completely separate sockets, DNS state, caches or network-service processes.

A source alias created with `resource.Borrow` shares the original source metadata,
admission and native context ceiling. It cannot restart a process or manufacture a
new allowance. Independent Provider entries borrowing the same externally owned
browser do not collectively establish a process-wide quota; composition owns that
external sharing and its containment.

Native tab cancellation is joined independently of BrowserContext disposal;
native detach/close errors are preserved even when disposal succeeds. Connection
loss cancels session contexts and rejects new work on the dead connection.
Disposal failure transfers the unresolved context to the same source owner; its
native slot remains occupied. Explicit assembly cleanup reconciles known context
IDs and can continue after an incomplete result. If a borrowed connection is lost,
explicit cleanup can open one bounded cleanup-only connection to reconcile those
known IDs, without creating targets or closing the external browser. A creation
command that failed without returning an ID can leave an unknown context. Local process termination
settles that owner's native resources; a borrowed browser's unknown context cannot
be identified by guessing or deleting contexts created by other sessions. Its
unresolved cleanup remains explicit and may require external-owner reconciliation.
Retained cleanup records contain only IDs and creation uncertainty, not completed
sessions' output/event buffers. Copies of Session share one state and allowance.

Owned-process termination does not wait behind CDP socket shutdown. Native
chromedp socket writes ignore context cancellation: a permanently unresponsive
borrowed browser can still leave its owned connection's release unconfirmed.
Cleanup reports pending rather than killing an externally owned browser; the
external owner must restore or terminate that peer. No bounded shutdown of an
arbitrarily blocked borrowed transport is promised.

Cancellation of a callback, tab, connection or process never proves remote HTTP
handlers stopped, effects were reversed or data was complete. Noncooperative Go
callbacks/serializers cannot be forcibly timed out. Do not call assembly shutdown
reentrantly, retain owning capabilities in errors, or leave caller goroutines running.

## Native browser capabilities and restrictions

`Navigate` uses the native page-load navigation action. Redirects and subresources
are browser behavior; a returned load event does not wait for all background work.
`Actions` supports executor-only native Actions, including Evaluate and typed CDP
commands for DOM, input, emulation, technical cookies, response-body retrieval,
debugging and request interception. Caller-owned output pointers remain runtime
values; `Save` makes a separate, bounded copy for required evidence.

One navigation and one Actions group may overlap, with event reception, so an
intercepted request can be continued while navigation waits. Same-kind overlap is
rejected. All paths retain the session deadline and source reservation. No user
callback runs on a synchronous CDP event reader: `NextEvent` consumes a bounded
queue of immutable native event encodings. Overflow cancels the session and remains
a technical failure even when the caller ignores the returned error.

Actions receive no chromedp owning Context. `chromedp.Run`, `ListenTarget`,
high-level selector and navigation helpers that require `FromContext` are not
supported through Actions. Use `Navigate`, native CDP DOM commands and `NextEvent`.
Callback panics become a technical error; error-valued panic causes remain inspectable.
This restriction preserves useful native execution without exposing an allocator,
browser, Target, unrestricted executor, cancel authority or mutable owning handle.

The technical command boundary permits the domains listed by `Actions`.
Browser/Target/global Storage authority, native IO streams, downloads and selected
lifecycle/global mutations are refused. Unknown domains are not silently granted.
This includes process-global headless screen inventory/mutations, Network stream
creation and stream-mode PDF output. Inline PDF and target-local emulation remain
available through the same controlled executor.
These are resource-capability restrictions, not business profiles or site allowlists.
A captured executor is revoked when its Actions group returns; replacing or
detaching the Go context does not renew its authority. Extensions are trusted Go
code, not sandboxed plugins or a guarantee against arbitrary filesystem/network I/O
performed directly by caller code or by scripts running in the browser.

## Bounds and output meanings

Defaults are four live BrowserContexts, no queued calls, 256 extension commands
per session, 1 MiB per encoded command, 2 MiB cumulative delivered command/event
output, 2 MiB retained evidence and 2 MiB / 256 events in the queue. Admission,
startup, session and cleanup defaults are 1, 15, 30 and 5 seconds. Durations and
layered `*_ns` fields are nanoseconds. Refer to `OptionsV1` for validated ranges.

Working reservations, independent evidence bytes and native context slots are
different bounds. Context slots cover active and pending-cleanup contexts across
aliases, including their workers/popups/background activity until disposal.
They do **not** bound the number of those dependent targets, native renderer/network
processes, physical HTTP requests, redirects, WebSockets, connections or RSS.
Native CDP WebSocket decoding occurs before the extension response-size check;
Chrome and chromedp allocations are not bounded by the retained-output reservation.
Deployments requiring hard native-memory/process/network limits must provide real
external containment; this Provider must not be presented as satisfying those
limits by Go admission alone. It does not configure cgroups or system security.

`Result.CallbackCompleted` means the callback returned nil before its deadline.
It does not mean all actions succeeded, the output is complete or no effects occurred.
The first observed action/native/limit error is retained even when the callback
catches it. Canceling only a NextEvent wait returns a separate waiting error;
it does not cancel browser execution or turn missing events into empty success.
`ContextReleased` records positive disposal evidence independently of
`Receipt.Released`, which describes this producer's local use/ownership transfer.

`Save` requires unique non-secret names; returned bytes are copies. Explicit empty
output differs from missing output. A saved DOM value is not original response
bytes or a business result certificate. The original response and rendered DOM
can be inspected separately through native commands; no HTML persistence is implied.

`Commands` counts explicit extension commands, not initialization/cleanup or every
native command issued by a high-level navigation. `Navigations` counts completed
navigation helpers. `RequestEvents` counts observed target
Network.requestWillBeSent events only. None is an exact physical HTTP request
count; invocation attempts deliberately remain inexact with zero observed attempts.

Errors use internal `fault.Kind` and preserve native `errors.Is/As` causes.
Primary and cleanup evidence are separate. Ordinary formatting, logging and JSON
do not expose runtime configuration, browser payloads or native error text.
Deliberate copies and native cause inspection can expose sensitive data.

## Version axes and verification

The import major v0, OptionsV1, SessionOptionsV1, selected chromedp v0.16.0,
cdproto `v0.0.0-20260714215040-dc233986426f`, actual consuming Go/module graph,
actual Chrome/Chromium version, CDP version and launch arguments are separate axes.
`Build` reads the consuming executable. `Profile` reports a genuinely observed
browser product/version and CDP version after startup, not the browser's HTTP
protocol or arbitrary tuple qualification. `LaunchArgumentsCopy` deliberately
returns supplied local arguments; it is not attestation of effective browser flags.
Borrowed-process launch arguments remain unknown. `Profile.Native` intentionally
remains unknown: the observed product/version and private argument marker cannot
certify a deployed native binary/launch configuration through `compatibility.Assess`.
No blanket compatibility catalog
is installed.

The native Chrome 150 target-creation counterexample is retained as an opt-in test:
explicit `NewWindow` is a separate CDP construction choice, not a business profile
or inferred feature of a synthetic browser label. Changing the browser/bindings
tuple requires requalifying that control, not silently skipping it.

From the repository root, after dependency acquisition:

```sh
go mod tidy -diff
go vet ./internal/browser/chromedp/v0
go test -race -count=1 ./internal/browser/chromedp/v0
FATHOMRY_CHROMEDP_INTEGRATION=1 \
FATHOMRY_CHROME_EXECUTABLE=/absolute/path/to/test-owned-chrome \
go test -race -count=1 -timeout=3m ./internal/browser/chromedp/v0
```

Missing opt-in is a skip, not browser acceptance. With opt-in, missing/unusable
Chrome is a failure. Tests use their own fresh profiles, dynamic ports and local
synthetic pages/proxies, preserve the sandbox and compare HTML only in memory.
Use a short test TMPDIR because Chrome's Unix singleton socket has a path limit.
Do not point tests at an existing debugging port or real browser profile.

[Tests](../../../../../../internal/browser/chromedp/v0/) cover source/evidence
admission, aliases, copies, scope revocation, startup and partial creation, native
interception, original bytes versus rendered DOM, Cookie/storage isolation,
cancellation versus remote effects, runtime proxies and independent consumer builds.
Private experiments, browser profiles and agent reviews are not published.
Unselected SDKs are not imported. No public-site, anti-bot, distributed quota,
hard memory, persistent-profile, production TLS or full browser protocol certificate
is claimed.

The [SDK architecture](../../../../../architecture/sdk-integration.md),
[integration standards](../../../../../architecture/internal-sdk-integration.md)
and [issue #52](https://github.com/frost-leo/fathomry/issues/52) govern this boundary.
Pinned upstream [context/cancellation](https://github.com/chromedp/chromedp/blob/7963c203ed5458147d27dc39a5c06d2b12e81664/chromedp.go)
and [allocator](https://github.com/chromedp/chromedp/blob/7963c203ed5458147d27dc39a5c06d2b12e81664/allocate.go)
contracts explain why browser ownership is not ordinary HTTP transport ownership.
