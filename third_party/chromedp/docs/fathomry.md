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

# Fathomry maintenance for issue 139

This tree starts from the official chromedp v0.21.0 module ZIP.
UPSTREAM.json records its origin and module checksums. LICENSE is unchanged.
The maintained module requires cdproto v0.157.9. Upstream v0.21.0 requires v0.157.8.
The protocol upgrade changes README.md only. Runtime files and dependency declarations are identical.

## Local changes

EventBuffer bounds native subscription queues by count and bytes.
The charges include the pump's blocked batch. Overflow stops delivery.
The configured callback can cancel the owner without running browser commands.
Bounded queues stop on target termination instead of waiting for an absent reader.

WaitEvent uses a lifetime-bound subscription. A failed trigger now cancels it.
Raw cdp.Events still requires its consumer to own cancellation at construction.

CallLimits admits work before encoding and checks results before caller decoding.
Poll and function helpers admit argument preparation once, before argument codecs.
Their argument buffers share the selected input limit.
Function-call argument encoding preserves explicit empty JSON values and null.
This correction is local to Runtime.callFunctionOn parameters, not global JSON behavior.
It does not bound native transport allocation or browser memory.
Target cleanup can use a caller-selected timeout instead of the five-second default.
Managed target dispatch can outlive an operation's cancellation for IO cleanup.
Browser cancellation closes blocked transports through a separate path.
WaitStopped joins the native dispatch and reader loops. Subscription cancellation joins its pump.
Pending target calls also end when their target stops, without closing the browser peer.

BindingLimits bounds name routing before it admits payload decoding and handler creation.
Unrelated binding events do not compete for another handler's active slot.
PrivateErrors separates page-visible error text from deliberately inspectable causes.
JoinBindings seals admission and waits for actual handlers and replies.
Cancellation does not kill a cooperative handler or release its active charge.

BindFunc owns one registration and uses the same argument conversion as ExposeFunc.
ExposeFunc delegates to this engine. It does not keep a second implementation.
Admission precedes argument decoding and goroutine creation.
Completion follows result encoding and the reply, including panic and Goexit paths.
Replies use execution context unique IDs, not reusable numeric IDs.
Close removes the native binding, document script and captured page aliases.
It restores an earlier page property only while the wrapper still owns it.
Cleanup retries retain original capture identity and earlier failures.
A lost cleanup reply does not authorize capturing a replacement function.
Unknown cleanup stays incomplete. Closed requires actual handler completion too.
Existing isolated worlds retain their public properties.
The public wrapper runs in the same default worlds as native ExposeFunc.
Technical hooks must not block, panic, call Goexit or run browser commands.
This mechanism is not a sandbox against page scripts or independent peers.

Fathomry command permissions, source generations, shared quotas and receipts stay
outside this module. Native hooks do not decide those policies.

SubscribeMany with no methods receives every target event in one bounded queue.
The Internal bridge ignores unknown protocol methods, as the native target does.
Malformed known events still fail decoding.

SubscribeBounded selects limits for one queue without changing other subscribers.
Observe runs after queue admission and before delivery, under the native publisher lock.
Its sequence belongs to that subscription, not to the browser or target.
Both tagged and ordinary subscriptions pass the actual method to Observe.
Private binding routing checks bounded bytes before it consumes queue capacity.
Execution context unique IDs are recorded before native event publication.
Queued work can capture the event-time origin instead of a reused numeric ID.

DecodeConsole provides Console's data conversion without target access.
Malformed console entries, exceptions and nested pointer lists return errors.
They no longer panic in the native formatter.

Clear now clears dirty live input and textarea values, including an empty textarea.
It retains default-value cleanup and does not add input or change events.
The property correction adds three CDP calls per selected node.

The remote package accepts an explicit TLS configuration.
It affects only the CDP connection, not the browser's HTTP traffic.

## Verification and distribution

TestManaged tests cover queue staging, failed triggers, logical bytes and binding joins.
They also cover argument presence, single command charging, binding routing and dirty form values.
The Fathomry issue packet retains original failures and real-browser controls.
The blocked writer test keeps its original timing and process-termination assertions.

UPSTREAM.json records the official source archive, not the maintained build's identity.
Independent consumers must explicitly select the maintained SDK. Root replacements do not propagate.
The local consumer negative control intentionally fails against unpatched upstream.
Publication requires separate immutable revision and module-checksum evidence.
This document does not attest publication or complete Browser Adapter acceptance.
