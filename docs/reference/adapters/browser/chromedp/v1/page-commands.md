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



# Managed page commands

[Documentation](../../../../../README.md) / Browser Adapter reference

**Audience:** callers of the chromedp Browser Adapter.
**Status:** implemented finite typed-command route; browser support is reported by
the selected native browser, not inferred from the CDP module version.
**Package:** `github.com/frost-leo/fathomry/adapters/browser/chromedp/v1`.

## Use native types without exporting the native owner

`Command(nativeCommand, nativeParams)` builds a closed `Action[R]`. Execute it
with `RunAction` on an admitted Page. The command name and its exact native
parameter and result types are checked together. Changing a command's generic
types or spelling cannot acquire additional authority. A Page never exposes its
native Target, Browser or an allocator-bearing context.

This route delegates one protocol operation. It does not replace native selector
readiness, navigation completion, HTTP response observation or Poll semantics.
Use the corresponding high-level actions for those operations.

Parameters, including nested pointers and slices, remain caller-owned and
immutable until execution returns. The Adapter copies them after admission and
before dispatch. Explicit native zero values and absence keep their native
meanings. Command count, encoded input/output bounds and independent receipt
admission use the same limits as the other page operations.

## Which commands use this route?

The [finite command table](../../../../../../internal/browser/chromedp/v0/page_command.go)
is the exact authority list, separate from the larger protocol vocabulary
catalogue. Its implemented groups are:

- Page finite screenshot and MHTML capture, layout metrics, navigation history
  observation and explicit history reset.
- DOMSnapshot capture with native computed-style and layout options.
- Page-local Emulation overrides for dimensions, device posture, display features,
  scale, focus, background, dark mode, media, text, vision, touch, script execution,
  scrollbars, document-cookie access, user agent, safe-area insets and virtual-keyboard geometry.
- Network extra HTTP headers for the page's requests.
- Input keyboard, mouse, touch, composition, text insertion, input suppression,
  drag cancellation and finite synthetic gestures.
- Performance.GetMetrics, without implicitly enabling metric collection.

An empty or nil SetDisplayFeaturesOverride feature list is rejected. Use the
native ClearDisplayFeaturesOverride command to clear an override. Nil feature
entries are also invalid. This input guard avoids a demonstrated browser failure;
it does not replace the native display implementation.

Screens, process-wide locale/timezone/CPU effects, intercepted drags, browser-host
files, streams and long-running producers need their separate ownership routes.
They are not authorized by a page-command name or a domain-wide prefix. A method
outside this finite route returns ErrUnsupported here; this is not a statement
that the native SDK lacks that method.

## Page request configuration

Use `Command(network.SetExtraHTTPHeaders, network.SetExtraHTTPHeadersParams{...})`
for browser HTTP request headers, and
`Command(emulation.SetUserAgentOverride, emulation.SetUserAgentOverrideParams{...})`
for native user-agent, language, platform and client-hint metadata options.
`Settings.Headers` instead configures the **CDP connection handshake**; it does
not set headers on fetched pages. These commands use the existing exact-type,
quota-before-copy and bounded-evidence path, without creating another owner.

An empty header map clears that session's extra headers. An empty user-agent
parameter with no metadata clears its override. Native validation and omission
semantics remain in effect; a native error does not prove no setting changed.
For example, Chrome 150 can assign user-agent fields before rejecting metadata.

The settings apply to requests of the target while that CDP session is attached,
not one navigation, request or origin. They can affect work by another caller of
a borrowed page. Do not use them as an origin-restricted credential mechanism.
Sibling pages are separate; cookies still belong to the BrowserContext and a
shared borrowed context requires its explicit storage grant. Closing the managed
borrow detaches its session rather than closing the peer's page. Synthetic Chrome
150 controls verify reset, sibling isolation and removal of these request
overrides after detach in
[`request_configuration_test.go`](../../../../../../adapters/browser/chromedp/v1/request_configuration_test.go).

## Results, identities and effects

Native snapshot node/frame identifiers and history entries are observation data.
They do not authorize later operations on a node, frame, document or another page.
Sensitive page contents, URLs and pixels are deliberate return values and receipt
payloads. Ordinary Adapter formatting does not print those payloads.

A native acknowledgement is not proof of HTTP success. Cancellation does not
undo input, page mutation or a gesture already submitted to the browser. Receipt
redelivery reads retained evidence and never repeats the command. Follow changes
new retention only; an existing Page continues to use its captured source.

Context-wide cookies and permissions are not page commands. See the
[provider contract](interface.md) for their retained-context grants and mutation
semantics. Network response-body/post-data/search and explicit XHR replay use
RunRequestCommand with a producer-observed Request grant. Fetch response bodies
use their Pending control token, not an arbitrary request ID. These routes retain
their separate output, continuation and cleanup obligations.
These boundaries are exercised by `command_test.go`, `request_test.go`, `control_body_test.go`,
`navigation_test.go`, `cookies_test.go` and `permissions_test.go`.
