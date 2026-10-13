/*
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

package chromedp

import (
	"github.com/chromedp/cdproto/css"
	"github.com/chromedp/cdproto/dom"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

func TextContent[S TextSelector](selector S, options ...QueryOption) Action[string] {
	return Action[string]{native: native.TextContent(selector, nativeQuery(options)...), name: "text_content"}
}
func OuterHTML[S TextSelector](selector S, options ...QueryOption) Action[string] {
	return Action[string]{native: native.OuterHTML(selector, nativeQuery(options)...), name: "outer_html"}
}
func InnerHTML[S TextSelector](selector S, options ...QueryOption) Action[string] {
	return Action[string]{native: native.InnerHTML(selector, nativeQuery(options)...), name: "inner_html"}
}
func Attributes[S TextSelector](selector S, options ...QueryOption) Action[map[string]string] {
	return Action[map[string]string]{native: native.Attributes(selector, nativeQuery(options)...), name: "attributes"}
}
func AttributesAll[S TextSelector](selector S, options ...QueryOption) Action[[]map[string]string] {
	return Action[[]map[string]string]{native: native.AttributesAll(selector, nativeQuery(options)...), name: "attributes_all"}
}
func Dimensions[S TextSelector](selector S, options ...QueryOption) Action[*dom.BoxModel] {
	return Action[*dom.BoxModel]{native: native.Dimensions(selector, nativeQuery(options)...), name: "dimensions"}
}
func ComputedStyle[S TextSelector](selector S, options ...QueryOption) Action[[]*css.ComputedStyleProperty] {
	return Action[[]*css.ComputedStyleProperty]{native: native.ComputedStyle(selector, nativeQuery(options)...), name: "computed_style"}
}
func MatchedStyle[S TextSelector](selector S, options ...QueryOption) Action[*css.GetMatchedStylesForNodeResult] {
	return Action[*css.GetMatchedStylesForNodeResult]{native: native.MatchedStyle(selector, nativeQuery(options)...), name: "matched_style"}
}
func WaitReady[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.WaitReady(selector, nativeQuery(options)...), name: "wait_ready"}
}
func WaitVisible[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.WaitVisible(selector, nativeQuery(options)...), name: "wait_visible"}
}
func WaitNotVisible[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.WaitNotVisible(selector, nativeQuery(options)...), name: "wait_not_visible"}
}
func WaitEnabled[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.WaitEnabled(selector, nativeQuery(options)...), name: "wait_enabled"}
}
func WaitSelected[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.WaitSelected(selector, nativeQuery(options)...), name: "wait_selected"}
}
func WaitNotPresent[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.WaitNotPresent(selector, nativeQuery(options)...), name: "wait_not_present"}
}
func Focus[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.Focus(selector, nativeQuery(options)...), name: "focus"}
}
func Blur[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.Blur(selector, nativeQuery(options)...), name: "blur"}
}
func Clear[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.Clear(selector, nativeQuery(options)...), name: "clear"}
}
func Tap[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.Tap(selector, nativeQuery(options)...), name: "tap"}
}
func DoubleClick[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.DoubleClick(selector, nativeQuery(options)...), name: "double_click"}
}
func Submit[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.Submit(selector, nativeQuery(options)...), name: "submit"}
}
func Reset[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.Reset(selector, nativeQuery(options)...), name: "reset"}
}
func ScrollIntoView[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.ScrollIntoView(selector, nativeQuery(options)...), name: "scroll_into_view"}
}
func SetValue[S TextSelector](selector S, value string, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.SetValue(selector, value, nativeQuery(options)...), name: "set_value"}
}

// SetAttributes borrows the map until execution returns. Copying and native
// selector work occur only after resource/evidence admission.
func SetAttributes[S TextSelector](selector S, attributes map[string]string, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.SetAttributes(selector, attributes, nativeQuery(options)...), name: "set_attributes"}
}
func AttributeValue[S TextSelector](selector S, name string, options ...QueryOption) Action[sdk.AttributeResult] {
	return Action[sdk.AttributeResult]{native: native.AttributeValue(selector, name, nativeQuery(options)...), name: "attribute"}
}
func SetAttributeValue[S TextSelector](selector S, name, value string, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.SetAttributeValue(selector, name, value, nativeQuery(options)...), name: "set_attribute"}
}
func RemoveAttribute[S TextSelector](selector S, name string, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.RemoveAttribute(selector, name, nativeQuery(options)...), name: "remove_attribute"}
}
func KeyEvent(keys string, options ...sdk.KeyOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.KeyEvent(keys, options...), name: "key_event"}
}
func MouseEvent(kind input.DispatchMouseEventType, x, y float64, options ...sdk.MouseOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.MouseEvent(kind, x, y, options...), name: "mouse_event"}
}
func TapXY(x, y float64) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.TapXY(x, y), name: "tap"}
}
func DragAndDrop[S, T TextSelector](from S, to T, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.DragAndDrop(from, to, nativeQuery(options)...), name: "drag_drop"}
}
func DragAndDropXY(fromX, fromY, toX, toY float64, steps ...int) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.DragAndDropXY(fromX, fromY, toX, toY, steps...), name: "drag_drop"}
}
func NavigateResponse(address string) Action[*network.Response] {
	return Action[*network.Response]{native: native.NavigateResponse(address), name: "navigate_response"}
}
func NavigationEntries() Action[page.GetNavigationHistoryResult] {
	return Action[page.GetNavigationHistoryResult]{native: native.NavigationEntries(), name: "navigation_entries"}
}
func NavigateBack() Action[sdk.Void] {
	return Action[sdk.Void]{native: native.NavigateBack(), name: "back"}
}
func NavigateForward() Action[sdk.Void] {
	return Action[sdk.Void]{native: native.NavigateForward(), name: "forward"}
}
func Reload() Action[sdk.Void] { return Action[sdk.Void]{native: native.Reload(), name: "reload"} }
func Stop() Action[sdk.Void]   { return Action[sdk.Void]{native: native.Stop(), name: "stop"} }
func Title() Action[string]    { return Action[string]{native: native.Title(), name: "title"} }
func Location() Action[string] { return Action[string]{native: native.Location(), name: "location"} }
func EmulateViewport(width, height int64, options ...sdk.EmulateViewportOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.EmulateViewport(width, height, options...), name: "emulate_viewport"}
}
func ResetViewport() Action[sdk.Void] {
	return Action[sdk.Void]{native: native.ResetViewport(), name: "reset_viewport"}
}

// Emulate delegates the native device profile without exposing a native Target.
// Device data is read after admission and must remain immutable during execution.
func Emulate(device sdk.Device) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.Emulate(device), name: "emulate"}
}

func EmulateReset() Action[sdk.Void] {
	return Action[sdk.Void]{native: native.EmulateReset(), name: "emulate_reset"}
}

// PrintToPDF accepts native finite-output options. Use Page.PDFStream for an
// owned incremental stream rather than the SDK's whole-buffered stream option.
func PrintToPDF(options ...sdk.PDFOption) Action[[]byte] {
	return Action[[]byte]{native: native.PrintToPDF(options...), name: "pdf"}
}
