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
	"context"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/css"
	"github.com/chromedp/cdproto/dom"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
)

// deferred keeps native setup inside admitted execution.
func deferred[T any](size int64, build func() sdk.Action[T]) Action[T] {
	return Action[T]{bytes: size, local: func(ctx context.Context, session *Session) (T, error) {
		return sdk.Run(session.nativeWork(ctx), build())
	}}
}

func deferredPreparation[T any](size int64, build func() sdk.Action[T]) Action[T] {
	return Action[T]{bytes: size, local: func(ctx context.Context, session *Session) (value T, err error) {
		if err := session.reserveCommand(); err != nil {
			return value, err
		}
		var prepaid atomic.Bool
		prepaid.Store(true)
		work := session.commandWork(ctx, false, func() error {
			if prepaid.Swap(false) {
				return nil
			}
			return session.reserveCommand()
		})
		return sdk.Run(work, build())
	}}
}
func TextContent[S TextSelector](selector S, options ...QueryOption) Action[string] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[string] { return sdk.TextContent(selector, values...) })
}
func OuterHTML[S TextSelector](selector S, options ...QueryOption) Action[string] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[string] { return sdk.OuterHTML(selector, values...) })
}
func InnerHTML[S TextSelector](selector S, options ...QueryOption) Action[string] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[string] { return sdk.InnerHTML(selector, values...) })
}
func Attributes[S TextSelector](selector S, options ...QueryOption) Action[map[string]string] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[map[string]string] {
		return sdk.Attributes(selector, values...)
	})
}
func AttributesAll[S TextSelector](selector S, options ...QueryOption) Action[[]map[string]string] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[[]map[string]string] {
		return sdk.AttributesAll(selector, values...)
	})
}
func Dimensions[S TextSelector](selector S, options ...QueryOption) Action[*dom.BoxModel] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[*dom.BoxModel] { return sdk.Dimensions(selector, values...) })
}
func ComputedStyle[S TextSelector](selector S, options ...QueryOption) Action[[]*css.ComputedStyleProperty] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[[]*css.ComputedStyleProperty] {
		return sdk.ComputedStyle(selector, values...)
	})
}
func MatchedStyle[S TextSelector](selector S, options ...QueryOption) Action[*css.GetMatchedStylesForNodeResult] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[*css.GetMatchedStylesForNodeResult] {
		return sdk.MatchedStyle(selector, values...)
	})
}
func WaitReady[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.WaitReady(selector, values...) })
}
func WaitVisible[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.WaitVisible(selector, values...) })
}
func WaitNotVisible[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.WaitNotVisible(selector, values...) })
}
func WaitEnabled[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.WaitEnabled(selector, values...) })
}
func WaitSelected[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.WaitSelected(selector, values...) })
}
func WaitNotPresent[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.WaitNotPresent(selector, values...) })
}
func Focus[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.Focus(selector, values...) })
}
func Blur[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.Blur(selector, values...) })
}
func Clear[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.Clear(selector, values...) })
}
func Tap[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.Tap(selector, values...) })
}
func DoubleClick[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.DoubleClick(selector, values...) })
}
func Submit[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.Submit(selector, values...) })
}
func Reset[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.Reset(selector, values...) })
}
func ScrollIntoView[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.ScrollIntoView(selector, values...) })
}
func SetValue[S TextSelector](selector S, value string, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)+len(value)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.SetValue(selector, value, values...) })
}

// SetAttributes borrows the map until execution returns and copies it only after
// admission. The constructor does not walk or duplicate unbounded caller data.
func SetAttributes[S TextSelector](selector S, attributes map[string]string, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return Action[sdk.Void]{bytes: int64(len(selector)), local: func(ctx context.Context, session *Session) (sdk.Void, error) {
		work, values, err := session.queryWork(ctx, options)
		if err != nil {
			return sdk.Void{}, err
		}
		limit := session.owner.settings.MaxCommandBytes
		if int64(len(attributes)) > limit/4 {
			return sdk.Void{}, failure(ErrLimit, "attributes")
		}
		size := int64(len(selector))
		for key, value := range attributes {
			if int64(len(key)) > limit-size || int64(len(value)) > limit-size-int64(len(key)) {
				return sdk.Void{}, failure(ErrLimit, "attributes")
			}
			size += int64(len(key) + len(value))
		}
		return sdk.Run(work, sdk.SetAttributes(selector, maps.Clone(attributes), values...))
	}}
}
func AttributeValue[S TextSelector](selector S, name string, options ...QueryOption) Action[sdk.AttributeResult] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)+len(name)), options, func(values []sdk.QueryOption) sdk.Action[sdk.AttributeResult] {
		return sdk.AttributeValue(selector, name, values...)
	})
}
func SetAttributeValue[S TextSelector](selector S, name, value string, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)+len(name)+len(value)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] {
		return sdk.SetAttributeValue(selector, name, value, values...)
	})
}
func RemoveAttribute[S TextSelector](selector S, name string, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)+len(name)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] {
		return sdk.RemoveAttribute(selector, name, values...)
	})
}
func KeyEvent(keys string, options ...sdk.KeyOption) Action[sdk.Void] {
	if len(options) > 64 {
		return Action[sdk.Void]{}
	}
	options = slices.Clone(options)
	return deferred(int64(len(keys)), func() sdk.Action[sdk.Void] { return sdk.KeyEvent(keys, options...) })
}
func MouseEvent(kind input.DispatchMouseEventType, x, y float64, options ...sdk.MouseOption) Action[sdk.Void] {
	if len(options) > 64 {
		return Action[sdk.Void]{}
	}
	options = slices.Clone(options)
	return deferred(0, func() sdk.Action[sdk.Void] { return sdk.MouseEvent(kind, x, y, options...) })
}
func TapXY(x, y float64) Action[sdk.Void] {
	return deferred(0, func() sdk.Action[sdk.Void] { return sdk.TapXY(x, y) })
}

// TouchSwipeXY delegates a paced native touch sequence with release cleanup.
func TouchSwipeXY(fromX, fromY, toX, toY float64, duration time.Duration) Action[sdk.Void] {
	return deferred(0, func() sdk.Action[sdk.Void] { return sdk.TouchSwipeXY(fromX, fromY, toX, toY, duration) })
}
func DragAndDrop[S, T TextSelector](from S, to T, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(from)+len(to)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.DragAndDrop(from, to, values...) })
}
func DragAndDropXY(fromX, fromY, toX, toY float64, steps ...int) Action[sdk.Void] {
	steps = slices.Clone(steps[:min(len(steps), 1)])
	return deferred(0, func() sdk.Action[sdk.Void] { return sdk.DragAndDropXY(fromX, fromY, toX, toY, steps...) })
}
func NavigateResponse(address string) Action[*network.Response] {
	action := deferred(int64(len(address)), func() sdk.Action[*network.Response] { return sdk.NavigateResponse(address) })
	action.navigation = true
	return action
}

// Navigate uses the controlled action budget, unlike legacy Session.Navigate.
func Navigate(address string) Action[sdk.Void] {
	if address == "" || strings.ContainsRune(address, '\x00') {
		return Action[sdk.Void]{}
	}
	action := deferred(int64(len(address)), func() sdk.Action[sdk.Void] { return sdk.Navigate(address) })
	action.navigation = true
	return action
}
func NavigationEntries() Action[page.GetNavigationHistoryResult] {
	return deferred(0, sdk.NavigationEntries)
}
func NavigateBack() Action[sdk.Void] {
	a := deferred(0, sdk.NavigateBack)
	a.navigation = true
	return a
}
func NavigateForward() Action[sdk.Void] {
	a := deferred(0, sdk.NavigateForward)
	a.navigation = true
	return a
}
func Reload() Action[sdk.Void] { a := deferred(0, sdk.Reload); a.navigation = true; return a }
func Stop() Action[sdk.Void]   { return deferred(0, sdk.Stop) }
func Title() Action[string]    { return deferred(0, sdk.Title) }
func Location() Action[string] { return deferred(0, sdk.Location) }
func EmulateViewport(width, height int64, options ...sdk.EmulateViewportOption) Action[sdk.Void] {
	if len(options) > 64 {
		return Action[sdk.Void]{}
	}
	options = slices.Clone(options)
	return deferredPreparation(0, func() sdk.Action[sdk.Void] { return sdk.EmulateViewport(width, height, options...) })
}
func ResetViewport() Action[sdk.Void] { return deferred(0, sdk.ResetViewport) }

func Emulate(device sdk.Device) Action[sdk.Void] {
	if device == nil || nilLike(device) {
		return Action[sdk.Void]{}
	}
	return deferredPreparation(0, func() sdk.Action[sdk.Void] { return sdk.Emulate(device) })
}

func EmulateReset() Action[sdk.Void] { return deferred(0, sdk.EmulateReset) }

// PrintToPDF retains finite native bytes. Streaming uses Session.PDFStream.
// Option functions run after admission and must return cooperatively.
func PrintToPDF(options ...sdk.PDFOption) Action[[]byte] {
	if len(options) > 64 {
		return Action[[]byte]{}
	}
	options = slices.Clone(options)
	return Action[[]byte]{local: func(ctx context.Context, session *Session) ([]byte, error) {
		if err := session.reserveCommand(); err != nil {
			return nil, err
		}
		var params page.PrintToPDFParams
		for _, option := range options {
			if option == nil {
				return nil, failure(ErrInput, "pdf-option")
			}
			option(&params)
		}
		if params.TransferMode != "" && params.TransferMode != page.PrintToPDFTransferModeReturnAsBase64 {
			return nil, failure(ErrInput, "pdf-stream-handle-required")
		}
		buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
		if err := marshalBounded(&buffer, params); err != nil {
			return nil, err
		}
		var copied page.PrintToPDFParams
		if err := jsonv2.Unmarshal(buffer.data, &copied); err != nil {
			return nil, err
		}
		return sdk.Run(session.commandWork(ctx, false), sdk.PrintToPDF(func(params *page.PrintToPDFParams) { *params = copied }))
	}}
}
