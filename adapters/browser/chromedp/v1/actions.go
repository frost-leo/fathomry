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
	"errors"
	"sync"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// Action delegates to a native helper without exposing the native owner.
type Action[T any] struct {
	private
	native native.Action[T]
	name   string
}
type QueryOption struct{ native native.QueryOption }
type TextSelector interface{ ~string }

func nativeQuery(options []QueryOption) []native.QueryOption {
	if len(options) > 64 {
		return []native.QueryOption{{}}
	}
	values := make([]native.QueryOption, len(options))
	for i, v := range options {
		values[i] = v.native
	}
	return values
}
func NodeVisible() QueryOption      { return QueryOption{native.NodeVisible()} }
func NodeReady() QueryOption        { return QueryOption{native.NodeReady()} }
func NodeEnabled() QueryOption      { return QueryOption{native.NodeEnabled()} }
func NodeSelected() QueryOption     { return QueryOption{native.NodeSelected()} }
func NodeNotVisible() QueryOption   { return QueryOption{native.NodeNotVisible()} }
func NodeNotPresent() QueryOption   { return QueryOption{native.NodeNotPresent()} }
func AtLeast(count int) QueryOption { return QueryOption{native.AtLeast(count)} }
func RetryInterval(interval time.Duration) QueryOption {
	return QueryOption{native.RetryInterval(interval)}
}
func Text[S TextSelector](selector S, options ...QueryOption) Action[string] {
	return Action[string]{native: native.Text(selector, nativeQuery(options)...), name: "text"}
}
func Value[S TextSelector](selector S, options ...QueryOption) Action[string] {
	return Action[string]{native: native.Value(selector, nativeQuery(options)...), name: "value"}
}
func Click[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.Click(selector, nativeQuery(options)...), name: "click"}
}

// SendKeys retains native text and file-input behavior. For a file input, value
// is a browser-host path, not a Go-local upload. SetUploadFiles makes that choice
// explicit and also supports multiple files or clearing the selected files.
func SendKeys[S TextSelector](selector S, value string, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.SendKeys(selector, value, nativeQuery(options)...), name: "send_keys"}
}
func Screenshot[S TextSelector](selector S, options ...QueryOption) Action[[]byte] {
	return Action[[]byte]{native: native.Screenshot(selector, nativeQuery(options)...), name: "screenshot"}
}
func Evaluate[T any](expression string, options ...EvaluateOption) Action[T] {
	return Action[T]{native: native.Evaluate[T](expression, nativeEval(options)...), name: "evaluate"}
}
func Poll[T any](expression string, options ...PollOption) Action[T] {
	return Action[T]{native: native.Poll[T](expression, nativePoll(options)...), name: "poll"}
}
func CaptureScreenshot() Action[[]byte] {
	return Action[[]byte]{native: native.CaptureScreenshot(), name: "capture"}
}
func FullScreenshot(quality int) Action[[]byte] {
	return Action[[]byte]{native: native.FullScreenshot(quality), name: "full_screenshot"}
}

// RunAction reserves independent evidence before native setup and decoding.
// Returning or ignoring the value never acknowledges its independent receipt.
func RunAction[T any](ctx context.Context, page *Page, action Action[T]) (value T, receipt *adapters.Receipt[Result], err error) {
	if action.native.NeedsCustody() {
		return runManagedValue(ctx, page, action)
	}
	if isObjectResult[T]() {
		object, receipt, err := runRemoteAction(ctx, page, action)
		if object != nil && object.native.NativeValue() != nil {
			value = any(object.native.NativeValue()).(T)
		}
		return value, receipt, err
	}
	return runPageOperation(ctx, page, action.name, func(ctx context.Context) (T, error) {
		return native.RunAction(ctx, page.native, action.native)
	})
}

func runPageOperation[T any](ctx context.Context, page *Page, name string, run func(context.Context) (T, error)) (value T, receipt *adapters.Receipt[Result], err error) {
	if ctx == nil || page == nil || page.context == nil || name == "" {
		return value, nil, fail(ErrInput, "action")
	}
	return runOperation(ctx, page.context, name, page.beginOperation, run)
}

func runContextOperation[T any](ctx context.Context, family *Context, name string, run func(context.Context) (T, error)) (value T, receipt *adapters.Receipt[Result], err error) {
	if ctx == nil || family == nil || family.contextState == nil || name == "" {
		return value, nil, fail(ErrInput, "context-operation")
	}
	return runOperation(ctx, family, name, family.beginOperation, run)
}

func runOperation[T any](ctx context.Context, family *Context, name string, begin func() (func(), error), run func(context.Context) (T, error)) (value T, receipt *adapters.Receipt[Result], err error) {
	var primary error
	receipt, err = family.endpoint.Child(ctx, family.call.Scope(), request(name, family.id, 0, family.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := begin()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		defer finish()
		var cleanup helperCleanupEvidence
		resolved := false
		defer func() {
			if !resolved {
				cause, _ := recover().(error)
				primary = fail(ErrOperation, "action-did-not-return", cause)
				_ = call.Resolve(adapters.Outcome[Result]{Primary: primary, Cleanup: cleanup.get()})
			}
		}()
		output, e := run(native.WithHelperCleanupObserver(call.Context(), cleanup.observe))
		primary = translate(e, name)
		cleanup.observe(native.HelperCleanup(e))
		buffer := payloadBuffer{limit: family.budget.EvidenceBytes - 65536}
		if e == nil {
			if e = native.WriteNativeResult(&buffer, output); e != nil {
				primary = translate(e, "retain-output")
			} else {
				value = output
			}
		}
		info, _ := call.Receipt().Snapshot()
		observation := browser.Observation{Returned: e == nil, Effect: browser.Unknown}
		if e == nil {
			observation.Effect = browser.Acknowledged
		}
		_ = call.Resolve(adapters.Outcome[Result]{Value: Result{payload: buffer.data, observation: observation, attribution: attribution(info.Info())}, Present: true, Primary: primary, Cleanup: cleanup.get()})
		resolved = true
	})
	return value, receipt, errors.Join(err, receiptError(receipt, primary))
}

type helperCleanupEvidence struct {
	mu  sync.Mutex
	err error
}

func (e *helperCleanupEvidence) observe(err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err == nil {
		e.err = translate(err, "cleanup")
	}
}

func (e *helperCleanupEvidence) get() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func (p *Page) Navigate(ctx context.Context, address string) (*adapters.Receipt[Result], error) {
	if p == nil || p.context == nil || ctx == nil {
		return nil, fail(ErrInput, "navigate")
	}
	c := p.context
	var primary error
	receipt, err := c.endpoint.Child(ctx, c.call.Scope(), request("navigate", c.id, 0, c.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := p.beginOperation()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		defer finish()
		_, err = native.RunAction(call.Context(), p.native, native.Navigate(address))
		primary = translate(err, "navigate")
		info, _ := call.Receipt().Snapshot()
		_ = call.Resolve(adapters.Outcome[Result]{Value: Result{attribution: attribution(info.Info()), observation: browser.Observation{Returned: primary == nil, Effect: browser.Unknown}}, Present: true, Primary: primary})
	})
	return receipt, errors.Join(err, receiptError(receipt, primary))
}

type payloadBuffer struct {
	data  []byte
	limit int64
}

func (b *payloadBuffer) Write(data []byte) (int, error) {
	if int64(len(data)) > b.limit-int64(len(b.data)) {
		return 0, fail(ErrLimit, "output")
	}
	b.data = append(b.data, data...)
	return len(data), nil
}
func contextOptions(value ContextOptions) native.SessionOptionsV1 {
	return native.SessionOptionsV1{ProxyServer: value.ProxyServer, ProxyBypassList: value.ProxyBypassList, Borrow: value.Borrow, ContextID: cdp.BrowserContextID(value.ContextID), TargetID: target.ID(value.TargetID), DefaultContext: value.DefaultContext, SharedStorage: value.SharedStorage, SharedPermissions: value.SharedPermissions, SharedDownloads: value.SharedDownloads, ManagedEvents: true}
}
