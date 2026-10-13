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

	"github.com/chromedp/cdproto/debugger"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/performance"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// ControlOptions selects page interception, dialogs, screencast or observations.
// Native parameters are copied within admitted construction. Screencast frame
// bytes share the finite control ledger; this is not an unbounded video recorder.
// Network observes SDK-enabled request/response events without intercepting them
// or enabling/disabling the Network domain. All selections share this finite ledger.
// ServiceWorkers grants its BrowserContext's native storage-partition observer
// and controls; borrowed contexts require SharedStorage. Native Disable clears
// the partition's force-update setting, not a saved value from another controller.
type ControlOptions struct {
	private
	Fetch          *fetch.EnableParams
	Dialogs        bool
	Screencast     *page.StartScreencastParams
	Recording      *page.StartScreenRecordingParams
	FileChooser    bool
	Downloads      *DownloadOptions
	Network        bool
	ServiceWorkers bool
	Profiler       bool
	Performance    *performance.EnableParams
	Debugger       *debugger.EnableParams
	// SharedIsolateEffects explicitly permits debugger interference with other
	// sessions sharing a JavaScript isolate, even for an otherwise owned page.
	SharedIsolateEffects bool
}

// Control owns a finite event and reply ledger. Its receipt is reserved before
// enabling the producer. Next and Reply use this reservation even with a full
// Inbox, an exhausted ordinary command allowance, or a waiting page operation.
// MaxEvents and MaxResultBytes bound its cumulative ledger, not only its queue.
type Control struct {
	private
	state *controlState
}

type controlState struct {
	native   *native.Control
	context  *Context
	page     *Page
	call     *adapters.Call[Result]
	guard    adapters.Guard
	gate     chan struct{}
	mu       sync.Mutex
	complete bool
	final    error
}

// Pending is a single-use target-origin reply. Its payload is deliberately
// inspectable native data, not a transferable request-ID authorization.
type Pending struct {
	private
	native  native.Pending
	control *Control
}

type Reply struct {
	private
	native native.Reply
}

func ContinueRequest(params fetch.ContinueRequestParams) Reply {
	return Reply{native: native.ContinueRequest(params)}
}
func ContinueResponse(params fetch.ContinueResponseParams) Reply {
	return Reply{native: native.ContinueResponse(params)}
}
func FailRequest(params fetch.FailRequestParams) Reply {
	return Reply{native: native.FailRequest(params)}
}
func FulfillRequest(params fetch.FulfillRequestParams) Reply {
	return Reply{native: native.FulfillRequest(params)}
}
func ContinueWithAuth(params fetch.ContinueWithAuthParams) Reply {
	return Reply{native: native.ContinueWithAuth(params)}
}
func HandleDialog(params page.HandleJavaScriptDialogParams) Reply {
	return Reply{native: native.HandleDialog(params)}
}

// ScreencastFrameAck acknowledges the exact pending frame without accepting a raw ID.
func ScreencastFrameAck() Reply { return Reply{native: native.ScreencastFrameAck()} }

// FileChooserFiles selects explicit browser-host paths for the latest observed
// input-element chooser. It does not transfer Go-local files to a remote host.
// Nil is invalid. An empty slice is forwarded with native empty-selection semantics.
func FileChooserFiles(files []browser.HostPath) Reply {
	return Reply{native: native.FileChooserFiles(files)}
}

func (p *Page) Control(ctx context.Context, options ControlOptions) (*Control, *adapters.Receipt[Result], error) {
	if ctx == nil || p == nil || p.context == nil {
		return nil, nil, fail(ErrInput, "control")
	}
	c := p.context
	var control *Control
	var primary error
	receipt, err := c.endpoint.Child(ctx, c.call.Scope(), request("control", c.id, 0, c.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := p.beginOperation()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		defer finish()
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		nativeOptions := native.ControlOptions{Fetch: options.Fetch, Dialogs: options.Dialogs, Screencast: options.Screencast, Recording: options.Recording, FileChooser: options.FileChooser, Network: options.Network, ServiceWorkers: options.ServiceWorkers, Profiler: options.Profiler, Performance: options.Performance, Debugger: options.Debugger, SharedIsolateEffects: options.SharedIsolateEffects}
		if options.Downloads != nil {
			nativeOptions.Downloads = &native.DownloadOptions{LocalDirectory: options.Downloads.LocalDirectory, BrowserDirectory: string(options.Downloads.BrowserDirectory)}
		}
		value, err := p.native.Control(call.Context(), nativeOptions)
		primary = translate(err, "control")
		if value == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &controlState{native: value, context: c, page: p, call: call, guard: guard, gate: make(chan struct{}, 1)}
		control = &Control{state: state}
		c.mu.Lock()
		if c.controls == nil {
			c.controls = make(map[*controlState]struct{})
		}
		c.controls[state] = struct{}{}
		c.mu.Unlock()
		go func() {
			<-value.Stopping()
			_ = control.Close(context.Background())
		}()
	})
	return control, receipt, errors.Join(err, receiptError(receipt, primary))
}
func (control *Control) Next(ctx context.Context) (Pending, error) {
	if ctx == nil || control == nil || control.state == nil {
		return Pending{}, fail(ErrInput, "control-next")
	}
	value, err := control.state.native.Next(ctx)
	return Pending{native: value, control: control}, translate(err, "control-next")
}
func (pending Pending) Method() string   { return pending.native.Method() }
func (pending Pending) DataCopy() []byte { return pending.native.DataCopy() }

// Reply borrows parameters through completion and fills native identity only in
// a copy. Only one reply runs per control lease. A submitted reply cannot retry.
// CDP dialog replies address the page's current dialog, not an atomic dialog ID.
// Known stale dialog observations are refused, but another browser controller can
// still replace the dialog after that check. No exclusive-peer guarantee is made.
func (pending Pending) Reply(ctx context.Context, reply Reply) error {
	return translate(pending.native.Reply(ctx, reply.native), "control-reply")
}
func (control *Control) Closed() bool {
	if control == nil || control.state == nil {
		return false
	}
	control.state.mu.Lock()
	defer control.state.mu.Unlock()
	return control.state.complete
}
func (control *Control) Close(ctx context.Context) error {
	if ctx == nil || control == nil || control.state == nil {
		return fail(ErrInput, "control-close")
	}
	state := control.state
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "control-close")
	}
	state.mu.Lock()
	if state.complete {
		err := state.final
		state.mu.Unlock()
		return err
	}
	state.mu.Unlock()
	_ = state.call.Cancel(nil)
	cleanup := translate(state.native.Close(ctx), "control-close")
	if !state.native.Closed() {
		return cleanup
	}
	payload, err := state.native.EvidenceCopy()
	primary := errors.Join(translate(state.native.Err(), "control"), translate(err, "control-evidence"))
	info, _ := state.call.Receipt().Snapshot()
	if err := state.call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{payload: payload, attribution: attribution(info.Info()), observation: browser.Observation{Returned: primary == nil, Effect: browser.Unknown}}, Primary: primary, Cleanup: cleanup}); err != nil {
		return err
	}
	state.mu.Lock()
	state.complete = true
	state.final = errors.Join(primary, cleanup)
	state.mu.Unlock()
	state.context.mu.Lock()
	delete(state.context.controls, state)
	state.context.mu.Unlock()
	_ = state.guard.Release()
	return state.final
}
