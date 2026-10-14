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

	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	resource "github.com/frost-leo/fathomry/internal/resource"
)

// KeptBrowser is the new owner of an explicitly transferred local process and
// profile cleanup. It owns no Provider connection. It needs Close even when
// KeepOpen also reports old-source cleanup failure. Copies share ownership.
type KeptBrowser struct {
	private
	state *keptBrowserState
}
type keptBrowserState struct {
	native *native.KeptBrowser
	call   *adapters.Call[Result]
	guard  adapters.Guard
	gate   chan struct{}
	mu     sync.Mutex
	closed bool
	final  error
}

// KeepOpen transfers a quiescent local process to an independent root receipt,
// then closes the old Provider connection. Close retained contexts/producers first.
// An explicitly borrowed default page can remain visible; owned isolated contexts
// have their normal disposal semantics. No external browser can be transferred.
//
// ctx is the new cleanup owner's lifetime, independent of the old Owner's context.
// Cancellation closes the kept process. Use a suitable long-lived context, and
// call KeptBrowser.Close before closing the shared Runtime. Admission failure
// leaves all responsibility with the old Owner.
func (owner *Owner) KeepOpen(ctx context.Context) (kept *KeptBrowser, receipt *adapters.Receipt[Result], err error) {
	if owner == nil || owner.state == nil || owner.client == nil || ctx == nil {
		return nil, nil, fail(ErrInput, "keep-open")
	}
	source := owner.state
	var primary error
	receipt, err = owner.client.endpoint.Run(ctx, request("keep_open", "", 65536, source.prepared.policy.Budget.SourceEvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := source.use()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		defer finish()
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		capability, _, err := resource.Bind(source.assembly, source.selected)
		var value *native.KeptBrowser
		if err == nil {
			value, err = native.KeepOpen(call.Context(), capability)
		}
		primary = translate(err, "keep-open")
		if value == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &keptBrowserState{native: value, call: call, guard: guard, gate: make(chan struct{}, 1)}
		kept = &KeptBrowser{state: state}
		source.mu.Lock()
		source.transferred = true
		source.mu.Unlock()
		context.AfterFunc(call.Context(), func() { _ = kept.Close(context.Background()) })
	})
	err = errors.Join(err, receiptError(receipt, primary))
	if kept != nil {
		err = errors.Join(err, owner.Close(ctx))
	}
	return kept, receipt, err
}

// Endpoint deliberately exposes the loopback browser connection address. Any
// later connection is a new borrowing operation, not revival of the closed Owner.
func (kept *KeptBrowser) Endpoint() string {
	if kept == nil || kept.state == nil {
		return ""
	}
	return kept.state.native.Endpoint()
}
func (kept *KeptBrowser) ProfileDirectory() browser.HostPath {
	if kept == nil || kept.state == nil {
		return ""
	}
	return browser.HostPath(kept.state.native.ProfileDirectory())
}
func (kept *KeptBrowser) ProcessExited() bool {
	return kept != nil && kept.state != nil && kept.state.native.ProcessExited()
}
func (kept *KeptBrowser) Closed() bool {
	if kept == nil || kept.state == nil {
		return false
	}
	kept.state.mu.Lock()
	defer kept.state.mu.Unlock()
	return kept.state.closed
}

// Close does not require new root admission. Actual exit, temporary-directory
// cleanup and receipt resolution must all finish before Closed becomes true.
func (kept *KeptBrowser) Close(ctx context.Context) error {
	if kept == nil || kept.state == nil || ctx == nil {
		return fail(ErrInput, "kept-browser-close")
	}
	state := kept.state
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "kept-browser-join")
	}
	state.mu.Lock()
	closed, final := state.closed, state.final
	state.mu.Unlock()
	if closed {
		return final
	}
	cleanup := translate(state.native.Close(ctx), "kept-browser-close")
	if !state.native.Closed() {
		return cleanup
	}
	info, _ := state.call.Receipt().Snapshot()
	value := Result{attribution: attribution(info.Info()), observation: browser.Observation{Returned: true, Transferred: true, ProcessExited: state.native.ProcessExited()}}
	if err := state.call.Resolve(adapters.Outcome[Result]{Present: true, Value: value, Cleanup: cleanup}); err != nil {
		return err
	}
	state.mu.Lock()
	state.closed, state.final = true, cleanup
	state.mu.Unlock()
	_ = state.guard.Release()
	return cleanup
}
