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
	"slices"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// ClosePage releases only this retained context's page/session. Owned pages
// close; discovered/explicitly borrowed targets detach. No root admission is
// needed, and a failed cleanup keeps its registry slot for retry.
func (retained *Context) ClosePage(ctx context.Context, page *Session) (err error) {
	if retained == nil || retained.contextState == nil || page == nil || page.sessionState == nil || ctx == nil || page.accounting() != retained.session.sessionState {
		return failure(ErrInput, "page-origin")
	}
	defer func() {
		if err != nil {
			err = failure(ErrCleanup, "page-close", err)
			page.mu.Lock()
			if page.pageCleanup == nil {
				page.pageCleanup = err
			}
			page.mu.Unlock()
			page.rememberCleanup(err)
		}
	}()
	select {
	case retained.gate <- struct{}{}:
		defer func() { <-retained.gate }()
	case <-ctx.Done():
		return errors.Join(ctx.Err(), context.Cause(ctx))
	}
	page.mu.Lock()
	complete, history := page.pageComplete, page.pageCleanup
	page.mu.Unlock()
	if complete {
		return history
	}
	work, cancel, err := (invocation.Budget{Limit: page.owner.settings.CleanupTimeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return err
	}
	defer cancel()
	if err := page.stopAndWait(work); err != nil {
		return err
	}
	if err := page.joinSubscriptions(work); err != nil {
		return err
	}
	page.mu.Lock()
	control := page.control
	page.mu.Unlock()
	if control != nil {
		value := &Control{state: control}
		history = errors.Join(history, value.Close(work))
		if page.borrowedTarget && !value.Closed() {
			return history
		}
	}
	own := page.owner
	own.mu.Lock()
	var bindings []*managedBinding
	var objects []*objectGroupState
	var streams []*streamState
	for state := range page.browserContext.bindings {
		if state.session.sessionState == page.sessionState {
			bindings = append(bindings, state)
		}
	}
	for state := range page.browserContext.objectGroups {
		if state.session.sessionState == page.sessionState {
			objects = append(objects, state)
		}
	}
	for state := range page.browserContext.streams {
		if state.session.sessionState == page.sessionState {
			streams = append(streams, state)
		}
	}
	own.mu.Unlock()
	for _, state := range bindings {
		binding := &Binding{state: state}
		history = errors.Join(history, binding.Close(work))
		if page.borrowedTarget && !binding.Closed() {
			return history
		}
	}
	if page.native != nil {
		if native := sdk.FromContext(page.native).Target; native != nil {
			if err := native.JoinBindings(work); err != nil {
				return errors.Join(history, err)
			}
		}
	}
	for _, state := range objects {
		history = errors.Join(history, (&ObjectGroup{state: state}).Close(work))
	}
	for _, state := range streams {
		stream := &Stream{state: state}
		history = errors.Join(history, stream.Close(work))
		if page.borrowedTarget && !stream.Closed() {
			return history
		}
	}
	joined, cleanup := page.closeNative(work)
	history = errors.Join(history, cleanup)
	if !joined {
		return history
	}
	if page.borrowedTarget {
		page.mu.Lock()
		detached := page.objectSessionDetached
		page.mu.Unlock()
		if !detached {
			return errors.Join(history, failure(ErrCleanup, "page-detach-unknown"))
		}
	} else if err := page.confirmTargetGone(work); err != nil {
		return errors.Join(history, err)
	}
	if control != nil && !page.borrowedTarget {
		if err := control.disposed(work); err != nil {
			history = errors.Join(history, err)
			if !(&Control{state: control}).Closed() {
				return history
			}
		}
	}
	for _, state := range bindings {
		value := &Binding{state: state}
		history = errors.Join(history, value.Close(work))
		if !value.Closed() {
			return history
		}
	}
	for _, state := range streams {
		value := &Stream{state: state}
		history = errors.Join(history, value.Close(work))
		if !value.Closed() {
			return history
		}
	}
	for _, state := range objects {
		value := &ObjectGroup{state: state}
		history = errors.Join(history, value.Close(work))
		if !value.Closed() {
			return history
		}
	}
	page.mu.Lock()
	page.pageComplete, page.pageCleanup = true, history
	page.mu.Unlock()
	record := page.accounting()
	record.mu.Lock()
	if page.borrowedTarget {
		record.data.pagesDetached++
	} else {
		record.data.pagesClosed++
	}
	record.mu.Unlock()
	retained.mu.Lock()
	retained.pages = slices.DeleteFunc(retained.pages, func(value *Session) bool { return value.sessionState == page.sessionState })
	retained.mu.Unlock()
	return history
}

func (page *Session) confirmTargetGone(ctx context.Context) error {
	if page.nativeResourcesGone() {
		return nil
	}
	if page.targetID == "" {
		return failure(ErrCleanup, "unknown-page-target")
	}
	work := sdk.WithCallLimits(ctx, sdk.CallLimits{MaxInputBytes: 4096, MaxOutputBytes: page.owner.settings.MaxResultBytes})
	browser := sdk.FromContext(page.owner.root).Browser
	result, err := cdp.Call(work, browser, target.GetTargets, target.GetTargetsParams{})
	if err != nil {
		return err
	}
	for _, info := range result.TargetInfos {
		if info != nil && info.TargetID == page.targetID {
			if info.BrowserContextID != page.observedID {
				return failure(ErrCleanup, "page-target-origin")
			}
			_, err := cdp.Call(work, browser, target.CloseTarget, target.CloseTargetParams{TargetID: page.targetID})
			return errors.Join(failure(ErrCleanup, "page-target-still-present"), err)
		}
	}
	page.mu.Lock()
	page.targetGone = true
	page.mu.Unlock()
	page.owner.mu.Lock()
	delete(page.browserContext.ownedTargets, page.targetID)
	page.owner.mu.Unlock()
	return nil
}

func (session *Session) PageClosed() bool {
	if session == nil || session.sessionState == nil {
		return false
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.pageComplete
}

// The SDK cancellation attempt runs once, and its actual goroutines stay joined
// across cleanup timeouts. Context and individual-page cleanup use the same state.
func (session *Session) closeNative(ctx context.Context) (bool, error) {
	session.mu.Lock()
	if session.nativeCloseDone == nil {
		session.nativeCloseDone = make(chan struct{})
		go func() {
			defer close(session.nativeCloseDone)
			if session.native == nil {
				return
			}
			err := sdk.Cancel(session.native)
			if err == nil {
				session.mu.Lock()
				session.objectSessionDetached = true
				session.mu.Unlock()
			} else if session.borrowedTarget {
				session.owner.mu.Lock()
				session.browserContext.detachUncertain = true
				session.owner.mu.Unlock()
			}
			if target := sdk.FromContext(session.native).Target; target != nil {
				err = errors.Join(err, target.WaitStopped(context.Background()))
			}
			session.nativeCloseErr = err
		}()
	}
	done := session.nativeCloseDone
	session.mu.Unlock()
	select {
	case <-done:
		return true, session.nativeCloseErr
	case <-ctx.Done():
		return false, failure(ErrCleanup, "native-target-join", ctx.Err(), context.Cause(ctx))
	}
}
