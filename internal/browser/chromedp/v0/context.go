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

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Context retains one admitted BrowserContext and its pages.
// Close is required even when Retain returns a non-nil handle with an error.
type Context struct {
	private
	*contextState
}

type contextState struct {
	session           *Session
	life              context.Context
	cancel            context.CancelCauseFunc
	call              *invocation.Call[Result]
	stop              func()
	mu                sync.Mutex
	pages             []*Session
	closing, complete bool
	commandBusy       bool
	commandIdle       chan struct{}
	gate              chan struct{}
	final             error
}

// Retain admits a context before starting native setup. Its receipt stays owned
// until Close joins page work and transfers any uncertain disposal to the source.
func (client *Client) Retain(ctx context.Context, id fault.Correlation, options ...SessionOptionsV1) (*Context, error) {
	if client == nil || client.owner == nil || client.access == nil || ctx == nil {
		return nil, failure(ErrInput, "retain")
	}
	value := client.owner.settings
	input, err := sessionOptions(options, value.MaxCommandBytes)
	if err != nil {
		return nil, err
	}
	call, err := invocation.Begin(ctx, client.access, invocation.Request{Name: "session", Correlation: id, Shape: invocation.Session, Bytes: value.reservation(), EvidenceBytes: value.evidenceBytes(), Admission: invocation.Budget{Limit: value.AdmissionTimeout}}, client.inbox, client.observer)
	if err != nil {
		return nil, err
	}
	work, done, err := (invocation.Budget{Limit: value.SessionTimeout}).Context(ctx, invocation.Lifetime)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return nil, err
	}
	session := &Session{sessionState: &sessionState{browserContext: &browserContext{}, owner: client.owner, data: resultData{output: make(map[string][]byte)}, events: make(chan Event, value.MaxEvents)}}
	life, cancel := context.WithCancelCause(work)
	session.work, session.cancel = context.WithCancelCause(life)
	session.managedEvents = input.ManagedEvents
	stopOwner := context.AfterFunc(client.owner.life, func() { cancel(context.Cause(client.owner.life)) })
	retained := &Context{contextState: &contextState{session: session, life: life, cancel: cancel, call: call, gate: make(chan struct{}, 1), pages: []*Session{session}}}
	retained.stop = sync.OnceFunc(func() { cancel(context.Canceled); stopOwner(); done() })
	err = client.owner.start(session.work)
	if err == nil {
		stopConnection := context.AfterFunc(client.owner.root, func() { cancel(failure(ErrDisconnected, "connection", context.Cause(client.owner.root))) })
		previous := retained.stop
		retained.stop = sync.OnceFunc(func() { stopConnection(); previous() })
		err = client.open(session, input)
	}
	if err != nil {
		err = failure(ErrNative, "retain", err)
	}
	session.remember(err)
	return retained, err
}

// Page returns the initial page. It cannot close the retained BrowserContext.
func (retained *Context) Page() *Session {
	if retained == nil || retained.contextState == nil {
		return nil
	}
	return retained.session
}
func (retained *Context) Receipt() *invocation.Receipt[Result] {
	if retained == nil || retained.contextState == nil || retained.call == nil {
		return nil
	}
	return retained.call.Receipt()
}

// Context is the retained BrowserContext lifetime, not the initial page lifetime.
// It carries cancellation only and cannot recover a native owner.
func (retained *Context) Context() context.Context {
	if retained == nil || retained.contextState == nil || retained.life == nil {
		return nil
	}
	return bareContext{retained.life}
}

// NewPage creates another owned page in this exact BrowserContext.
func (retained *Context) NewPage(ctx context.Context) (*Session, error) {
	return retained.openPage(ctx, "", false)
}

func (retained *Context) openPage(ctx context.Context, identity target.ID, detach bool) (*Session, error) {
	if retained == nil || retained.contextState == nil || retained.session == nil || ctx == nil {
		return nil, failure(ErrInput, "page")
	}
	retained.mu.Lock()
	root := retained.session
	if retained.closing || retained.life == nil || retained.life.Err() != nil {
		retained.mu.Unlock()
		return nil, failure(ErrState, "page")
	}
	if root.browserID == "" && !(root.borrowed && root.defaultContext && root.defaultIdentityObserved) {
		retained.mu.Unlock()
		return nil, failure(ErrState, "page-context-identity")
	}
	if identity != "" {
		for _, existing := range retained.pages {
			if existing.targetID != identity {
				continue
			}
			retained.mu.Unlock()
			if existing.setupDone != nil {
				select {
				case <-existing.setupDone:
				case <-ctx.Done():
					return nil, failure(ErrState, "target-setup", ctx.Err(), context.Cause(ctx))
				}
			}
			if err := existing.work.Err(); err != nil {
				return nil, failure(ErrState, "target-state", err, context.Cause(existing.work))
			}
			return existing, nil
		}
	}
	if len(retained.pages) >= root.owner.settings.MaxPages {
		retained.mu.Unlock()
		return nil, failure(ErrLimit, "pages")
	}
	work, cancel := context.WithCancelCause(retained.life)
	page := &Session{sessionState: &sessionState{browserContext: root.browserContext, owner: root.owner, work: work, cancel: cancel, account: root.sessionState, targetID: identity, borrowedTarget: detach, events: make(chan Event, root.owner.settings.MaxEvents), setupDone: make(chan struct{})}}
	page.managedEvents = root.managedEvents
	retained.pages = append(retained.pages, page)
	retained.mu.Unlock()
	defer close(page.setupDone)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	defer stop()
	var err error
	if identity == "" {
		root.owner.targetAttempt(root.browserContext)
		var created target.CreateTargetResult
		created, err = cdp.Call(work, sdk.FromContext(root.owner.root).Browser, target.CreateTarget, target.CreateTargetParams{URL: "about:blank", BrowserContextID: root.browserID, NewWindow: new(root.owner.settings.NewWindow)})
		root.owner.targetCreated(root.browserContext, created.TargetID)
		identity = created.TargetID
		retained.mu.Lock()
		page.targetID = identity
		retained.mu.Unlock()
		if err == nil && identity == "" {
			err = failure(ErrNative, "missing-target-identity")
		}
	}
	if err == nil {
		options := []sdk.ContextOption{sdk.WithTargetID(identity), sdk.WithTargetCleanupTimeout(root.owner.settings.CleanupTimeout), sdk.WithManagedTargetLifetime()}
		if detach {
			options = append(options, sdk.WithDetachOnCancel())
		}
		page.native, _ = sdk.NewContext(context.WithoutCancel(root.owner.root), options...)
		err = sdk.Do(nativeContext{Context: work, native: page.native})
		if err == nil {
			page.startEvents()
		}
	}
	if err != nil {
		err = failure(ErrNative, "new-page", err)
	}
	page.remember(err)
	if err != nil {
		cancel(err)
	}
	return page, err
}

// Close stops admission and joins actual page work. A canceled wait leaves this
// same handle and receipt live. It never declares an active callback released.
func (retained *Context) Close(ctx context.Context) error {
	return retained.close(ctx, ctx)
}

func (retained *Context) close(ctx, cleanupCtx context.Context) error {
	if retained == nil || retained.contextState == nil || retained.session == nil || ctx == nil {
		return failure(ErrInput, "close-context")
	}
	select {
	case retained.gate <- struct{}{}:
		defer func() { <-retained.gate }()
	case <-ctx.Done():
		return failure(ErrCleanup, "close-context", ctx.Err(), context.Cause(ctx))
	}
	retained.mu.Lock()
	if retained.complete {
		err := retained.final
		retained.mu.Unlock()
		return err
	}
	retained.closing = true
	retained.cancel(context.Canceled)
	retained.session.mu.Lock()
	retained.session.contextClosed = true
	retained.session.mu.Unlock()
	pages := append([]*Session(nil), retained.pages...)
	commandBusy, commandIdle := retained.commandBusy, retained.commandIdle
	retained.mu.Unlock()
	if commandBusy {
		select {
		case <-commandIdle:
		case <-ctx.Done():
			return failure(ErrCleanup, "context-command-join", ctx.Err(), context.Cause(ctx))
		}
	}
	retained.session.remember(errors.Join(retained.session.owner.life.Err(), context.Cause(retained.session.owner.life)))
	var cleanup error
	for _, page := range pages {
		page.cancel(context.Canceled)
	}
	rootOwner := retained.session.owner
	// Binding construction is an admitted page operation. The snapshot below
	// must follow that join; session cancellation already seals its callbacks.
	for _, page := range pages {
		if err := page.stopAndWait(ctx); err != nil {
			return failure(ErrCleanup, "page-work", err)
		}
		if err := page.joinSubscriptions(ctx); err != nil {
			cleanup = errors.Join(cleanup, err)
			if ctx.Err() != nil {
				return failure(ErrCleanup, "subscription-work", cleanup)
			}
		}
		page.mu.Lock()
		control := page.control
		page.mu.Unlock()
		if control != nil {
			managed := &Control{state: control}
			if err := managed.Close(ctx); err != nil {
				cleanup = errors.Join(cleanup, err)
				if page.borrowedTarget && !managed.Closed() {
					return cleanup
				}
			}
		}
	}
	rootOwner.mu.Lock()
	bindings := make([]*managedBinding, 0, len(retained.session.browserContext.bindings))
	for binding := range retained.session.browserContext.bindings {
		bindings = append(bindings, binding)
	}
	rootOwner.mu.Unlock()
	for _, state := range bindings {
		binding := &Binding{state: state}
		if err := binding.Close(ctx); err != nil {
			cleanup = errors.Join(cleanup, err)
			if state.session.borrowedTarget && !binding.Closed() {
				return cleanup
			}
		}
	}
	for _, page := range pages {
		if page.native != nil {
			if native := sdk.FromContext(page.native).Target; native != nil {
				if err := native.JoinBindings(ctx); err != nil {
					return failure(ErrCleanup, "bindings", err)
				}
			}
		}
	}
	rootOwner.mu.Lock()
	objects := make([]*objectGroupState, 0, len(retained.session.browserContext.objectGroups))
	for group := range retained.session.browserContext.objectGroups {
		objects = append(objects, group)
	}
	rootOwner.mu.Unlock()
	for _, state := range objects {
		cleanup = errors.Join(cleanup, (&ObjectGroup{state: state}).Close(cleanupCtx))
	}
	rootOwner.mu.Lock()
	streams := make([]*streamState, 0, len(retained.session.browserContext.streams))
	for stream := range retained.session.browserContext.streams {
		streams = append(streams, stream)
	}
	rootOwner.mu.Unlock()
	for _, state := range streams {
		stream := &Stream{state: state}
		work, done, err := (invocation.Budget{Limit: rootOwner.settings.CleanupTimeout}).Context(cleanupCtx, invocation.Cleanup)
		if err == nil {
			err = stream.Close(work)
			done()
		}
		if err != nil {
			if state.session.borrowedTarget && !stream.Closed() {
				return err
			}
			cleanup = errors.Join(cleanup, err)
		}
	}
	for _, borrowed := range []bool{true, false} {
		for _, page := range pages {
			if page.borrowedTarget != borrowed {
				continue
			}
			joined, err := page.closeNative(ctx)
			cleanup = errors.Join(cleanup, err)
			if !joined {
				return failure(ErrCleanup, "native-target-join", cleanup)
			}
		}
	}
	root := retained.session
	if root.reserved {
		work, done, err := (invocation.Budget{Limit: root.owner.settings.CleanupTimeout}).Context(cleanupCtx, invocation.Cleanup)
		if err != nil {
			cleanup = errors.Join(cleanup, err)
		} else {
			released, err := root.owner.dispose(work, root.browserContext)
			if root.borrowed {
				root.data.contextDetached = released
			} else {
				root.data.contextReleased = released
			}
			cleanup = errors.Join(cleanup, err)
			done()
		}
	}
	root.mu.Lock()
	if root.data.contextReleased {
		for _, state := range streams {
			state.mu.Lock()
			state.closed = true
			state.mu.Unlock()
		}
	}
	root.mu.Unlock()
	for _, state := range streams {
		select {
		case state.gate <- struct{}{}:
			<-state.gate
		case <-ctx.Done():
			return failure(ErrCleanup, "stream-work", ctx.Err(), context.Cause(ctx))
		}
	}
	root.mu.Lock()
	if root.data.contextReleased {
		root.owner.mu.Lock()
		clear(root.browserContext.streams)
		root.owner.mu.Unlock()
	}
	data, primary := root.data, root.primary
	cleanup = errors.Join(cleanup, root.cleanup)
	root.mu.Unlock()
	if data.contextReleased || data.contextDetached {
		for _, page := range pages {
			if !data.contextReleased && page.borrowedTarget {
				continue
			}
			page.mu.Lock()
			control := page.control
			page.mu.Unlock()
			if control != nil {
				if err := control.disposed(ctx); err != nil {
					cleanup = errors.Join(cleanup, err)
					if !(&Control{state: control}).Closed() {
						return cleanup
					}
				}
			}
		}
		for _, state := range bindings {
			binding := &Binding{state: state}
			if err := binding.Close(ctx); err != nil && !binding.Closed() {
				return errors.Join(cleanup, err)
			}
		}
	}
	for _, state := range objects {
		group := &ObjectGroup{state: state}
		cleanup = errors.Join(cleanup, group.Close(ctx))
		if !group.Closed() {
			if !state.transferToSource() {
				return failure(ErrCleanup, "object-work", cleanup)
			}
		}
	}
	if primary != nil {
		primary = failure(ErrNative, "session", primary)
	}
	if cleanup != nil {
		cleanup = failure(ErrCleanup, "session", cleanup)
	}
	retained.stop()
	retained.call.Complete(invocation.Outcome[Result]{Value: Result{data: &data}, Present: true, Primary: primary, Cleanup: cleanup})
	retained.mu.Lock()
	retained.complete = true
	retained.final = errors.Join(primary, cleanup)
	retained.mu.Unlock()
	return retained.final
}

func (session *Session) stopAndWait(ctx context.Context) error {
	session.cancel(context.Canceled)
	if session.setupDone != nil {
		select {
		case <-session.setupDone:
		case <-ctx.Done():
			return errors.Join(ctx.Err(), context.Cause(ctx))
		}
	}
	session.mu.Lock()
	session.closed = true
	busy, idle, navigating, navigationIdle := session.busy, session.idle, session.navigating, session.navigationIdle
	session.mu.Unlock()
	session.cancel(context.Canceled)
	for _, wait := range []struct {
		needed bool
		done   <-chan struct{}
	}{{busy, idle}, {navigating, navigationIdle}, {session.eventsDone != nil, session.eventsDone}} {
		if wait.needed {
			select {
			case <-wait.done:
			case <-ctx.Done():
				return errors.Join(ctx.Err(), context.Cause(ctx))
			}
		}
	}
	session.mu.Lock()
	origin := session.objectOrigin
	session.mu.Unlock()
	if origin != nil {
		select {
		case <-origin.done:
		case <-ctx.Done():
			return errors.Join(ctx.Err(), context.Cause(ctx))
		}
	}
	return nil
}

func (session *Session) accounting() *sessionState {
	if session.account != nil {
		return session.account
	}
	return session.sessionState
}

// Losing a connection alone proves nothing. Only owned-process exit or confirmed
// source reconciliation of an owned context/page can end uncertain native custody.
func (session *Session) nativeResourcesGone() bool {
	session.mu.Lock()
	gone := session.targetGone
	session.mu.Unlock()
	if gone {
		return true
	}
	if done := session.owner.processDone; done != nil {
		select {
		case <-done:
			return true
		default:
		}
	}
	if !session.accounting().reserved || session.borrowed && session.borrowedTarget {
		return false
	}
	session.owner.mu.Lock()
	defer session.owner.mu.Unlock()
	_, pending := session.owner.sessions[session.browserContext]
	return !pending
}
