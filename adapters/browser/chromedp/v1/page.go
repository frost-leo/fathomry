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

	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

type pageState struct {
	gate              chan struct{}
	closing, complete bool
	active            int
	idle              chan struct{}
	final             error
}

func (retained *Context) page(value *native.Session) *Page {
	retained.mu.Lock()
	defer retained.mu.Unlock()
	if retained.pages == nil {
		retained.pages = make(map[*native.Session]*pageState)
	}
	state := retained.pages[value]
	if state == nil {
		state = &pageState{gate: make(chan struct{}, 1), complete: value.PageClosed()}
		retained.pages[value] = state
	}
	return &Page{context: retained, native: value, state: state}
}

func (page *Page) beginOperation() (func(), error) {
	family := page.context
	finish, err := family.beginOperation()
	if err != nil {
		return nil, err
	}
	family.mu.Lock()
	state := page.state
	if state == nil || state.closing || state.complete {
		family.mu.Unlock()
		finish()
		return nil, fail(ErrState, "page")
	}
	if state.active == 0 {
		state.idle = make(chan struct{})
	}
	state.active++
	family.mu.Unlock()
	return func() {
		family.mu.Lock()
		state.active--
		if state.active == 0 {
			close(state.idle)
		}
		family.mu.Unlock()
		finish()
	}, nil
}

// Close seals one page, joins its operations and releases its native target or
// borrowed attachment. It uses the retained context's existing cleanup charge,
// not fresh Inbox admission. Other pages and context-scoped operations remain.
// Do not synchronously close a page from its own active handler or serializer.
func (page *Page) Close(ctx context.Context) (err error) {
	if page == nil || page.context == nil || page.state == nil || ctx == nil {
		return fail(ErrInput, "page-close")
	}
	family, state := page.context, page.state
	defer func() {
		if err != nil {
			family.mu.Lock()
			if family.cleanupError == nil {
				family.cleanupError = err
			}
			if state.final == nil {
				state.final = err
			}
			family.mu.Unlock()
		}
	}()
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "page-close")
	}
	family.mu.Lock()
	complete, final := state.complete, state.final
	family.mu.Unlock()
	if complete {
		return final
	}
	finish, err := family.beginOperation()
	if err != nil {
		return err
	}
	defer finish()
	family.mu.Lock()
	state.closing = true
	active, idle := state.active, state.idle
	family.mu.Unlock()
	cleanup := translate(family.native.ClosePage(ctx, page.native), "page-close")
	if !page.native.PageClosed() {
		return cleanup
	}
	if active != 0 {
		select {
		case <-idle:
		case <-ctx.Done():
			return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "page-work-join")
		}
	}
	family.mu.Lock()
	var streams []*streamState
	var subscriptions []*subscriptionState
	var controls []*controlState
	var bindings []*bindingState
	var objects []*objectGroupState
	for value := range family.streams {
		if value.page.native == page.native {
			streams = append(streams, value)
		}
	}
	for value := range family.subscriptions {
		if value.page.native == page.native {
			subscriptions = append(subscriptions, value)
		}
	}
	for value := range family.controls {
		if value.page.native == page.native {
			controls = append(controls, value)
		}
	}
	for value := range family.bindings {
		if value.page.native == page.native {
			bindings = append(bindings, value)
		}
	}
	for value := range family.objectGroups {
		if value.page.native == page.native {
			objects = append(objects, value)
		}
	}
	family.mu.Unlock()
	for _, value := range streams {
		stream := &Stream{state: value}
		cleanup = errors.Join(cleanup, stream.Close(ctx))
		if !stream.Closed() {
			return cleanup
		}
	}
	for _, value := range subscriptions {
		cleanup = errors.Join(cleanup, value.close(ctx))
		if !value.closed() {
			return cleanup
		}
	}
	for _, value := range controls {
		control := &Control{state: value}
		cleanup = errors.Join(cleanup, control.Close(ctx))
		if !control.Closed() {
			return cleanup
		}
	}
	for _, value := range bindings {
		binding := &Binding{state: value}
		cleanup = errors.Join(cleanup, binding.Close(ctx))
		if !binding.Closed() {
			return cleanup
		}
	}
	for _, value := range objects {
		group := &ObjectGroup{state: value}
		cleanup = errors.Join(cleanup, group.Close(ctx))
		if !group.Closed() {
			return cleanup
		}
	}
	family.mu.Lock()
	state.complete, state.final = true, errors.Join(state.final, cleanup)
	cleanup = state.final
	delete(family.pages, page.native)
	family.mu.Unlock()
	return cleanup
}

// Closed reports completion of this page's explicit Close, not browser exit.
func (page *Page) Closed() bool {
	if page == nil || page.context == nil || page.state == nil {
		return false
	}
	page.context.mu.Lock()
	defer page.context.mu.Unlock()
	return page.state.complete
}
