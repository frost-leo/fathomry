/**
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
	"reflect"
	"strings"
	"sync"

	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
	jsonv2 "github.com/go-json-experiment/json"
)

// Session is a callback-scoped, non-owning capability. One Actions group may run
// alongside one Navigate and event reception, so paused requests can be continued.
// Same-kind overlap is refused; callers must join their goroutines before return.
// Copies share the same state and allowances. No native owning handles are exposed.
type Session struct {
	private
	*sessionState
}

type sessionState struct {
	*browserContext
	navigating                   bool
	navigationIdle               chan struct{}
	owner                        *owner
	work                         context.Context
	cancel                       context.CancelCauseFunc
	native                       context.Context
	closeTab                     context.CancelFunc
	stopTab                      func() bool
	reserved                     bool
	mu                           sync.Mutex
	closed, busy                 bool
	idle                         chan struct{}
	primary                      error
	data                         resultData
	delivered, saved, eventBytes int64
	events                       chan Event
}

// Context carries cancellation/deadline only, without caller/native values.
// It is valid only within Run; its cancellation is not a completion receipt.
func (session *Session) Context() context.Context {
	if session == nil || session.sessionState == nil || session.work == nil {
		return nil
	}
	return bareContext{session.work}
}
func (session *Session) enter(ctx context.Context, navigation bool) (context.Context, func(), error) {
	if session == nil || session.sessionState == nil || session.owner == nil || ctx == nil {
		return nil, nil, failure(ErrInput, "session")
	}
	session.mu.Lock()
	if session.closed || (!navigation && session.busy) || (navigation && session.navigating) || session.work == nil || session.native == nil {
		session.mu.Unlock()
		return nil, nil, failure(ErrState, "session")
	}
	if err := ctx.Err(); err != nil {
		session.mu.Unlock()
		return nil, nil, failure(ErrState, "context", err, context.Cause(ctx))
	}
	if err := session.work.Err(); err != nil {
		session.mu.Unlock()
		return nil, nil, failure(ErrState, "context", err, context.Cause(session.work))
	}
	if navigation {
		session.navigating = true
		session.navigationIdle = make(chan struct{})
	} else {
		session.busy = true
		session.idle = make(chan struct{})
	}
	session.mu.Unlock()
	work, cancel := context.WithCancelCause(session.work)
	deadlineCancel := func() {}
	if deadline, ok := ctx.Deadline(); ok {
		work, deadlineCancel = context.WithDeadline(work, deadline)
	}
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	return work, func() {
		stop()
		deadlineCancel()
		cancel(context.Canceled)
		session.mu.Lock()
		if navigation {
			session.navigating = false
			close(session.navigationIdle)
		} else {
			session.busy = false
			close(session.idle)
		}
		session.mu.Unlock()
	}, nil
}
func (session *Session) remember(err error) {
	if err == nil || session == nil || session.sessionState == nil {
		return
	}
	session.mu.Lock()
	if session.primary == nil {
		session.primary = err
	}
	session.mu.Unlock()
}
func (session *Session) stopAndJoin() {
	session.mu.Lock()
	session.closed = true
	idle, busy := session.idle, session.busy
	navigationIdle, navigating := session.navigationIdle, session.navigating
	session.mu.Unlock()
	session.cancel(context.Canceled)
	if busy {
		<-idle
	}
	if navigating {
		<-navigationIdle
	}
	if session.stopTab != nil {
		session.stopTab()
	}
}

// Navigate waits for the native page-load action. It neither certifies HTTP
// success nor waits for every subresource, worker or background request.
// Read native response events and body bytes separately when needed.
func (session *Session) Navigate(ctx context.Context, address string) error {
	work, leave, err := session.enter(ctx, true)
	if err != nil {
		session.remember(err)
		return err
	}
	defer leave()
	if address == "" || int64(len(address)) > session.owner.settings.MaxCommandBytes || strings.ContainsRune(address, '\x00') {
		err = failure(ErrInput, "navigation")
	} else {
		native := nativeContext{Context: work, native: session.native}
		err = sdk.Run(native, sdk.Navigate(address))
		if err == nil {
			session.mu.Lock()
			session.data.navigations++
			session.mu.Unlock()
		}
	}
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), work.Err(), context.Cause(work))
	if err != nil {
		err = failure(ErrNative, "navigation", err)
	}
	session.remember(err)
	return err
}

type nativeContext struct {
	context.Context
	native context.Context
}

func (ctx nativeContext) Value(key any) any { return ctx.native.Value(key) }

// Save copies explicit evidence. Names are non-secret labels (at most 64 ASCII
// lowercase letters/digits/.-_); duplicates are refused. Empty bytes are present.
// The caller defines their meaning; page load never proves data completeness.
func (session *Session) Save(name string, data []byte) error {
	if session == nil || session.sessionState == nil || session.owner == nil {
		return failure(ErrInput, "save")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	var err error
	switch {
	case session.closed || session.work == nil:
		err = failure(ErrState, "save")
	case session.work.Err() != nil:
		err = failure(ErrState, "save", session.work.Err(), context.Cause(session.work))
	case name == "" || !(fault.Context{Operation: name}).Valid():
		err = failure(ErrInput, "output-name")
	case len(session.data.output) >= 128 || int64(len(name)+len(data)) > session.owner.settings.MaxResultBytes-session.saved:
		err = failure(ErrLimit, "saved-output")
	default:
		if _, exists := session.data.output[name]; exists {
			err = failure(ErrInput, "duplicate-output")
		} else {
			session.data.output[name] = append([]byte{}, data...)
			session.saved += int64(len(name) + len(data))
		}
	}
	if err != nil && session.primary == nil {
		session.primary = err
	}
	return err
}

// NextEvent waits for a bounded native event without user callbacks on the CDP
// reader. Overflow cancels the session and remains evidence even when ignored.
// Events consume the cumulative delivered-result budget as well as queue capacity.
func (session *Session) NextEvent(ctx context.Context) (Event, error) {
	if session == nil || session.sessionState == nil || session.owner == nil || session.work == nil || ctx == nil {
		return Event{}, failure(ErrInput, "event")
	}
	session.mu.Lock()
	closed := session.closed
	session.mu.Unlock()
	if closed {
		return Event{}, failure(ErrState, "event")
	}
	if err := ctx.Err(); err != nil {
		return Event{}, failure(ErrState, "event", err, context.Cause(ctx))
	}
	select {
	case <-ctx.Done():
		return Event{}, failure(ErrState, "event", ctx.Err(), context.Cause(ctx))
	case <-session.work.Done():
		return Event{}, failure(ErrState, "event", session.work.Err(), context.Cause(session.work))
	case event := <-session.events:
		session.mu.Lock()
		session.eventBytes -= int64(len(event.data))
		if int64(len(event.data)) > session.owner.settings.MaxResultBytes-session.delivered {
			err := failure(ErrLimit, "event-output")
			if session.primary == nil {
				session.primary = err
			}
			session.mu.Unlock()
			session.cancel(err)
			return Event{}, err
		}
		session.delivered += int64(len(event.data))
		session.mu.Unlock()
		return event, nil
	}
}

func (session *Session) observe(value any) {
	kind := reflect.TypeOf(value)
	if kind == nil || kind.Kind() != reflect.Pointer || !strings.HasPrefix(kind.Elem().Name(), "Event") {
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return
	}
	if _, ok := value.(*network.EventRequestWillBeSent); ok {
		session.data.requestEvents++
	}
	buffer := boundedBuffer{limit: session.owner.settings.MaxEventBytes - session.eventBytes}
	err := jsonv2.MarshalWrite(&buffer, value, sdk.DefaultMarshalOptions)
	if err == nil {
		event := Event{name: kind.String(), data: buffer.data}
		select {
		case session.events <- event:
			session.eventBytes += int64(len(event.data))
			return
		default:
		}
	}
	session.data.eventOverflow = true
	cause := failure(ErrLimit, "event-queue", err)
	if session.primary == nil {
		session.primary = cause
	}
	session.cancel(cause)
}

type boundedBuffer struct {
	data  []byte
	limit int64
}

func (buffer *boundedBuffer) Write(data []byte) (int, error) {
	if int64(len(data)) > buffer.limit-int64(len(buffer.data)) {
		return 0, failure(ErrLimit, "encoded-bytes")
	}
	buffer.data = append(buffer.data, data...)
	return len(data), nil
}
