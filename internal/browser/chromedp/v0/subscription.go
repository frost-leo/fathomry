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
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/log"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

// Subscription owns one bounded native queue. It starts at construction, even
// when never read. Cancellation stops and joins the producer independently of Next.
type Subscription struct {
	private
	state *subscriptionState
}

type subscriptionState struct {
	session     *Session
	methods     []string
	console     bool
	kind        reflect.Type
	life        context.Context
	cancel      context.CancelCauseFunc
	raw         <-chan jsonv2.Value
	unsubscribe func()
	stopCaller  func() bool
	done        chan struct{}
	once        sync.Once
	mu          sync.Mutex
	reading     bool
	readDone    chan struct{}
	closed      bool
	primary     error
}

// SubscribeEvent accepts only an exact selected native event descriptor.
// Browser-wide discovery and owner events require their separate owner route.
func SubscribeEvent[E any](ctx context.Context, session *Session, event cdp.Event[E]) (*Subscription, error) {
	kind, err := eventType[E](event)
	if err != nil {
		return nil, err
	}
	return subscribeEvents(ctx, session, kind, false, event.Method)
}

// SubscribeConsole keeps native console, exception and log order in one queue.
func SubscribeConsole(ctx context.Context, session *Session) (*Subscription, error) {
	return subscribeEvents(ctx, session, reflect.TypeFor[sdk.ConsoleMessage](), true, runtime.ConsoleAPICalled.Method, runtime.ExceptionThrown.Method, log.EntryAdded.Method)
}

func subscribeEvents(ctx context.Context, session *Session, kind reflect.Type, console bool, methods ...string) (*Subscription, error) {
	if ctx == nil || session == nil || session.sessionState == nil || session.owner == nil {
		return nil, failure(ErrInput, "subscribe")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.work == nil || session.native == nil || session.work.Err() != nil || ctx.Err() != nil {
		return nil, failure(ErrState, "subscribe")
	}
	target := sdk.FromContext(session.native)
	if target == nil || target.Target == nil {
		return nil, failure(ErrState, "subscribe-target")
	}
	own := session.owner
	own.mu.Lock()
	if len(session.browserContext.subscriptions) >= own.settings.MaxSubscriptions {
		own.mu.Unlock()
		return nil, failure(ErrLimit, "subscriptions")
	}
	life, cancel := context.WithCancelCause(session.work)
	state := &subscriptionState{session: session, methods: slices.Clone(methods), console: console, kind: kind, life: life, cancel: cancel, done: make(chan struct{})}
	if session.browserContext.subscriptions == nil {
		session.browserContext.subscriptions = make(map[*subscriptionState]struct{})
	}
	session.browserContext.subscriptions[state] = struct{}{}
	own.mu.Unlock()
	state.raw, state.unsubscribe = target.Target.SubscribeBounded(sdk.EventBuffer{
		MaxEvents: own.settings.MaxEvents, MaxBytes: own.settings.MaxEventBytes,
		OnOverflow: func(err error) {
			state.record(failure(ErrLimit, "subscription-overflow", err))
			cancel(err)
		},
	}, methods...)
	state.stopCaller = context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	context.AfterFunc(life, state.finish)
	return &Subscription{state: state}, nil
}

func eventType[E any](event cdp.Event[E]) (reflect.Type, error) {
	domain, _, ok := strings.Cut(event.Method, ".")
	if !ok || len(event.Method) > 128 {
		return nil, failure(ErrInput, "event-method")
	}
	if domain == "Browser" || domain == "Target" || domain == "SystemInfo" {
		return nil, failure(ErrUnsupported, "owner-event")
	}
	sample, err := cdproto.UnmarshalMessage(&cdproto.Message{Method: cdproto.MethodType(event.Method), Params: jsonv2.Value(`{}`)}, sdk.DefaultUnmarshalOptions)
	kind := reflect.TypeFor[E]()
	if err != nil || kind.Kind() != reflect.Struct || !strings.HasPrefix(kind.Name(), "Event") || reflect.TypeOf(sample) != reflect.PointerTo(kind) {
		return nil, failure(ErrInput, "event-type")
	}
	return kind, nil
}

// ReceiveEvent returns native data and independent frozen bytes. Only one reader
// is admitted at a time. The exact native schema is checked before decoding.
func ReceiveEvent[E any](ctx context.Context, subscription *Subscription) (value E, evidence Event, err error) {
	if ctx == nil || subscription == nil || subscription.state == nil {
		return value, evidence, failure(ErrInput, "event")
	}
	state := subscription.state
	state.mu.Lock()
	if state.closed || state.reading || state.kind != reflect.TypeFor[E]() {
		state.mu.Unlock()
		return value, evidence, failure(ErrState, "event-reader")
	}
	state.reading = true
	state.readDone = make(chan struct{})
	state.mu.Unlock()
	defer func() {
		state.mu.Lock()
		state.reading = false
		close(state.readDone)
		state.mu.Unlock()
	}()
	if err := ctx.Err(); err != nil {
		return value, evidence, failure(ErrState, "event", err, context.Cause(ctx))
	}
	var raw jsonv2.Value
	select {
	case <-ctx.Done():
		return value, evidence, failure(ErrState, "event", ctx.Err(), context.Cause(ctx))
	case <-state.life.Done():
		return value, evidence, failure(ErrState, "event", context.Cause(state.life), subscription.Err())
	case data, ok := <-state.raw:
		if !ok {
			return value, evidence, failure(ErrState, "event", subscription.Err())
		}
		raw = data
	}
	record := state.session.accounting()
	record.mu.Lock()
	if int64(len(raw)) > state.session.owner.settings.MaxResultBytes-record.delivered {
		record.mu.Unlock()
		err = failure(ErrLimit, "event-output")
		state.record(err)
		state.cancel(err)
		return value, evidence, err
	}
	record.delivered += int64(len(raw))
	record.mu.Unlock()
	var message cdproto.Message
	if err = jsonv2.Unmarshal(raw, &message, sdk.DefaultUnmarshalOptions); err == nil {
		if !slices.Contains(state.methods, string(message.Method)) {
			err = failure(ErrNative, "event-method")
		} else if state.console {
			var message sdk.ConsoleMessage
			message, err = sdk.DecodeConsole(raw)
			value = any(message).(E)
		} else {
			err = jsonv2.Unmarshal(message.Params, &value, sdk.DefaultUnmarshalOptions)
		}
	}
	evidence = Event{name: string(message.Method), data: append([]byte(nil), message.Params...)}
	if state.console {
		evidence.data = append([]byte(nil), raw...)
	}
	if err != nil {
		err = failure(ErrNative, "event-decode", err)
		state.record(err)
		state.cancel(err)
	}
	return value, evidence, err
}

func (state *subscriptionState) record(err error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.primary == nil {
		state.primary = err
	}
}
func (state *subscriptionState) finish() {
	state.once.Do(func() {
		state.mu.Lock()
		state.closed = true
		reading, idle := state.reading, state.readDone
		state.mu.Unlock()
		state.stopCaller()
		state.unsubscribe()
		if reading {
			<-idle
		}
		close(state.done)
	})
}
func (subscription *Subscription) Close(ctx context.Context) error {
	if subscription == nil || subscription.state == nil || ctx == nil {
		return failure(ErrInput, "subscription-close")
	}
	state := subscription.state
	state.cancel(context.Canceled)
	select {
	case <-state.done:
		state.session.owner.mu.Lock()
		delete(state.session.browserContext.subscriptions, state)
		state.session.owner.mu.Unlock()
		err := subscription.Err()
		state.session.remember(err)
		return nil
	case <-ctx.Done():
		return failure(ErrCleanup, "subscription-join", ctx.Err(), context.Cause(ctx))
	}
}
func (subscription *Subscription) Closed() bool {
	if subscription == nil || subscription.state == nil {
		return false
	}
	select {
	case <-subscription.state.done:
		return true
	default:
		return false
	}
}

// Done closes only after the native pump and active receiver have exited.
func (subscription *Subscription) Done() <-chan struct{} {
	if subscription == nil || subscription.state == nil {
		return nil
	}
	return subscription.state.done
}
func (subscription *Subscription) Err() error {
	if subscription == nil || subscription.state == nil {
		return failure(ErrInput, "subscription")
	}
	subscription.state.mu.Lock()
	defer subscription.state.mu.Unlock()
	return subscription.state.primary
}

// joinSubscriptions runs after page admission and work stop, before target detach.
func (session *Session) joinSubscriptions(ctx context.Context) error {
	session.owner.mu.Lock()
	var active []*subscriptionState
	for state := range session.browserContext.subscriptions {
		if state.session == session {
			active = append(active, state)
		}
	}
	session.owner.mu.Unlock()
	for _, state := range active {
		state.cancel(context.Canceled)
	}
	var joined error
	for _, state := range active {
		sub := &Subscription{state: state}
		joined = errors.Join(joined, sub.Close(ctx))
		if !sub.Closed() {
			return joined
		}
	}
	return joined
}
