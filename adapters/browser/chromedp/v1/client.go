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
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Client is non-owning. Follow selects a generation for each new retention only.
type Client struct {
	private
	endpoint adapters.Endpoint[Result]
	direct   Handle
	source   *resource.Ref[Handle]
	lifetime context.Context
	budget   browser.Budget
	id       string
}

func Using(lifetime context.Context, ref resource.Ref[Handle], budget browser.Budget, d Dependencies) (*Client, error) {
	if lifetime == nil || budget.WorkBytes < 1 || budget.EvidenceBytes < 1 {
		return nil, fail(ErrInput, "using")
	}
	if _, err := ref.Inspect(); err != nil {
		return nil, err
	}
	endpoint, err := bind(d)
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint, source: &ref, lifetime: lifetime, budget: budget}, nil
}
func (c *Client) WithID(id string) (*Client, error) {
	if c == nil || c.lifetime == nil || len(id) > 256 || !utf8.ValidString(id) || strings.ContainsFunc(id, func(r rune) bool { return r < 32 || r == 127 }) {
		return nil, fail(ErrInput, "id")
	}
	copy := *c
	copy.id = strings.Clone(id)
	return &copy, nil
}

// ContextOptions names an explicit isolation or borrow choice.
// Borrowing shared/default storage does not promise exclusive cookie isolation.
type ContextOptions struct {
	ProxyServer     string
	ProxyBypassList string
	Borrow          bool
	ContextID       string
	TargetID        string
	DefaultContext  bool
	// SharedStorage explicitly grants context-wide storage access when Borrow is true.
	SharedStorage bool
	// SharedPermissions accepts context-wide permission changes and the native
	// reset of all overrides in modified contexts when the control session closes.
	SharedPermissions bool
	// SharedDownloads accepts context-wide download policy changes and reset.
	// It does not grant Go-local access to files on an external browser host.
	SharedDownloads bool
}

// Context retains the source, admission and cleanup of all its pages.
type Context struct {
	private
	*contextState
}

type contextState struct {
	native         *native.Context
	endpoint       adapters.Endpoint[Result]
	call           *adapters.Call[Result]
	guard          adapters.Guard
	record         *invocation.DeliveryRecord[native.Result]
	stop           func()
	budget         browser.Budget
	id             string
	gate           chan struct{}
	mu             sync.Mutex
	complete       bool
	final          error
	callbackError  error
	cleanupError   error
	callbackDone   chan struct{}
	source         *sourceState
	closing        bool
	operations     int
	operationsIdle chan struct{}
	streams        map[*streamState]struct{}
	subscriptions  map[*subscriptionState]struct{}
	controls       map[*controlState]struct{}
	bindings       map[*bindingState]struct{}
	objectGroups   map[*objectGroupState]struct{}
	pages          map[*native.Session]*pageState
}
type Page struct {
	private
	context *Context
	native  *native.Session
	state   *pageState
}

// Scope is non-owning callback access. Only Retain returns cleanup authority.
type Scope struct {
	private
	retained *Context
}

func (s *Scope) Context() context.Context {
	if s == nil || s.retained == nil {
		return nil
	}
	return s.retained.Context()
}
func (s *Scope) Page() *Page {
	if s == nil || s.retained == nil {
		return nil
	}
	return s.retained.Page()
}
func (s *Scope) NewPage(ctx context.Context) (*Page, *adapters.Receipt[Result], error) {
	if s == nil || s.retained == nil {
		return nil, nil, fail(ErrInput, "scope")
	}
	return s.retained.NewPage(ctx)
}

func (c *Context) beginOperation() (func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.complete {
		return nil, fail(ErrState, "context")
	}
	if c.operations == 0 {
		c.operationsIdle = make(chan struct{})
	}
	c.operations++
	return sync.OnceFunc(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.operations--
		if c.operations == 0 {
			close(c.operationsIdle)
		}
	}), nil
}

func (c *Client) Retain(ctx context.Context, options ContextOptions) (*Context, *adapters.Receipt[Result], error) {
	return c.retain(ctx, options, false)
}

func (c *Client) retain(ctx context.Context, options ContextOptions, callback bool) (*Context, *adapters.Receipt[Result], error) {
	if c == nil || c.lifetime == nil || ctx == nil {
		return nil, nil, fail(ErrInput, "retain")
	}
	live, cancel := context.WithCancelCause(ctx)
	stopClient := context.AfterFunc(c.lifetime, func() { cancel(context.Cause(c.lifetime)) })
	stop := sync.OnceFunc(func() { stopClient(); cancel(nil) })
	transferred := false
	defer func() {
		if !transferred {
			stop()
		}
	}()
	var retained *Context
	var primary error
	run := func(call *adapters.Call[Result], handle Handle) {
		reject := func(err error) {
			primary = translate(err, "retain")
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
		}
		s := handle.state
		if s == nil {
			reject(fail(ErrState, "source"))
			return
		}
		if s.prepared.policy.Budget.WorkBytes > c.budget.WorkBytes || s.prepared.policy.Budget.EvidenceBytes > c.budget.EvidenceBytes {
			reject(fail(ErrLimit, "generation-budget"))
			return
		}
		release, err := s.use()
		if err != nil {
			reject(err)
			return
		}
		guard, err := call.Hold()
		if err != nil {
			release()
			reject(err)
			return
		}
		owned, stopOwned := context.WithCancelCause(call.Context())
		stopSource := context.AfterFunc(s.call.Context(), func() { stopOwned(context.Cause(s.call.Context())) })
		finish := sync.OnceFunc(func() { stopSource(); stopOwned(nil); stop(); release() })
		inbox, err := invocation.NewInbox[native.Result](1, s.prepared.native.Budget().EvidenceBytes)
		var client *native.Client
		if err == nil {
			client, err = native.Bind(s.assembly, s.selected, inbox, nil)
		}
		if err != nil {
			finish()
			reject(err)
			_ = guard.Release()
			return
		}
		info, _ := call.Receipt().Snapshot()
		contextHandle, err := client.Retain(owned, fault.Correlation{Call: "browser-" + strconv.FormatUint(info.Info().Sequence, 10)}, contextOptions(options))
		primary = translate(err, "retain")
		if contextHandle == nil {
			finish()
			reject(err)
			_ = guard.Release()
			return
		}
		record, claimErr := inbox.Next(context.Background())
		if claimErr != nil {
			_ = contextHandle.Close(context.Background())
			finish()
			reject(claimErr)
			_ = guard.Release()
			return
		}
		retained = &Context{contextState: &contextState{native: contextHandle, endpoint: c.endpoint, call: call, guard: guard, record: record, stop: finish, budget: c.budget, id: c.id, gate: make(chan struct{}, 1)}}
		transferred = true
		retained.source = s
		if callback {
			retained.callbackDone = make(chan struct{})
		}
		s.mu.Lock()
		if s.contexts == nil {
			s.contexts = make(map[*contextState]struct{})
		}
		s.contexts[retained.contextState] = struct{}{}
		s.mu.Unlock()
		context.AfterFunc(owned, func() { _ = retained.Close(context.Background()) })
	}
	req := request("retain", c.id, c.budget.WorkBytes, c.budget.EvidenceBytes)
	var receipt *adapters.Receipt[Result]
	var err error
	if c.source != nil {
		receipt, err = adapters.UsingWithLifetime(ctx, live, c.endpoint, *c.source, req, run)
	} else {
		receipt, err = c.endpoint.RunWithLifetime(ctx, live, req, func(call *adapters.Call[Result]) { run(call, c.direct) })
	}
	return retained, receipt, errors.Join(err, receiptError(receipt, primary))
}
func (c *Context) Page() *Page {
	if c == nil || c.contextState == nil || c.native == nil {
		return nil
	}
	return c.page(c.native.Page())
}

// Context carries cancellation only. It cannot recover native ownership.
func (c *Context) Context() context.Context {
	if c == nil || c.contextState == nil || c.native == nil {
		return nil
	}
	return c.native.Context()
}
func (c *Context) NewPage(ctx context.Context) (*Page, *adapters.Receipt[Result], error) {
	if c == nil || c.contextState == nil || ctx == nil {
		return nil, nil, fail(ErrInput, "page")
	}
	var page *Page
	var primary error
	receipt, err := c.endpoint.Child(ctx, c.call.Scope(), request("page", c.id, 0, c.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := c.beginOperation()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		defer finish()
		nativePage, e := c.native.NewPage(call.Context())
		primary = translate(e, "page")
		if nativePage != nil {
			page = c.page(nativePage)
		}
		info, _ := call.Receipt().Snapshot()
		_ = call.Resolve(adapters.Outcome[Result]{Value: Result{attribution: attribution(info.Info()), observation: browser.Observation{Returned: e == nil, Effect: browser.Unknown}}, Present: true, Primary: primary})
	})
	return page, receipt, errors.Join(err, receiptError(receipt, primary))
}
func (c *Context) Close(ctx context.Context) error {
	if c == nil || c.contextState == nil || c.native == nil || ctx == nil {
		return fail(ErrInput, "close-context")
	}
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "close")
	}
	c.mu.Lock()
	if c.complete {
		err := c.final
		c.mu.Unlock()
		return err
	}
	c.closing = true
	active, idle := c.operations, c.operationsIdle
	c.mu.Unlock()
	_ = c.call.Cancel(nil)
	cleanup := c.native.Close(ctx)
	if snapshot, ok := c.native.Receipt().Result(); !ok || !snapshot.Released {
		return translate(errors.Join(cleanup, fail(ErrCleanup, "context-incomplete")), "close")
	}
	nativeResult, err := c.native.Receipt().WaitReleased(ctx)
	if err != nil {
		return translate(errors.Join(err, cleanup), "close")
	}
	if active != 0 {
		select {
		case <-idle:
		case <-ctx.Done():
			return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "operation-join")
		}
	}
	c.mu.Lock()
	streams := make([]*streamState, 0, len(c.streams))
	for state := range c.streams {
		streams = append(streams, state)
	}
	c.mu.Unlock()
	for _, state := range streams {
		stream := &Stream{state: state}
		if err := stream.Close(ctx); err != nil && !stream.Closed() {
			return err
		}
	}
	c.mu.Lock()
	subscriptions := make([]*subscriptionState, 0, len(c.subscriptions))
	for state := range c.subscriptions {
		subscriptions = append(subscriptions, state)
	}
	c.mu.Unlock()
	for _, state := range subscriptions {
		if err := state.close(ctx); err != nil && !state.closed() {
			return err
		}
	}
	c.mu.Lock()
	controls := make([]*controlState, 0, len(c.controls))
	for state := range c.controls {
		controls = append(controls, state)
	}
	c.mu.Unlock()
	for _, state := range controls {
		control := &Control{state: state}
		if err := control.Close(ctx); err != nil && !control.Closed() {
			return err
		}
	}
	c.mu.Lock()
	bindings := make([]*bindingState, 0, len(c.bindings))
	for state := range c.bindings {
		bindings = append(bindings, state)
	}
	c.mu.Unlock()
	for _, state := range bindings {
		binding := &Binding{state: state}
		if err := binding.Close(ctx); err != nil && !binding.Closed() {
			return err
		}
	}
	c.mu.Lock()
	objectGroups := make([]*objectGroupState, 0, len(c.objectGroups))
	for state := range c.objectGroups {
		objectGroups = append(objectGroups, state)
	}
	c.mu.Unlock()
	for _, state := range objectGroups {
		group := &ObjectGroup{state: state}
		if err := group.Close(ctx); err != nil && !group.Closed() {
			return err
		}
	}
	if c.callbackDone != nil {
		select {
		case <-c.callbackDone:
		case <-ctx.Done():
			return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "callback-join")
		}
	}
	info, _ := c.call.Receipt().Snapshot()
	value := Result{native: nativeResult.Outcome.Value, attribution: attribution(info.Info()), observation: browser.Observation{Returned: nativeResult.Outcome.Primary == nil, ContextDisposed: nativeResult.Outcome.Value.ContextReleased(), ContextDetached: nativeResult.Outcome.Value.ContextDetached()}}
	primary := translate(nativeResult.Outcome.Primary, "operation")
	c.mu.Lock()
	primary = errors.Join(primary, c.callbackError)
	c.mu.Unlock()
	c.mu.Lock()
	cleanup = errors.Join(translate(nativeResult.Outcome.Cleanup, "cleanup"), c.cleanupError)
	c.mu.Unlock()
	if err := c.call.Resolve(adapters.Outcome[Result]{Value: value, Present: true, Primary: primary, Cleanup: cleanup}); err != nil {
		return err
	}
	if err := c.record.Release(); err != nil {
		return translate(err, "evidence")
	}
	c.mu.Lock()
	c.final = errors.Join(primary, cleanup)
	c.complete = true
	c.mu.Unlock()
	if c.source != nil {
		c.source.mu.Lock()
		delete(c.source.contexts, c.contextState)
		c.source.mu.Unlock()
	}
	c.stop()
	_ = c.guard.Release()
	return c.final
}

// Run is the fresh-context convenience over Retain, including the same evidence.
func (c *Client) Run(ctx context.Context, options ContextOptions, work func(*Scope) error) (receipt *adapters.Receipt[Result], err error) {
	if work == nil {
		return nil, fail(ErrInput, "run")
	}
	retained, receipt, err := c.retain(ctx, options, true)
	if retained == nil {
		return receipt, err
	}
	returned := false
	defer func() {
		if value := recover(); value != nil {
			cause, _ := value.(error)
			err = fail(ErrOperation, "callback-panic", cause)
		} else if !returned {
			err = fail(ErrOperation, "callback-did-not-return")
		}
		err = translate(err, "callback")
		retained.mu.Lock()
		retained.callbackError = err
		retained.mu.Unlock()
		close(retained.callbackDone)
		err = errors.Join(err, retained.Close(context.Background()))
	}()
	if err != nil {
		returned = true
		return receipt, err
	}
	err = work(&Scope{retained: retained})
	returned = true
	return receipt, err
}
