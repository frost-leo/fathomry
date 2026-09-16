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

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Source is a non-owning composition token, not an executable native handle.
type Source struct {
	private
	owner *owner
}

// Client is a concurrent, non-owning facade. Aliases share their authoritative
// source identity, process/connection, native context ceiling and admission policy.
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Select snapshots external configuration without starting Chrome. lifetime is
// explicitly caller-owned and must be cancellable. It must outlive all borrowing
// scopes; canceling it may terminate the owned process and this CDP connection,
// never an externally borrowed browser. Close the assembly even after cancellation.
func Select(lifetime context.Context, options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	if lifetime == nil || lifetime.Done() == nil {
		return resource.Selection[Source]{}, failure(ErrInput, "lifetime")
	}
	if err := lifetime.Err(); err != nil {
		return resource.Selection[Source]{}, failure(ErrState, "lifetime", err, context.Cause(lifetime))
	}
	version := options.Version
	if version == 0 {
		version = 1
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if err := ctx.Err(); err != nil {
			return resource.Resource[Source]{}, failure(ErrState, "construct", err, context.Cause(ctx))
		}
		if err := lifetime.Err(); err != nil {
			return resource.Resource[Source]{}, failure(ErrState, "lifetime", err, context.Cause(lifetime))
		}
		life, cancel := context.WithCancel(lifetime)
		own := &owner{settings: value, life: life, cancel: cancel, gate: make(chan struct{}, 1), sessions: make(map[*browserContext]struct{})}
		result := resource.Resource[Source]{Acquired: true, Capability: Source{owner: own}, Release: own.release}
		if value.CheckReady {
			result.Check = own.start
		}
		return result, nil
	}), nil
}

// Bind joins the authoritative resource record to a separately owned Inbox.
func Bind(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Client, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	limits, value := access.Limits(), source.owner.settings
	if limits.Active > value.MaxSessions || limits.Queued > value.QueuedCalls || limits.Bytes < value.reservation() || limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}
func (client *Client) EvidenceBytes() int64 {
	if client == nil || client.owner == nil {
		return 0
	}
	return client.owner.settings.evidenceBytes()
}

// Run synchronously executes one callback in a fresh isolated BrowserContext.
// cleanupCtx separately authorizes disposal, even after ctx cancellation. A non-nil
// receipt means accepted work: inspect its primary/cleanup errors independently.
// Callback inputs/outputs/errors are trusted borrowed values and must not retain
// owning handles. Callbacks must return cooperatively; Go code cannot be forcibly
// stopped. Same-kind operations and use after return are rejected; one navigation
// may overlap one Actions group and event reception for native interception.
//
// Unconfirmed native context cleanup is transferred to the same source owner and
// remains charged against MaxSessions until explicit assembly cleanup confirms it.
// Receipt release then describes this producer, not disposal of that context.
func (client *Client) Run(ctx, cleanupCtx context.Context, id fault.Correlation, run func(*Session) error, options ...SessionOptionsV1) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil || client.access == nil || ctx == nil || cleanupCtx == nil || run == nil {
		return nil, failure(ErrInput, "run")
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
	session := &Session{sessionState: &sessionState{browserContext: &browserContext{}, owner: client.owner, data: resultData{output: make(map[string][]byte)}, events: make(chan Event, value.MaxEvents)}}
	work, cancel, err := (invocation.Budget{Limit: value.SessionTimeout}).Context(ctx, invocation.Lifetime)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), err
	}
	session.work, session.cancel = context.WithCancelCause(work)
	defer cancel()
	stopLifetime := context.AfterFunc(client.owner.life, func() { session.cancel(context.Cause(client.owner.life)) })
	defer stopLifetime()
	primary := client.owner.start(session.work)
	if primary == nil {
		stopConnection := context.AfterFunc(client.owner.root, func() {
			if client.owner.life.Err() != nil {
				session.cancel(context.Cause(client.owner.life))
			} else {
				session.cancel(failure(ErrDisconnected, "connection", client.owner.root.Err(), context.Cause(client.owner.root)))
			}
		})
		defer stopConnection()
		primary = client.open(session, input)
	}
	if primary == nil {
		primary = invoke(func() error { return run(session) })
		session.mu.Lock()
		session.data.callbackCompleted = primary == nil && session.work.Err() == nil
		session.mu.Unlock()
	}
	primary = errors.Join(primary, session.work.Err(), context.Cause(session.work))
	session.stopAndJoin()
	var cleanupErr error
	if session.native != nil {
		cleanupErr = sdk.Cancel(session.native)
	}
	cleanup, done, err := (invocation.Budget{Limit: value.CleanupTimeout}).Context(cleanupCtx, invocation.Cleanup)
	if err != nil {
		cleanupErr = errors.Join(cleanupErr, err)
	} else {
		defer done()
		if session.reserved {
			released, err := client.owner.dispose(cleanup, session.browserContext)
			session.data.contextReleased = released
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	session.mu.Lock()
	primary = errors.Join(primary, session.primary)
	data := session.data
	session.mu.Unlock()
	if primary != nil {
		primary = failure(ErrNative, "session", primary)
	}
	if cleanupErr != nil {
		cleanupErr = failure(ErrCleanup, "session", cleanupErr)
	}
	call.Complete(invocation.Outcome[Result]{Value: Result{data: &data}, Present: true, Primary: primary, Cleanup: cleanupErr})
	return call.Receipt(), errors.Join(primary, cleanupErr)
}
func (client *Client) open(session *Session, input SessionOptionsV1) error {
	if err := session.work.Err(); err != nil {
		return err
	}
	if err := client.owner.reserve(session, client.access.Limits().Active); err != nil {
		return err
	}
	session.reserved = true
	own := client.owner
	executor := cdp.WithExecutor(session.work, sdk.FromContext(own.root).Browser)
	session.creationAttempted = true
	id, err := target.CreateBrowserContext().WithDisposeOnDetach(true).WithProxyServer(input.ProxyServer).WithProxyBypassList(input.ProxyBypassList).Do(executor)
	session.browserID = id
	if err != nil {
		return err
	}
	tabID, err := target.CreateTarget("about:blank").WithBrowserContextID(id).WithNewWindow(own.settings.NewWindow).Do(executor)
	if err != nil {
		return err
	}
	// Initialization mutates native Context fields. Do not race its cancellation
	// handler against attachment; the operation budget still governs SDK commands.
	session.native, session.closeTab = sdk.NewContext(context.WithoutCancel(own.root), sdk.WithTargetID(tabID))
	if err := sdk.Run(nativeContext{Context: session.work, native: session.native}); err != nil {
		return err
	}
	session.stopTab = context.AfterFunc(session.work, func() { session.closeTab() })
	sdk.ListenTarget(session.native, session.observe)
	return nil
}

func invoke(callback func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			cause, _ := value.(error)
			err = failure(ErrPanic, "callback", cause)
		}
	}()
	return callback()
}
