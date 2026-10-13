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
	"slices"

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
	prepared, err := PrepareV1(options, layers...)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return prepared.Select(lifetime)
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
func (client *Client) Run(ctx, cleanupCtx context.Context, id fault.Correlation, run func(*Session) error, options ...SessionOptionsV1) (receipt *invocation.Receipt[Result], err error) {
	if cleanupCtx == nil || run == nil {
		return nil, failure(ErrInput, "run")
	}
	retained, err := client.Retain(ctx, id, options...)
	if retained == nil {
		return nil, err
	}
	session := retained.Page()
	returned := false
	defer func() {
		if !returned {
			session.remember(failure(ErrPanic, "callback-did-not-return"))
			_ = retained.Close(context.Background())
		}
	}()
	if err == nil {
		err = invoke(func() error { return run(session) })
		session.mu.Lock()
		session.data.callbackCompleted = err == nil && session.work.Err() == nil
		session.mu.Unlock()
	}
	if err != nil {
		err = failure(ErrNative, "run", err)
	}
	session.remember(errors.Join(err, session.work.Err(), context.Cause(session.work), client.owner.life.Err(), context.Cause(client.owner.life)))
	cleanup := retained.close(context.Background(), cleanupCtx)
	returned = true
	return retained.Receipt(), errors.Join(err, cleanup)
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
	executor := sdk.FromContext(own.root).Browser
	if input.Borrow {
		session.borrowed = true
		session.sharedStorage = input.SharedStorage
		session.sharedPermissions = input.SharedPermissions
		session.sharedDownloads = input.SharedDownloads
		session.defaultContext = input.DefaultContext
		if !input.DefaultContext {
			contexts, err := cdp.Call(session.work, executor, target.GetBrowserContexts, cdp.Empty{})
			if err != nil {
				return err
			}
			if !slices.Contains(contexts.BrowserContextIDs, input.ContextID) {
				return failure(ErrInput, "context-membership")
			}
		}
		session.browserID = input.ContextID
		session.observedID = input.ContextID
		if input.DefaultContext && input.TargetID != "" {
			if err := own.identifyDefaultContext(session.work, session.browserContext, executor); err != nil {
				return err
			}
		}
	} else {
		session.creationAttempted = true
		created, err := cdp.Call(session.work, executor, target.CreateBrowserContext, target.CreateBrowserContextParams{DisposeOnDetach: new(true), ProxyServer: input.ProxyServer, ProxyBypassList: input.ProxyBypassList})
		session.browserID = created.BrowserContextID
		session.observedID = created.BrowserContextID
		if err != nil {
			return err
		}
		if session.browserID == "" {
			return failure(ErrNative, "missing-context-identity")
		}
	}
	tabID := input.TargetID
	if tabID != "" {
		info, err := cdp.Call(session.work, executor, target.GetTargetInfo, target.GetTargetInfoParams{TargetID: tabID})
		if err != nil {
			return err
		}
		if info.TargetInfo == nil || info.TargetInfo.TargetID != tabID || info.TargetInfo.BrowserContextID != session.observedID {
			return failure(ErrInput, "target-membership")
		}
	} else {
		own.targetAttempt(session.browserContext)
		newWindow := own.settings.NewWindow || !input.Borrow
		tab, err := cdp.Call(session.work, executor, target.CreateTarget, target.CreateTargetParams{URL: "about:blank", BrowserContextID: session.browserID, NewWindow: new(newWindow)})
		own.targetCreated(session.browserContext, tab.TargetID)
		if err != nil {
			return err
		}
		tabID = tab.TargetID
		if tabID == "" {
			return failure(ErrNative, "missing-target-identity")
		}
		if input.DefaultContext {
			info, err := cdp.Call(session.work, executor, target.GetTargetInfo, target.GetTargetInfoParams{TargetID: tabID})
			if err != nil {
				return err
			}
			if info.TargetInfo == nil || info.TargetInfo.TargetID != tabID {
				return failure(ErrNative, "target-identity")
			}
			session.observedID = info.TargetInfo.BrowserContextID
			session.defaultIdentityObserved = true
		}
	}
	// Initialization mutates native Context fields. Do not race its cancellation
	// handler against attachment; the operation budget still governs SDK commands.
	nativeOptions := []sdk.ContextOption{sdk.WithTargetID(tabID), sdk.WithTargetCleanupTimeout(own.settings.CleanupTimeout), sdk.WithManagedTargetLifetime()}
	session.targetID = tabID
	if input.TargetID != "" {
		session.borrowedTarget = true
		nativeOptions = append(nativeOptions, sdk.WithDetachOnCancel())
	}
	session.native, _ = sdk.NewContext(context.WithoutCancel(own.root), nativeOptions...)
	if err := sdk.Do(nativeContext{Context: session.work, native: session.native}); err != nil {
		return err
	}
	session.startEvents()
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
