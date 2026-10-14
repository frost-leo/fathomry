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

	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
)

type responseLeaseKey struct{}

// Valid reports whether the action came from a valid closed constructor.
func (action Action[T]) Valid() bool {
	return action.native != nil || action.local != nil || action.object != nil || action.managed != nil
}

// RunResponse reserves the navigation lane and delegates correlation to the SDK.
// The callback receives cancellation and a private step lease, never a native owner.
// It must return before this operation can finish.
func RunResponse(ctx context.Context, session *Session, run func(context.Context) error) (*network.Response, error) {
	if run == nil {
		return nil, failure(ErrInput, "response")
	}
	return RunAction(ctx, session, Action[*network.Response]{navigation: true, local: func(work context.Context, session *Session) (*network.Response, error) {
		session.mu.Lock()
		lease := session.navigationIdle
		session.mu.Unlock()
		return sdk.RunResponse(session.nativeWork(work), sdk.Func(func(trigger context.Context, _ *sdk.Target) error {
			return run(context.WithValue(bareContext{trigger}, responseLeaseKey{}, lease))
		}))
	}})
}

// ResponseStep copies an action into the ordinary lane while its response wait
// holds the navigation lane. A stale or foreign response context cannot do this.
func ResponseStep(ctx context.Context, session *Session, action Action[sdk.Void]) (Action[sdk.Void], error) {
	if ctx == nil || session == nil || session.sessionState == nil || !action.Valid() {
		return Action[sdk.Void]{}, failure(ErrInput, "response-step")
	}
	lease, _ := ctx.Value(responseLeaseKey{}).(chan struct{})
	session.mu.Lock()
	valid := lease != nil && lease == session.navigationIdle && session.navigating && !session.closed && ctx.Err() == nil
	session.mu.Unlock()
	if !valid {
		return Action[sdk.Void]{}, failure(ErrState, "response-step")
	}
	if !action.navigation {
		return action, nil
	}
	return Action[sdk.Void]{bytes: action.bytes, local: func(work context.Context, current *Session) (sdk.Void, error) {
		session.mu.Lock()
		valid := current.sessionState == session.sessionState && lease == session.navigationIdle && session.navigating && !session.closed && ctx.Err() == nil
		session.mu.Unlock()
		if !valid {
			return sdk.Void{}, failure(ErrState, "response-step")
		}
		return action.run(work, current)
	}}, nil
}
