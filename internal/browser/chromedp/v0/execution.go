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

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

// ExecutionContext borrows one observed V8 context from its original page.
// Its unique identity, not its reusable numeric ID, authorizes evaluation.
// It does not keep the context alive or create a second event-state table.
type ExecutionContext struct {
	private
	session *Session
	origin  sdk.ExecutionOrigin
	data    []byte
}

// DataCopy contains native numeric and unique identity, not inferred name,
// origin URL, frame membership or default-world status.
func (execution *ExecutionContext) DataCopy() []byte {
	if execution == nil {
		return nil
	}
	return append([]byte(nil), execution.data...)
}

func (session *Session) executionOrigins(maximum int) ([]sdk.ExecutionOrigin, error) {
	if session == nil || session.sessionState == nil || session.native == nil || session.work == nil || session.work.Err() != nil {
		return nil, failure(ErrState, "execution-contexts")
	}
	native := sdk.FromContext(session.native)
	if native == nil || native.Target == nil {
		return nil, failure(ErrState, "execution-contexts")
	}
	values, err := native.Target.ExecutionOrigins(maximum)
	if errors.Is(err, sdk.ErrEventOverflow) {
		return nil, failure(ErrLimit, "execution-contexts", err)
	}
	if err != nil {
		return nil, failure(ErrState, "execution-contexts", err)
	}
	return values, nil
}
func (execution *ExecutionContext) valid(session *Session) error {
	if execution == nil || execution.session == nil || execution.origin.ID <= 0 || execution.origin.UniqueID == "" {
		return failure(ErrInput, "execution-context")
	}
	if session == nil || session.sessionState != execution.session.sessionState {
		return failure(ErrInput, "execution-context-origin")
	}
	values, err := session.executionOrigins(session.owner.settings.MaxEvents)
	if err != nil {
		return err
	}
	for _, value := range values {
		if value == execution.origin {
			return nil
		}
	}
	return failure(ErrState, "execution-context-retired")
}

// ExecutionContexts snapshots native identities, including contexts created
// during attachment before caller subscriptions existed. No protocol toggle or
// page script is needed. Results are bounded before copying and retention.
func (session *Session) ExecutionContexts(ctx context.Context) (values []*ExecutionContext, err error) {
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		return nil, err
	}
	defer leave()
	defer func() {
		err = errors.Join(err, work.Err(), context.Cause(work))
		session.remember(err)
	}()
	account := session.accounting()
	account.mu.Lock()
	remaining := session.owner.settings.MaxResultBytes - account.delivered
	account.mu.Unlock()
	origins, err := session.executionOrigins(min(session.owner.settings.MaxEvents, int(remaining/128)))
	if err != nil {
		return nil, err
	}
	values = make([]*ExecutionContext, 0, len(origins))
	var used int64
	for _, origin := range origins {
		if origin.ID <= 0 || origin.UniqueID == "" {
			return nil, failure(ErrNative, "execution-context-identity")
		}
		buffer := boundedBuffer{limit: remaining - used}
		if err := marshalBounded(&buffer, origin); err != nil {
			return nil, err
		}
		values = append(values, &ExecutionContext{session: session, origin: origin, data: buffer.data})
		used += int64(len(buffer.data)) + 64
		if used > remaining {
			return nil, failure(ErrLimit, "execution-context-output")
		}
	}
	account.mu.Lock()
	defer account.mu.Unlock()
	if used > session.owner.settings.MaxResultBytes-account.delivered {
		return nil, failure(ErrLimit, "execution-context-output")
	}
	account.delivered += used
	return values, nil
}

// EvaluateIn keeps the native Evaluate parameters, but fills this group's name
// and the exact execution unique ID only in an admitted copy. All returned
// references and exception/unknown custody stay in this ObjectGroup.
func (group *ObjectGroup) EvaluateIn(ctx context.Context, execution *ExecutionContext, params runtime.EvaluateParams) (object *Object, err error) {
	if group == nil || group.state == nil || ctx == nil {
		return nil, failure(ErrInput, "execution-evaluate")
	}
	session := group.state.session
	defer func() { session.remember(err) }()
	if err := execution.valid(session); err != nil {
		return nil, err
	}
	if params.ContextID != 0 && params.ContextID != execution.origin.ID ||
		params.UniqueContextID != "" && params.UniqueContextID != execution.origin.UniqueID || params.ObjectGroup != "" {
		return nil, failure(ErrInput, "execution-parameters")
	}
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		return nil, err
	}
	defer leave()
	return group.executeIn(work, func(name string) sdk.Action[*runtime.RemoteObject] {
		return func(ctx context.Context, target *sdk.Target) (*runtime.RemoteObject, error) {
			buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
			if err := marshalBounded(&buffer, params); err != nil {
				return nil, err
			}
			var copied runtime.EvaluateParams
			if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
				return nil, err
			}
			copied.ContextID = 0
			copied.UniqueContextID = execution.origin.UniqueID
			copied.ObjectGroup = name
			if err := execution.valid(session); err != nil {
				return nil, err
			}
			reply, err := cdp.Call(ctx, target, runtime.Evaluate, copied)
			if err != nil {
				return nil, err
			}
			if reply.ExceptionDetails != nil {
				return nil, &sdk.ExceptionError{ExceptionDetails: reply.ExceptionDetails}
			}
			return reply.Result, nil
		}
	}, true, execution)
}
