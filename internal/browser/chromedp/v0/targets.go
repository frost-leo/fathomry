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

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
)

// TargetRef is discovery provenance, not a native Target or ownership inferred
// from its string ID. Attach always borrows a newly discovered target.
type TargetRef struct {
	private
	context *contextState
	id      target.ID
	kind    string
	data    []byte
}

func (value *TargetRef) DataCopy() []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value.data...)
}

func (retained *Context) Targets(ctx context.Context, params target.GetTargetsParams) ([]*TargetRef, error) {
	if retained == nil || retained.contextState == nil {
		return nil, failure(ErrInput, "targets")
	}
	return runContextAction(ctx, retained, func(work context.Context, session *Session) ([]*TargetRef, error) {
		if session.observedID == "" && !(session.defaultContext && session.defaultIdentityObserved) {
			return nil, failure(ErrState, "target-context-identity")
		}
		if err := session.reserveCommand(); err != nil {
			return nil, err
		}
		buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
		if err := marshalBounded(&buffer, params); err != nil {
			return nil, err
		}
		var copied target.GetTargetsParams
		if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
			return nil, err
		}
		result, err := cdp.Call(session.commandWork(work, false), sdk.FromContext(session.owner.root).Browser, target.GetTargets, copied)
		if err != nil {
			return nil, err
		}
		values := make([]*TargetRef, 0)
		var size int64
		for _, info := range result.TargetInfos {
			if info == nil {
				return nil, failure(ErrNative, "target-info")
			}
			if info.Type == "browser" || info.BrowserContextID != session.observedID {
				continue
			}
			if info.TargetID == "" || info.Type == "" {
				return nil, failure(ErrNative, "target-identity")
			}
			data := boundedBuffer{limit: session.owner.settings.MaxResultBytes - size}
			if err := marshalBounded(&data, info); err != nil {
				return nil, err
			}
			size += int64(len(data.data))
			values = append(values, &TargetRef{context: retained.contextState, id: info.TargetID, kind: info.Type, data: data.data})
		}
		return values, nil
	})
}

// Attach rechecks the exact context and observed type before native attachment.
// Repeated attachment shares the original page state and quota. Discovery alone
// never authorizes closing a target that this Provider did not create.
func (retained *Context) Attach(ctx context.Context, value *TargetRef) (*Session, error) {
	if retained == nil || retained.contextState == nil || value == nil || value.context != retained.contextState || value.id == "" {
		return nil, failure(ErrInput, "target-origin")
	}
	_, err := runContextAction(ctx, retained, func(work context.Context, session *Session) (cdp.Empty, error) {
		if err := session.reserveCommand(); err != nil {
			return cdp.Empty{}, err
		}
		info, err := cdp.Call(session.commandWork(work, false), sdk.FromContext(session.owner.root).Browser, target.GetTargetInfo, target.GetTargetInfoParams{TargetID: value.id})
		if err != nil {
			return cdp.Empty{}, err
		}
		if info.TargetInfo == nil || info.TargetInfo.TargetID != value.id || info.TargetInfo.BrowserContextID != session.observedID || info.TargetInfo.Type != value.kind {
			return cdp.Empty{}, failure(ErrState, "target-origin-changed")
		}
		return cdp.Empty{}, nil
	})
	if err != nil {
		return nil, err
	}
	return retained.openPage(ctx, value.id, true)
}
