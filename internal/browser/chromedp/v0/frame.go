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
	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
)

// Frame borrows an observed document in one native target's frame tree.
// DataCopy is detached native metadata. It cannot supply a different frame ID.
type Frame struct {
	private
	session *Session
	id      cdp.FrameID
	epoch   uint64
	stamp   *nodeFrame
	version uint64
	data    []byte
}

func (frame *Frame) DataCopy() []byte {
	if frame == nil {
		return nil
	}
	return append([]byte(nil), frame.data...)
}
func (frame *Frame) valid(session *Session) error {
	if frame == nil || frame.session == nil || frame.id == "" || frame.stamp == nil {
		return failure(ErrInput, "frame")
	}
	if session == nil || frame.session.sessionState != session.sessionState {
		return failure(ErrInput, "frame-origin")
	}
	if session.work.Err() != nil || session.objectOrigin == nil || session.objectOrigin.epoch.Load() != frame.epoch || frame.stamp.epoch.Load() != frame.version {
		return failure(ErrState, "frame-document")
	}
	return nil
}

// Frames returns a preorder snapshot of the native frame tree. Each frame has
// document provenance. A metadata-only OOPIF is not an attached target or an
// execution grant; attach it through the retained context before running there.
func (session *Session) Frames(ctx context.Context) (frames []*Frame, err error) {
	return RunAction(ctx, session, Action[[]*Frame]{local: func(ctx context.Context, session *Session) ([]*Frame, error) {
		if err := session.startObjectOrigin(); err != nil {
			return nil, err
		}
		origin := session.objectOrigin
		origin.nodesMu.Lock()
		revision := origin.nodeRevision
		origin.nodesMu.Unlock()
		epoch := origin.epoch.Load()
		result, err := cdp.Call(session.commandWork(ctx, true), sdk.FromContext(session.native).Target, page.GetFrameTree, cdp.Empty{})
		if err != nil {
			return nil, err
		}
		account := session.accounting()
		account.mu.Lock()
		remaining := session.owner.settings.MaxResultBytes - account.delivered
		account.mu.Unlock()
		maximum := min(session.owner.settings.MaxEvents, int(remaining/128))
		pending := []*page.FrameTree{result.FrameTree}
		var values []*Frame
		var used int64
		for len(pending) != 0 {
			if len(values)+len(pending) > maximum {
				return nil, failure(ErrLimit, "frames")
			}
			current := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if current == nil || current.Frame == nil || current.Frame.ID == "" {
				return nil, failure(ErrNative, "frame-identity")
			}
			if len(values)+len(pending)+len(current.ChildFrames)+1 > maximum {
				return nil, failure(ErrLimit, "frames")
			}
			for index := len(current.ChildFrames) - 1; index >= 0; index-- {
				pending = append(pending, current.ChildFrames[index])
			}
			buffer := boundedBuffer{limit: remaining - used}
			if err := marshalBounded(&buffer, current.Frame); err != nil {
				return nil, err
			}
			used += int64(len(buffer.data)) + 64
			if used > remaining {
				return nil, failure(ErrLimit, "frame-output")
			}
			values = append(values, &Frame{session: session, id: current.Frame.ID, epoch: epoch, data: buffer.data})
		}
		origin.nodesMu.Lock()
		if origin.nodeRevision != revision || origin.epoch.Load() != epoch {
			origin.nodesMu.Unlock()
			return nil, failure(ErrState, "frame-document-changed")
		}
		if origin.nodeFrames == nil {
			origin.nodeFrames = make(map[cdp.FrameID]*nodeFrame)
		}
		for _, frame := range values {
			stamp := origin.nodeFrames[frame.id]
			if stamp == nil {
				stamp = &nodeFrame{}
				origin.nodeFrames[frame.id] = stamp
			}
			frame.stamp, frame.version = stamp, stamp.epoch.Load()
		}
		origin.nodesMu.Unlock()
		account.mu.Lock()
		defer account.mu.Unlock()
		if used > session.owner.settings.MaxResultBytes-account.delivered {
			return nil, failure(ErrLimit, "frame-output")
		}
		account.delivered += used
		return values, nil
	}})
}

// CreateWorld keeps all native parameters, including an explicit universal-access
// choice. Parameters are copied after admission before filling FrameID. Worlds
// belong to the document, not to an independent native destroy-world command.
// MaxEvents bounds cumulative creation attempts on this page, including unknown
// outcomes after cancellation. Navigation concurrent with creation is not atomic.
func (session *Session) CreateWorld(ctx context.Context, frame *Frame, params page.CreateIsolatedWorldParams) (execution *ExecutionContext, err error) {
	_, err = RunAction(ctx, session, Action[sdk.Void]{local: func(ctx context.Context, session *Session) (sdk.Void, error) {
		if err := frame.valid(session); err != nil {
			return sdk.Void{}, err
		}
		if params.FrameID != "" && params.FrameID != frame.id {
			return sdk.Void{}, failure(ErrInput, "frame-parameters")
		}
		if err := session.reserveCommand(); err != nil {
			return sdk.Void{}, err
		}
		session.mu.Lock()
		if session.worldAttempts >= session.owner.settings.MaxEvents {
			session.mu.Unlock()
			return sdk.Void{}, failure(ErrLimit, "world-attempts")
		}
		session.worldAttempts++
		session.mu.Unlock()
		buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
		if err := marshalBounded(&buffer, params); err != nil {
			return sdk.Void{}, err
		}
		var copied page.CreateIsolatedWorldParams
		if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
			return sdk.Void{}, err
		}
		copied.FrameID = frame.id
		if err := frame.valid(session); err != nil {
			return sdk.Void{}, err
		}
		result, err := cdp.Call(session.commandWork(ctx, false), sdk.FromContext(session.native).Target, page.CreateIsolatedWorld, copied)
		if err != nil {
			return sdk.Void{}, err
		}
		if err := frame.valid(session); err != nil {
			return sdk.Void{}, err
		}
		origins, err := session.executionOrigins(session.owner.settings.MaxEvents)
		if err != nil {
			return sdk.Void{}, err
		}
		for _, origin := range origins {
			if origin.ID == result.ExecutionContextID && origin.ID > 0 && origin.UniqueID != "" {
				data := boundedBuffer{limit: session.owner.settings.MaxResultBytes}
				if err := marshalBounded(&data, origin); err != nil {
					return sdk.Void{}, err
				}
				account := session.accounting()
				account.mu.Lock()
				if int64(len(data.data))+64 > session.owner.settings.MaxResultBytes-account.delivered {
					account.mu.Unlock()
					return sdk.Void{}, failure(ErrLimit, "world-output")
				}
				account.delivered += int64(len(data.data)) + 64
				account.mu.Unlock()
				execution = &ExecutionContext{session: session, origin: origin, data: data.data}
				return sdk.Void{}, nil
			}
		}
		return sdk.Void{}, failure(ErrNative, "world-identity-unobserved")
	}})
	return execution, err
}
