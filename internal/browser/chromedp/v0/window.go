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

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	sdk "github.com/chromedp/chromedp"
)

// Window is a non-owning window grant tied to an original target of an owned
// browser. A native WindowID in metadata is not a transferable grant.
type Window struct {
	private
	source  Source
	session *Session
	id      browser.WindowID
	data    []byte
}

func (window *Window) DataCopy() []byte {
	if window == nil {
		return nil
	}
	return append([]byte(nil), window.data...)
}

func (window *Window) valid() error {
	if window == nil || window.session == nil || window.source.owner == nil || window.session.owner != window.source.owner {
		return failure(ErrInput, "window-origin")
	}
	return nil
}

// AcquireWindow obtains native window identity from an exact owned-browser
// target. A borrowed browser connection cannot grant window ownership.
func AcquireWindow(ctx context.Context, source Source, session *Session) (*Window, error) {
	if session == nil || session.sessionState == nil || source.owner == nil || session.owner != source.owner || session.targetID == "" {
		return nil, failure(ErrInput, "window-origin")
	}
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		return nil, err
	}
	defer leave()
	result, err := runOwnerCommand(work, source, browser.GetWindowForTarget, browser.GetWindowForTargetParams{TargetID: session.targetID}, true, nil)
	if err != nil {
		return nil, err
	}
	if result.Bounds == nil {
		return nil, failure(ErrNative, "window-metadata")
	}
	buffer := boundedBuffer{limit: source.owner.settings.MaxResultBytes}
	if err := marshalBounded(&buffer, result); err != nil {
		return nil, err
	}
	return &Window{source: source, session: session, id: result.WindowID, data: buffer.data}, nil
}

// RunWindowCommand provides finite native bounds observations/mutations and
// content sizing. It rechecks target-window membership before each command,
// consuming one owner command for that check and one for the requested work.
// Another browser controller can still move a target between those native calls.
func RunWindowCommand[P, R any](ctx context.Context, window *Window, command cdp.Command[P, R], params P) (result R, err error) {
	if err := window.valid(); err != nil {
		return result, err
	}
	work, leave, err := window.session.enter(ctx, false)
	if err != nil {
		return result, err
	}
	defer leave()
	var copied P
	switch value := any(params).(type) {
	case browser.GetWindowBoundsParams:
		if value.WindowID != 0 && value.WindowID != window.id {
			return result, failure(ErrInput, "window-parameters")
		}
		value.WindowID = window.id
		copied = any(value).(P)
	case browser.SetWindowBoundsParams:
		if value.Bounds == nil || value.WindowID != 0 && value.WindowID != window.id {
			return result, failure(ErrInput, "window-parameters")
		}
		value.WindowID = window.id
		copied = any(value).(P)
	case browser.SetContentsSizeParams:
		if value.WindowID != 0 && value.WindowID != window.id {
			return result, failure(ErrInput, "window-parameters")
		}
		value.WindowID = window.id
		copied = any(value).(P)
	default:
		return result, failure(ErrUnsupported, "window-command")
	}
	return runOwnerCommand(work, window.source, command, copied, windowCommands[command.Method], func(work context.Context, own *owner) error {
		if own.commands >= uint64(own.settings.MaxCommands) {
			return failure(ErrLimit, "owner-commands")
		}
		own.commands++
		observed, err := cdp.Call(work, sdk.FromContext(own.root).Browser, browser.GetWindowForTarget, browser.GetWindowForTargetParams{TargetID: window.session.targetID})
		if err != nil {
			return err
		}
		if observed.WindowID != window.id {
			return failure(ErrState, "window-target-moved")
		}
		return nil
	})
}

var windowCommands = map[string]bool{
	"Browser.getWindowBounds": true,
	"Browser.setWindowBounds": true,
	"Browser.setContentsSize": true,
}
