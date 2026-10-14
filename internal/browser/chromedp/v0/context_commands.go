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
)

// Context commands keep their own admission lane and lifetime. Closing the
// initial page must not revoke the retained BrowserContext's storage or discovery.
func runContextAction[T any](ctx context.Context, retained *Context, run func(context.Context, *Session) (T, error)) (value T, err error) {
	if retained == nil || retained.contextState == nil || retained.session == nil || ctx == nil || run == nil {
		return value, failure(ErrInput, "context-command")
	}
	retained.mu.Lock()
	if retained.closing || retained.commandBusy || retained.life == nil || retained.life.Err() != nil || ctx.Err() != nil {
		retained.mu.Unlock()
		return value, failure(ErrState, "context-command", ctx.Err(), context.Cause(ctx))
	}
	retained.commandBusy = true
	retained.commandIdle = make(chan struct{})
	retained.mu.Unlock()
	defer func() {
		retained.mu.Lock()
		retained.commandBusy = false
		close(retained.commandIdle)
		retained.mu.Unlock()
	}()
	work, cancel := context.WithCancelCause(retained.life)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	defer stop()
	defer cancel(nil)
	returned := false
	defer func() {
		if !returned {
			err = failure(ErrPanic, "context-command-did-not-return")
		}
		if err != nil {
			err = failure(ErrNative, "context-command", err)
		}
		retained.session.remember(err)
	}()
	err = invoke(func() error {
		var err error
		value, err = run(work, retained.session)
		return err
	})
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), work.Err(), context.Cause(work))
	returned = true
	return value, err
}
