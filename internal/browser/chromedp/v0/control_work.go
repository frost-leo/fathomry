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

import "context"

// Ordinary commands join the existing control lifetime without using its
// reserved reply lane. The caller still obtains normal page/command admission.
func (state *controlState) enterCommand(ctx context.Context, check func() error) (context.Context, func(), error) {
	if ctx == nil || state == nil {
		return nil, nil, failure(ErrInput, "control-command")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closing || state.closed || state.life.Err() != nil || ctx.Err() != nil {
		return nil, nil, failure(ErrState, "control-command", ctx.Err(), context.Cause(ctx))
	}
	if err := check(); err != nil {
		return nil, nil, err
	}
	if state.commands == 0 {
		state.commandsIdle = make(chan struct{})
	}
	state.commands++
	work, cancel := context.WithCancelCause(state.life)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	return work, func() {
		stop()
		cancel(nil)
		state.mu.Lock()
		state.commands--
		if state.commands == 0 {
			close(state.commandsIdle)
		}
		state.mu.Unlock()
	}, nil
}

func (state *controlState) joinCommands(ctx context.Context) error {
	state.mu.Lock()
	active, idle := state.commands, state.commandsIdle
	state.mu.Unlock()
	if active == 0 {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return failure(ErrCleanup, "control-command-work", ctx.Err(), context.Cause(ctx))
	}
}
