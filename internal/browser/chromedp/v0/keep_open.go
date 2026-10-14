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
	"os"
	"sync"

	"github.com/frost-leo/fathomry/internal/invocation"
)

// KeptBrowser owns a transferred local process and its profile cleanup, not a CDP
// connection. Borrowed/default pages can remain visible; created isolated contexts
// must be closed before transfer. Copies share one cleanup responsibility.
type KeptBrowser struct {
	private
	state *keptBrowserState
}
type keptBrowserState struct {
	owner    *owner
	endpoint string
	mu       sync.Mutex
	closed   bool
	cleanup  error
}

// KeepOpen transfers only after all retained native contexts and producers have
// released. It revokes further source work, but does not claim process exit.
func KeepOpen(ctx context.Context, source Source) (kept *KeptBrowser, err error) {
	defer func() {
		if err != nil {
			err = failure(ErrNative, "keep-open", err)
		}
	}()
	own := source.owner
	if ctx == nil || own == nil {
		return nil, failure(ErrInput, "keep-open")
	}
	if own.settings.RemoteURL != "" {
		return nil, failure(ErrState, "browser-owner-required")
	}
	if err := own.start(ctx); err != nil {
		return nil, err
	}
	if err := own.lock(ctx); err != nil {
		return nil, err
	}
	defer own.unlock()
	own.mu.Lock()
	defer own.mu.Unlock()
	if own.closed || own.transferred || own.process == nil || own.life.Err() != nil || len(own.sessions) != 0 || len(own.pendingObjects) != 0 || len(own.cleanupControls) != 0 || own.trace != nil {
		return nil, failure(ErrState, "keep-open-not-quiescent")
	}
	select {
	case <-own.processDone:
		return nil, failure(ErrState, "keep-open-exited")
	default:
	}
	endpoint, err := own.debugURL(ctx)
	if err != nil {
		return nil, err
	}
	if err := errors.Join(ctx.Err(), context.Cause(ctx), own.life.Err(), context.Cause(own.life)); err != nil {
		return nil, failure(ErrState, "keep-open-canceled", err)
	}
	// The cancellation callback uses this same mutex. Once the new owner is
	// installed, cancellation of the old source can only stop its connection.
	own.transferred, own.closed = true, true
	return &KeptBrowser{state: &keptBrowserState{owner: own, endpoint: endpoint}}, nil
}

// Endpoint and ProfileDirectory deliberately disclose connection/profile data.
func (kept *KeptBrowser) Endpoint() string {
	if kept == nil || kept.state == nil {
		return ""
	}
	return kept.state.endpoint
}
func (kept *KeptBrowser) ProfileDirectory() string {
	if kept == nil || kept.state == nil {
		return ""
	}
	return kept.state.owner.profileDir
}
func (kept *KeptBrowser) ProcessExited() bool {
	if kept == nil || kept.state == nil {
		return false
	}
	select {
	case <-kept.state.owner.processDone:
		return true
	default:
		return false
	}
}
func (kept *KeptBrowser) Closed() bool {
	if kept == nil || kept.state == nil {
		return false
	}
	kept.state.mu.Lock()
	defer kept.state.mu.Unlock()
	return kept.state.closed
}

// Close joins actual exit before cleaning a task-owned temporary profile.
// Caller-selected persistent directories are never deleted. A timed-out close
// keeps the same cleanup handle; ProcessExited and Closed are separate facts.
func (kept *KeptBrowser) Close(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			err = failure(ErrCleanup, "kept-browser-close", err)
		}
	}()
	if kept == nil || kept.state == nil || ctx == nil {
		return failure(ErrInput, "kept-browser-close")
	}
	state, own := kept.state, kept.state.owner
	work, cancel, err := (invocation.Budget{Limit: own.settings.CleanupTimeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return failure(ErrCleanup, "kept-browser-close", err)
	}
	defer cancel()
	if err := own.lock(work); err != nil {
		return failure(ErrCleanup, "kept-browser-join", err)
	}
	defer own.unlock()
	state.mu.Lock()
	closed, history := state.closed, state.cleanup
	state.mu.Unlock()
	if closed {
		return history
	}
	if !kept.ProcessExited() {
		if err := own.process.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
			history = errors.Join(history, err)
		}
		select {
		case <-own.processDone:
		case <-work.Done():
			if err := own.process.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				history = errors.Join(history, err)
			}
			history = failure(ErrCleanup, "kept-process-exit", history, work.Err(), context.Cause(work))
			state.mu.Lock()
			state.cleanup = history
			state.mu.Unlock()
			return history
		}
	}
	err = own.cleanProfile()
	state.mu.Lock()
	defer state.mu.Unlock()
	if err != nil {
		if state.cleanup == nil {
			state.cleanup = failure(ErrCleanup, "kept-profile", err)
		}
		return errors.Join(state.cleanup, err)
	}
	state.closed = true
	state.cleanup = history
	return history
}
