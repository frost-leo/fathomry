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
	"time"
)

// WithManagedTargetLifetime keeps target dispatch alive until its owning Context
// is canceled. Operation contexts still cancel commands. The owner must call Cancel.
// This allows a separate cleanup context to close streams after work cancellation.
func WithManagedTargetLifetime() ContextOption {
	return func(c *Context) { c.managedTargetLifetime = true }
}

type targetLifetimeContext struct{ done <-chan struct{} }

func (c targetLifetimeContext) Done() <-chan struct{} { return c.done }
func (c targetLifetimeContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}
func (targetLifetimeContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (targetLifetimeContext) Value(any) any               { return nil }

// WaitStopped waits for the browser's reader, dispatcher and transport close.
// It is a join, not a request to close an externally owned browser.
func (b *Browser) WaitStopped(ctx context.Context) error {
	if b == nil || b.stopped == nil || ctx == nil {
		return ErrInvalidContext
	}
	select {
	case <-b.stopped:
		return b.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitStopped waits for both target event handlers. It does not close a tab.
func (t *Target) WaitStopped(ctx context.Context) error {
	if t == nil || t.stopped == nil || ctx == nil {
		return ErrInvalidContext
	}
	select {
	case <-t.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
