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
	"fmt"
	"time"

	"github.com/chromedp/cdproto/cdp"
)

// HelperCleanupError reports cleanup that a native helper did not confirm.
// Operation names the cleanup command. Err keeps the original error for inspection.
// A helper can return this error together with its primary operation error.
type HelperCleanupError struct {
	Operation string
	Err       error
}

func (e *HelperCleanupError) Error() string {
	return fmt.Errorf("cleaning up %s: %w", e.Operation, e.Err).Error()
}

func (e *HelperCleanupError) Unwrap() error { return e.Err }

type nodeObjectGroupKey struct{}

// WithNodeObjectGroup assigns resolved node objects to an explicit native group.
// Native function results inherit the receiver group. The caller owns the group
// and its cleanup, including exception references and results that arrive late.
// An empty name preserves the native ungrouped behavior.
func WithNodeObjectGroup(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, nodeObjectGroupKey{}, name)
}

func isHelperCleanup(err error) bool {
	remaining := 128
	var visit func(error) bool
	visit = func(err error) bool {
		if err == nil || remaining == 0 {
			return false
		}
		remaining--
		if cleanup, ok := err.(*HelperCleanupError); ok {
			return cleanup != nil
		}
		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			for _, cause := range value.Unwrap() {
				if remaining == 0 {
					break
				}
				if visit(cause) {
					return true
				}
			}
		case interface{ Unwrap() error }:
			return visit(value.Unwrap())
		}
		return false
	}
	return visit(err)
}

func helperCleanup[P, R any](ctx context.Context, t *Target, command cdp.Command[P, R], params P) error {
	limit := 5 * time.Second
	if c := FromContext(ctx); c != nil && c.cleanupTimeout > 0 {
		limit = c.cleanupTimeout
	}
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), limit)
	defer cancel()
	work = WithCallLimits(work, CallLimits{MaxInputBytes: 4096, MaxOutputBytes: 4096})
	_, err := cdp.Call(work, t, command, params)
	if err != nil {
		return &HelperCleanupError{Operation: command.Method, Err: err}
	}
	return nil
}

func reportHelperCleanup(ctx context.Context, err error) error {
	if cleanup, ok := err.(*HelperCleanupError); ok && cleanup != nil {
		limits, _ := ctx.Value(callLimitsKey{}).(CallLimits)
		if limits.Cleanup != nil {
			limits.Cleanup(cleanup)
		}
	}
	return err
}

func helperReleaseInput[P, R any](ctx context.Context, t *Target, command cdp.Command[P, R], params P) error {
	var queued, acknowledged bool
	work := withQueued(ctx, func(string) { queued = true })
	work = withReply(work, func(_ string, success bool) { acknowledged = success })
	_, err := cdp.Call(work, t, command, params)
	if err == nil || acknowledged {
		return err
	}
	if queued {
		return errors.Join(err, reportHelperCleanup(ctx, &HelperCleanupError{Operation: command.Method, Err: err}))
	}
	return errors.Join(err, reportHelperCleanup(ctx, helperCleanup(ctx, t, command, params)))
}
