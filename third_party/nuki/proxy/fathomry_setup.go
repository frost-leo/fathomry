/**
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

package proxy

import (
	"context"
	"errors"
	"time"
)

// Setup cancellation cannot retain authority over a transferred tunnel. Stopping
// a callback without joining it would allow a late deadline or stream cancel.
func watchSetup(ctx context.Context, timeout time.Duration, interrupt func() error) func() error {
	contextDone := make(chan struct{})
	var contextErr error
	stopContext := context.AfterFunc(ctx, func() {
		defer close(contextDone)
		contextErr = interrupt()
	})
	var timer *time.Timer
	var timerErr error
	timerDone := make(chan struct{})
	if timeout > 0 {
		timer = time.AfterFunc(timeout, func() {
			defer close(timerDone)
			timerErr = errors.Join(context.DeadlineExceeded, interrupt())
		})
	}
	return func() error {
		if !stopContext() {
			<-contextDone
		}
		if timer != nil && !timer.Stop() {
			<-timerDone
		}
		return errors.Join(ctx.Err(), context.Cause(ctx), contextErr, timerErr)
	}
}
