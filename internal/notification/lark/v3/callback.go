/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package lark

import (
	"context"

	"github.com/frost-leo/fathomry/internal/fault"
)

const callbackWorkBytes int64 = 64 << 10

// callBorrowed joins actual callback termination, including Goexit. Cancellation
// never abandons a callback goroutine or the operation's existing reservation.
func callBorrowed[T any](ctx context.Context, kind fault.Kind, operation string, run func() (T, error)) (value T, err error) {
	done := make(chan struct{})
	go func() {
		returned := false
		defer func() {
			recovered := recover()
			if !returned {
				var zero T
				value = zero
				cause, _ := recovered.(error)
				err = failure(kind, operation, cause, ctx.Err(), context.Cause(ctx))
			}
			close(done)
		}()
		value, err = run()
		returned = true
	}()
	<-done
	return value, err
}
