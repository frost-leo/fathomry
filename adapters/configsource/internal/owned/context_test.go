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

package owned

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestNativeContextRetainsSignalsButNotBorrowedCause(t *testing.T) {
	type key struct{}
	for _, prior := range []bool{false, true} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("prior=%t/deadline=%t", prior, deadline), func(t *testing.T) {
				borrowed := errors.New("private caller cause")
				base := context.WithValue(context.Background(), key{}, "caller value")
				var parent context.Context
				var cancel context.CancelFunc
				if deadline {
					parent, cancel = context.WithDeadlineCause(base, time.Now().Add(10*time.Millisecond), borrowed)
				} else {
					value, stop := context.WithCancelCause(base)
					parent, cancel = value, func() { stop(borrowed) }
				}
				defer cancel()
				if prior {
					if !deadline {
						cancel()
					}
					<-parent.Done()
				}
				native := NativeContext(parent)
				wantDeadline, wantOK := parent.Deadline()
				gotDeadline, gotOK := native.Deadline()
				if gotDeadline != wantDeadline || gotOK != wantOK || native.Done() != parent.Done() || native.Value(key{}) != "caller value" {
					t.Fatal("native context lost ordinary caller contract")
				}
				child, stop := context.WithCancel(native)
				defer stop()
				if !prior {
					if !deadline {
						cancel()
					}
					<-parent.Done()
				}
				select {
				case <-child.Done():
				case <-time.After(time.Second):
					t.Fatal("cancellation did not propagate")
				}
				if native.Err() != parent.Err() || context.Cause(native) != parent.Err() || context.Cause(child) != parent.Err() || context.Cause(parent) != borrowed {
					t.Fatal("native context leaked cause or changed cancellation evidence")
				}
			})
		}
	}
}
