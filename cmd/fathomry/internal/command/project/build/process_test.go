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

package build

import (
	"context"
	"errors"
	"testing"
)

func TestProcessBufferRetainsFailures(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		parent, cancelParent := context.WithCancelCause(context.Background())
		child, cancelChild := context.WithCancelCause(parent)
		cause := errors.New("caller stopped")
		if cancelFirst {
			cancelParent(cause)
		}
		stdout := &processBuffer{maximum: 4, cancel: cancelChild}
		stderr := &processBuffer{maximum: 4, cancel: cancelChild}
		for _, buffer := range []*processBuffer{stdout, stderr} {
			if count, err := buffer.Write([]byte("1234")); count != 4 || err != nil {
				t.Fatal(count, err)
			}
			if count, err := buffer.Write([]byte("5")); count != 0 || !errors.Is(err, ErrLimit) || buffer.err != err {
				t.Fatal("observed failure not retained", count, err, buffer.err)
			}
			if count, err := buffer.Write(nil); count != 0 || err != buffer.err || string(buffer.Bytes()) != "1234" {
				t.Fatal("failed writer changed state", count, err, string(buffer.Bytes()))
			}
		}
		cancelParent(cause)
		if !errors.Is(stdout.err, ErrLimit) || !errors.Is(stderr.err, ErrLimit) {
			t.Fatal("competing cancellation discarded observed stream failures")
		}
		if cancelFirst && context.Cause(child) != cause || !cancelFirst && context.Cause(child) != stdout.err {
			t.Fatal("first-wins context control changed", context.Cause(child))
		}
		cancelChild(nil)
	}
}
