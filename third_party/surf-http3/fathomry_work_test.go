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

package http3

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestFathomryWorkRetainsUntilActualEnd(t *testing.T) {
	var active atomic.Int32
	ctx := FathomryWithWork(context.Background(), func(context.Context) (func(), error) {
		active.Add(1)
		return func() { active.Add(-1) }, nil
	})
	finish, err := fathomryRetain(ctx)
	if err != nil || active.Load() != 1 {
		t.Fatal("native work was not retained")
	}
	finish()
	if active.Load() != 0 {
		t.Fatal("native work was not released")
	}
}
func TestFathomryWorkRejectsMissingReleaseAndPreservesCause(t *testing.T) {
	ctx := FathomryWithWork(context.Background(), func(context.Context) (func(), error) { return nil, nil })
	if _, err := fathomryRetain(ctx); err == nil {
		t.Fatal("missing completion authority accepted")
	}
	cause := errors.New("native admission failure")
	ctx = FathomryWithWork(context.Background(), func(context.Context) (func(), error) { return nil, cause })
	if _, err := fathomryRetain(ctx); !errors.Is(err, cause) {
		t.Fatal("admission cause changed")
	}
}
