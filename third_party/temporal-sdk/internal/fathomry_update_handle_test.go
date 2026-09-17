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

package internal

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/sdk/converter"
)

func TestFathomryScopedUpdatePreservesCompletion(t *testing.T) {
	denied := errors.New("expired scope")
	closed := false
	calls := 0
	dc := converter.GetDefaultDataConverter()
	payload, err := dc.ToPayloads("original-result")
	if err != nil {
		t.Fatal(err)
	}
	original := &completedUpdateHandle{value: newEncodedValue(payload, dc)}
	guarded, err := FathomryScopeWorkflowUpdateHandleV1(original, func(ctx context.Context, output any, next func(context.Context, any) error) error {
		if closed {
			return denied
		}
		calls++
		return next(ctx, output)
	})
	if err != nil || !IsUpdateWorkflowCompleted(guarded) {
		t.Fatal("completed Update classification changed", err)
	}
	var result string
	if err := guarded.Get(context.Background(), &result); err != nil || result != "original-result" || calls != 1 {
		t.Fatal("guarded result changed", err)
	}
	closed = true
	if err := guarded.Get(context.Background(), &result); !errors.Is(err, denied) || calls != 1 {
		t.Fatal("expired Get bypassed its guard")
	}
	lazy, err := FathomryScopeWorkflowUpdateHandleV1(&lazyUpdateHandle{}, func(context.Context, any, func(context.Context, any) error) error { return denied })
	if err != nil || IsUpdateWorkflowCompleted(lazy) {
		t.Fatal("pending Update became synchronous", err)
	}
	originalError := errors.New("original failure")
	failed, err := FathomryScopeWorkflowUpdateHandleV1(&completedUpdateHandle{err: originalError}, func(ctx context.Context, output any, next func(context.Context, any) error) error {
		return next(ctx, output)
	})
	if err != nil || !IsUpdateWorkflowCompleted(failed) || !errors.Is(failed.Get(context.Background(), nil), originalError) {
		t.Fatal("native failure identity changed", err)
	}
	if IsUpdateWorkflowCompleted(&lazyUpdateHandle{}) || !IsUpdateWorkflowCompleted(original) {
		t.Fatal("unguarded classification changed")
	}
}
