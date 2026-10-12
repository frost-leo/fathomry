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

package chromedp

import (
	"context"
	"testing"
	"time"
)

func TestManagedClearDirtyValuesPreservesEventBehavior(t *testing.T) {
	ctx, cancel := testAllocate(t, "")
	defer cancel()
	ctx, done := context.WithTimeout(ctx, 5*time.Second)
	defer done()
	if err := Do(ctx, Navigate("data:text/html,<input id='field' value='initial'><textarea id='text'>initial</textarea><textarea id='empty'></textarea>")); err != nil {
		t.Fatal(err)
	}
	for _, selector := range []CSS{"#field", "#text", "#empty"} {
		t.Run(string(selector), func(t *testing.T) {
			if err := Do(ctx, SetValue(selector, "dirty")); err != nil {
				t.Fatal(err)
			}
			if _, err := Run(ctx, Evaluate[Void]("window.events=0; document.oninput=()=>window.events++; document.onchange=()=>window.events++")); err != nil {
				t.Fatal(err)
			}
			if err := Do(ctx, Clear(selector)); err != nil {
				t.Fatal(err)
			}
			if value, err := Run(ctx, Value(selector)); err != nil || value != "" {
				t.Fatal("live value not cleared", value, err)
			}
			if events, err := Run(ctx, Evaluate[int]("window.events")); err != nil || events != 0 {
				t.Fatal("Clear introduced input or change events", events, err)
			}
			if err := Do(ctx, SetValue(selector, "again")); err != nil {
				t.Fatal(err)
			}
			if events, err := Run(ctx, Evaluate[int]("window.events")); err != nil || events != 2 {
				t.Fatal("SetValue lost its native input and change events", events, err)
			}
		})
	}
}
