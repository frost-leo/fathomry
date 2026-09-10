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

package invocation

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestZeroReceiptNeverInventsCompletion(t *testing.T) {
	for _, view := range []*Receipt[string]{nil, {}} {
		if _, ok := view.Result(); ok {
			t.Fatal("zero receipt became known result")
		}
		for _, wait := range []func(context.Context) (Result[string], error){
			view.Wait, view.WaitFinal, view.WaitReleased,
		} {
			if _, err := wait(context.Background()); !errors.Is(err, ErrWait) {
				t.Fatal("zero receipt wait did not report absent state")
			}
		}
	}
	if err := json.Unmarshal([]byte("{}"), new(Receipt[string])); err == nil {
		t.Fatal("private receipt reconstructed from JSON")
	}
}
