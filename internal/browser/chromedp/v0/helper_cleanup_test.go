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
	"errors"
	"testing"

	sdk "github.com/chromedp/chromedp"
)

type helperCauseCycle struct{ visits int }

func (*helperCauseCycle) Error() string       { return "synthetic cyclic cause" }
func (cause *helperCauseCycle) Unwrap() error { cause.visits++; return cause }
func (*helperCauseCycle) As(any) bool         { panic("foreign As must not run during classification") }

func TestHelperCleanupClassificationIsBoundedAndDoesNotInvokeForeignAs(t *testing.T) {
	cycle := &helperCauseCycle{}
	if HelperCleanup(cycle) != nil || cycle.visits > 128 {
		t.Fatal("cyclic error classification was not bounded")
	}
	var typedNil *sdk.HelperCleanupError
	if HelperCleanup(typedNil) != nil {
		t.Fatal("typed nil cleanup became evidence")
	}
	cause := errors.New("native cleanup fixture")
	observed := &sdk.HelperCleanupError{Operation: "Runtime.releaseObject", Err: cause}
	if cleanup := HelperCleanup(errors.Join(errors.New("primary"), observed)); !errors.Is(cleanup, ErrCleanup) || !errors.Is(cleanup, cause) {
		t.Fatal("native cleanup cause was not retained independently", cleanup)
	}
}
