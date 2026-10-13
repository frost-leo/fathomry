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
	"testing"
)

func TestDebuggerContextsAdmitBeforeGrantAccess(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		session := unitSession(t)
		session.owner.settings.MaxEvents = 1
		session.owner.settings.MaxCommands = 1
		if exhausted {
			session.accounting().data.commands = 1
		}
		state := &controlState{session: session, life: context.Background(), debugger: &debuggerState{evidence: debuggerEvidence{Enabled: true}}}
		calls := 0
		err := SetBlackboxContextArguments(context.Background(), &Control{state: state}, 2, func(int) *ExecutionContext {
			calls++
			return nil
		})
		if !errors.Is(err, ErrLimit) || calls != 0 || state.commands != 0 {
			t.Fatal("context count/command admission ran after grant access", exhausted, err)
		}
	}
	var control *Control
	if err := control.SetBlackboxContexts(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal("zero debugger accepted contexts", err)
	}
}
