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

	sdk "github.com/chromedp/chromedp"
)

func TestResponseStepLeaseDoesNotEscapeNavigation(t *testing.T) {
	session := unitSession(t)
	_, leave, err := session.enter(deadline(t), true)
	if err != nil {
		t.Fatal(err)
	}
	defer leave()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, responseLeaseKey{}, session.navigationIdle)
	calls := 0
	action := Action[sdk.Void]{navigation: true, local: func(context.Context, *Session) (sdk.Void, error) {
		calls++
		return sdk.Void{}, nil
	}}
	copy, err := ResponseStep(ctx, session, action)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunAction(deadline(t), session, copy); err != nil || calls != 1 {
		t.Fatal("response step could not use the ordinary lane", err, calls)
	}
	if !action.navigation || copy.navigation {
		t.Fatal("response step changed the caller action or kept the reserved lane")
	}
	foreign := unitSession(t)
	if _, err := ResponseStep(ctx, foreign, action); !errors.Is(err, ErrState) {
		t.Fatal("foreign session accepted the response lease", err)
	}
	if _, err := RunAction(deadline(t), foreign, copy); !errors.Is(err, ErrState) || calls != 1 {
		t.Fatal("copied response step granted foreign authority", err, calls)
	}
	if _, err := ResponseStep(context.Background(), session, action); !errors.Is(err, ErrState) {
		t.Fatal("ordinary context granted navigation reentry", err)
	}
	cancel()
	if _, err := RunAction(deadline(t), session, copy); !errors.Is(err, ErrState) || calls != 1 {
		t.Fatal("expired response step escaped the lease", err, calls)
	}
	if _, err := ResponseStep(ctx, session, action); !errors.Is(err, ErrState) {
		t.Fatal("expired response context renewed a lease", err)
	}
}

func TestResponseStepRejectsReplacedNavigationLease(t *testing.T) {
	session := unitSession(t)
	_, leave, err := session.enter(deadline(t), true)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), responseLeaseKey{}, session.navigationIdle)
	action, err := ResponseStep(ctx, session, Navigate("about:blank"))
	if err != nil {
		t.Fatal(err)
	}
	leave()
	_, leave, err = session.enter(deadline(t), true)
	if err != nil {
		t.Fatal(err)
	}
	defer leave()
	if _, err := RunAction(deadline(t), session, action); !errors.Is(err, ErrState) {
		t.Fatal("stale step used a replacement navigation lease", err)
	}
	if _, err := ResponseStep(ctx, session, Reload()); !errors.Is(err, ErrState) {
		t.Fatal("stale context used a replacement navigation lease", err)
	}
}
