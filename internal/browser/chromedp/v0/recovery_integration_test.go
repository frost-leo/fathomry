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

package chromedp

import (
	"context"
	"errors"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
	"testing"
	"time"
)

func TestBrowserBorrowedLostConnectionReconcilesOnlyKnownContexts(t *testing.T) {
	host := bindFixture(t, browserOptions(t), 1)
	if err := host.client.owner.start(deadline(t)); err != nil {
		t.Fatal(err)
	}
	foreign, err := target.CreateBrowserContext().WithDisposeOnDetach(true).Do(browserExecutor(t, host))
	if err != nil {
		t.Fatal(err)
	}
	defer target.DisposeBrowserContext(foreign).Do(browserExecutor(t, host))
	address, err := host.client.owner.debugURL(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	options := OptionsV1{Name: "borrowed", RemoteURL: address, NewWindow: true, CleanupTimeout: 100 * time.Millisecond}
	f := bindFixture(t, options, 1)
	cleanup, cancel := context.WithCancel(context.Background())
	cancel()
	receipt, err := f.client.Run(deadline(t), cleanup, fault.Correlation{Call: "retained"}, func(*Session) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal("missing cleanup failure")
	}
	settle(t, f, fault.Correlation{Call: "retained"}, receipt, nil, context.Canceled, func(t testing.TB, value Result) {
		if value.ContextReleased() {
			t.Error("cleanup falsely certified")
		}
	})
	f.client.owner.cancel()
	select {
	case <-sdk.FromContext(f.client.owner.root).Browser.LostConnection:
	case <-time.After(time.Second):
		t.Fatal("owned connection did not close")
	}
	bounded, finish := context.WithTimeout(context.Background(), time.Second)
	defer finish()
	if err := f.assembly.Close(bounded); err != nil {
		t.Fatal("known-context reconciliation failed after connection loss", err)
	}
	if contexts := browserContexts(t, host); len(contexts) != 1 || contexts[0] != foreign {
		t.Fatal("known context remains or foreign context was changed")
	}
}
