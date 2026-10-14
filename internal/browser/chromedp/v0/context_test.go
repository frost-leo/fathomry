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
	"os"
	"path/filepath"
	"testing"
	"time"

	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestRetainedPagesShareBudgets(t *testing.T) {
	options := browserOptions(t)
	options.MaxPages = 2
	options.MaxCommands = 2
	f := bindFixture(t, options, 1)
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "retained-pages"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := retained.NewPage(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range []*Session{retained.Page(), second} {
		value, err := RunAction(deadline(t), page, Evaluate[int]("42"))
		if err != nil || value != 42 {
			t.Fatal(value, err)
		}
	}
	if _, err := RunAction(deadline(t), second, Evaluate[int]("43")); !errors.Is(err, ErrLimit) {
		t.Fatal("page multiplied command quota", err)
	}
	copy := *retained
	if _, err := copy.NewPage(deadline(t)); !errors.Is(err, ErrLimit) {
		t.Fatal("page ceiling", err)
	}
	if err := retained.Close(deadline(t)); !errors.Is(err, ErrLimit) {
		t.Fatal("handled error disappeared", err)
	}
	settle(t, f, fault.Correlation{Call: "retained-pages"}, retained.Receipt(), ErrLimit, nil, func(t testing.TB, value Result) {
		if value.Commands() != 2 || !value.ContextReleased() {
			t.Fatal("retained accounting")
		}
	})
	if _, err := RunAction(context.Background(), second, Evaluate[int]("44")); !errors.Is(err, ErrState) {
		t.Fatal("closed page revived", err)
	}
}

func TestRetainedNavigationAccountingKeepsLegacyAndControlledRoutesDistinct(t *testing.T) {
	options := browserOptions(t)
	options.MaxPages, options.MaxCommands = 2, 2
	fixture := bindFixture(t, options, 1)
	id := fault.Correlation{Call: "navigation-accounting"}
	retained, err := fixture.client.Retain(deadline(t), id)
	if err != nil {
		t.Fatal(err)
	}
	second, err := retained.NewPage(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunAction(deadline(t), retained.Page(), Navigate("data:text/html,<h1>first</h1>")); err != nil {
		t.Fatal(err)
	}
	if _, err := RunAction(deadline(t), second, NavigateResponse("data:text/html,<h1>second</h1>")); err != nil {
		t.Fatal(err)
	}
	if err := second.Navigate(deadline(t), "data:text/html,<h1>legacy exception</h1>"); err != nil {
		t.Fatal("legacy Internal counter contract changed", err)
	}
	if err := retained.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, fixture, id, retained.Receipt(), nil, nil, func(t testing.TB, result Result) {
		if result.Commands() != 2 || result.Navigations() != 3 || !result.ContextReleased() {
			t.Fatal("navigation accounting lost a page or conflated the legacy exception")
		}
	})
}
func TestRetainedBorrowedPageDetaches(t *testing.T) {
	host := bindFixture(t, browserOptions(t), 1)
	original, err := host.client.Retain(deadline(t), fault.Correlation{Call: "peer"})
	if err != nil {
		t.Fatal(err)
	}
	address, err := host.client.owner.debugURL(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	f := bindFixture(t, OptionsV1{Name: "borrowed", RemoteURL: address, NewWindow: true}, 1)
	originalPage := original.Page()
	opts := SessionOptionsV1{Borrow: true, ContextID: originalPage.browserID, TargetID: sdk.FromContext(originalPage.native).Target.TargetID}
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "borrowed"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.NewPage(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := retained.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, f, fault.Correlation{Call: "borrowed"}, retained.Receipt(), nil, nil, func(t testing.TB, value Result) {
		if !value.ContextDetached() || value.ContextReleased() {
			t.Fatal("borrowed release claimed disposal")
		}
	})
	if err := f.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if value, err := RunAction(deadline(t), originalPage, Evaluate[int]("42")); err != nil || value != 42 {
		t.Fatal("independent peer unusable", value, err)
	}
	if err := original.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, host, fault.Correlation{Call: "peer"}, original.Receipt(), nil, nil, func(t testing.TB, value Result) {
		if !value.ContextReleased() {
			t.Fatal("owner not disposed")
		}
	})
}
func TestPersistentProfilePreservedAndLocked(t *testing.T) {
	options := browserOptions(t)
	options.TempDir = ""
	options.ProfileDir = t.TempDir()
	sentinel := filepath.Join(options.ProfileDir, "caller-owned")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	f := bindFixture(t, options, 1)
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "persistent"})
	if err != nil {
		t.Fatal(err)
	}
	second := bindFixture(t, options, 1)
	receipt, err := second.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "locked"}, func(*Session) error { t.Error("locked profile entered"); return nil })
	if !errors.Is(err, os.ErrExist) {
		t.Fatal("profile concurrency not refused", err)
	}
	settle(t, second, fault.Correlation{Call: "locked"}, receipt, os.ErrExist, nil, func(testing.TB, Result) {})
	if err := retained.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, f, fault.Correlation{Call: "persistent"}, retained.Receipt(), nil, nil, func(testing.TB, Result) {})
	if err := f.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "preserve" {
		t.Fatal("caller data removed", err)
	}
	if _, err := os.Stat(filepath.Join(options.ProfileDir, ".fathomry-browser.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("owned lock leaked", err)
	}
}
func TestRetainedCloseKeepsActualWorkCharged(t *testing.T) {
	f := bindFixture(t, browserOptions(t), 1)
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "cooperative"})
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_ = retained.Page().Actions(context.Background(), Step(func(context.Context) error { close(entered); <-release; return nil }))
	}()
	<-entered
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = retained.Close(short)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close falsely completed", err)
	}
	if usage := f.assembly.Snapshot().Sources[0].Usage; usage.Active != 1 {
		t.Fatal("actual work refunded")
	}
	close(release)
	<-done
	if err := retained.Close(deadline(t)); !errors.Is(err, context.Canceled) {
		t.Fatal("operation cancellation lost", err)
	}
	settle(t, f, fault.Correlation{Call: "cooperative"}, retained.Receipt(), context.Canceled, nil, func(testing.TB, Result) {})
}
