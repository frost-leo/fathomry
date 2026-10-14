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
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
)

func TestLivePageClosePreservesContextAndOtherPages(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	initial := retained.Page()
	other, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if err := initial.Close(bounded(t)); err != nil || !initial.Closed() {
		t.Fatal("initial page did not close", err)
	}
	if retained.Context().Err() != nil {
		t.Fatal("page close canceled its BrowserContext")
	}
	if _, _, err := RunAction(bounded(t), initial, Evaluate[int]("1")); !errors.Is(err, ErrState) {
		t.Fatal("closed page admitted work", err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), other, Evaluate[int]("42")); err != nil || value != 42 {
		t.Fatal("closing initial page revoked its sibling", err)
	}
	drain(t, dependencies, 1)
	if _, err := retained.SetCookies(bounded(t), []*network.CookieParam{{Name: "after-close", Value: "retained", URL: "http://cookie.invalid/"}}); err != nil {
		t.Fatal("context command depended on initial page lifetime", err)
	}
	drain(t, dependencies, 1)
	cookies, _, err := retained.Cookies(bounded(t))
	if err != nil || len(cookies) != 1 || cookies[0].Name != "after-close" {
		t.Fatal("context storage not retained", err)
	}
	drain(t, dependencies, 1)
	created, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal("released page slot was not reusable", err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), created, Evaluate[int]("73")); err != nil || value != 73 {
		t.Fatal("new page inherited closed initial accounting", err)
	}
	drain(t, dependencies, 1)
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal(err)
	}
	result, err := retained.call.Receipt().WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	facts, _ := result.ValueCopy()
	if facts.PagesClosed() != 1 || facts.PagesDetached() != 0 {
		t.Fatal("individual page disposal lost independent context evidence")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLivePageCleanupUsesExistingReservationForIgnoredChildren(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<h1>page children</h1>", 64)
	current := retained.Page()
	stream, _, err := current.PDFStream(retained.Context(), page.PrintToPDFParams{})
	if err != nil {
		t.Fatal(err)
	}
	object, _, err := EvaluateObject(retained.Context(), current, "({answer:42})")
	if err != nil {
		t.Fatal(err)
	}
	subscription, _, err := Subscribe(retained.Context(), current, network.ResponseReceived)
	if err != nil {
		t.Fatal(err)
	}
	binding, _, err := BindFunc(retained.Context(), current, "pageFixture", func(context.Context, int) (int, error) { return 42, nil })
	if err != nil {
		t.Fatal(err)
	}
	control, _, err := current.Control(retained.Context(), ControlOptions{FileChooser: true})
	if err != nil {
		t.Fatal(err)
	}
	fillControlInbox(t, retained, dependencies)
	if err := current.Close(bounded(t)); err != nil || !current.Closed() {
		t.Fatal("page cleanup required fresh admission", err)
	}
	if !stream.Closed() || !object.Closed() || !subscription.Closed() || !binding.Closed() || !control.Closed() {
		t.Fatal("page cleanup lost an ignored child")
	}
	if retained.Context().Err() != nil {
		t.Fatal("child cleanup canceled the context")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("saturated page cleanup blocked source release")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLivePageCloseJoinsOnlyItsActualDecoder(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>page decoder</p>", 64)
	blocked, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	valueDecodeEntered = make(chan struct{})
	valueDecodeExit = make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = RunAction(bounded(t), blocked, Evaluate[blockedValueDecoder]("1"))
	}()
	<-valueDecodeEntered
	if err := retained.Page().Close(bounded(t)); err != nil {
		close(valueDecodeExit)
		<-done
		t.Fatal("unrelated decoder prevented initial page close", err)
	}
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = blocked.Close(short)
	cancel()
	if err == nil || blocked.Closed() {
		close(valueDecodeExit)
		<-done
		t.Fatal("active decoder lost page custody", err)
	}
	close(valueDecodeExit)
	<-done
	_ = blocked.Close(bounded(t))
	if !blocked.Closed() {
		t.Fatal("joined decoder retained a closed page")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("page decoder cleanup did not settle source")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedPageCloseDetachesOnlyThatSession(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	current := retained.Page()
	if err := current.Close(bounded(t)); err != nil || !current.Closed() {
		t.Fatal("borrowed page did not detach", err)
	}
	if retained.Context().Err() != nil {
		t.Fatal("borrowed attachment closed the retained context")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("37")); err != nil || value != 37 {
		t.Fatal("page detach killed its external peer", err)
	}
	child, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal("borrowed context could not create its own child after root detach", err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), child, Evaluate[int]("19")); err != nil || value != 19 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal(err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("41")); err != nil || value != 41 {
		t.Fatal("owned child cleanup killed external peer", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

var pageMetadataEntered, pageMetadataRelease chan struct{}

type blockedPageMetadata struct{ Value int }

func (value blockedPageMetadata) MarshalJSON() ([]byte, error) {
	close(pageMetadataEntered)
	<-pageMetadataRelease
	return []byte(`{"Value":42}`), nil
}

func TestLivePageClosePreservesPublicMetadataJoinFailure(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	pageMetadataEntered = make(chan struct{})
	pageMetadataRelease = make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = RunAction(bounded(t), retained.Page(), Evaluate[blockedPageMetadata]("({Value:42})"))
	}()
	<-pageMetadataEntered
	current := retained.Page()
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err := current.Close(short)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || current.Closed() {
		close(pageMetadataRelease)
		<-done
		t.Fatal("public metadata work was released before returning", err)
	}
	close(pageMetadataRelease)
	<-done
	if err := current.Close(bounded(t)); !errors.Is(err, context.DeadlineExceeded) || !current.Closed() {
		t.Fatal("successful retry discarded public cleanup history", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("metadata completion did not settle source")
	}
	snapshot, err := retained.call.Receipt().WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Cleanup(), context.DeadlineExceeded) {
		t.Fatal("ignored page-close error vanished from context evidence", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
