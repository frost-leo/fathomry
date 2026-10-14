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

package chromedp_test

import (
	"context"
	"errors"
	"testing"

	browser "github.com/frost-leo/fathomry/adapters/browser/chromedp/v1"
)

func TestZeroAndTypedNilPublicHandlesHaveNoAuthority(t *testing.T) {
	ctx := context.Background()
	var client *browser.Client
	if value, receipt, err := client.Retain(ctx, browser.ContextOptions{}); value != nil || receipt != nil || !errors.Is(err, browser.ErrInput) {
		t.Fatal("nil client accepted", err)
	}
	var retained *browser.Context
	if retained.Page() != nil || retained.Context() != nil || !errors.Is(retained.Close(ctx), browser.ErrInput) {
		t.Fatal("nil retained context grants authority")
	}
	var scope *browser.Scope
	if scope.Page() != nil || scope.Context() != nil {
		t.Fatal("nil scope grants authority")
	}
	var page *browser.Page
	if _, receipt, err := browser.RunAction(ctx, page, browser.Evaluate[int]("1")); receipt != nil || !errors.Is(err, browser.ErrInput) {
		t.Fatal("nil page accepted", err)
	}
	var stream *browser.Stream
	if _, receipt, err := stream.Read(ctx, 1); receipt != nil || !errors.Is(err, browser.ErrInput) {
		t.Fatal("nil stream accepted", err)
	}
	if stream.Closed() {
		t.Fatal("nil stream certifies cleanup")
	}
	var owner *browser.Owner
	if owner.Client() != nil || owner.ShutdownComplete() || !errors.Is(owner.Close(ctx), browser.ErrInput) {
		t.Fatal("nil source grants authority")
	}
}

func TestSourceRepeatableCloseRevokesCapturedClient(t *testing.T) {
	prepared, err := browser.Prepare(browser.Settings{Name: "source", ExecPath: "/explicitly-not-started"}, browser.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := publicDependencies(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	client := owner.Client()
	copied := *owner
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := copied.Close(context.Background()); err != nil || !copied.ShutdownComplete() {
		t.Fatal("copied source lost shared cleanup", err)
	}
	if retained, _, err := client.Retain(context.Background(), browser.ContextOptions{}); retained != nil || err == nil {
		t.Fatal("closed source revived", err)
	}
	status, err := dependencies.Evidence.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	for range status.Outstanding {
		delivery, err := dependencies.Evidence.NextReleased(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
