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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/runtime"
)

func TestManagedBindingsIgnoreUnrelatedEventsAtCapacity(t *testing.T) {
	faults := make(chan error, 8)
	ctx, cancel := NewContext(allocCtx, WithBrowserOption(
		WithEventBuffer(EventBuffer{MaxEvents: 32, MaxBytes: 64 << 10}),
		WithBindingLimits(BindingLimits{MaxActive: 1, MaxInputBytes: 4096, OnError: func(err error) {
			select {
			case faults <- err:
			default:
			}
		}}),
	))
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()
	if err := Do(ctx, Navigate("data:text/html,<html><body>binding fixture</body></html>"), ExposeFunc("held", func(context.Context, int) (int, error) {
		close(entered)
		<-release
		return 7, nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, Evaluate[Void]("window.held(1).then(value => window.heldResult=value)")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("positive handler did not start")
	}
	if err := Do(ctx, ExposeFunc("other", func(_ context.Context, value int) (int, error) { return value, nil })); err != nil {
		t.Fatal(err)
	}
	if _, err := cdp.Call(ctx, FromContext(ctx).Target, runtime.AddBinding, runtime.AddBindingParams{Name: "unmanaged"}); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, Evaluate[Void]("window.unmanaged('unrelated')")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-faults:
		t.Errorf("unrelated binding consumed a handler slot: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	resume()
	work, done := context.WithTimeout(ctx, time.Second)
	defer done()
	if value, err := Run(work, Poll[int]("window.heldResult")); err != nil || value != 7 {
		t.Fatal("first handler lost its reply", value, err)
	}
	if value, err := Run(work, Evaluate[int]("window.other(3)", EvalAwaitPromise)); err != nil || value != 3 {
		t.Fatal("unrelated event removed the other listener", value, err)
	}
	if err := FromContext(ctx).Target.JoinBindings(work); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-faults:
		if errors.Is(err, ErrBindingLimit) {
			t.Fatal("spurious handler saturation", err)
		}
		t.Fatal(err)
	default:
	}
}
