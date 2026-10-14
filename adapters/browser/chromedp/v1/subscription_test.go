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
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	runtime "github.com/chromedp/cdproto/runtime"
)

func subscriptionFixture(t *testing.T, maxEvents int) (*Owner, *Context, Dependencies) {
	t.Helper()
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	p, err := Prepare(Settings{Name: "subscriptions", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(1), MaxEvents: new(maxEvents), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := mechanisms(t, p)
	owner, err := p.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<h1>subscription fixture</h1>"); err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	return owner, retained, d
}

func TestLiveSubscriptionsHaveIndependentNativeQueuesAndEvidence(t *testing.T) {
	owner, retained, d := subscriptionFixture(t, 32)
	page := retained.Page()
	first, _, err := Subscribe(retained.Context(), page, runtime.ConsoleAPICalled)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := Subscribe(retained.Context(), page, runtime.ConsoleAPICalled)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = RunAction(bounded(t), page, Evaluate[int](`console.log("sensitive-subscription"); 17`))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	for _, sub := range []*Subscription[runtime.EventConsoleAPICalled]{first, second} {
		event, receipt, err := sub.Next(bounded(t))
		if err != nil || len(event.Args) != 1 || string(event.Args[0].Value) != `"sensitive-subscription"` {
			t.Fatal("missing independent event", err)
		}
		event.Args[0].Value[1] = 'X'
		snapshot, err := receipt.WaitReleased(bounded(t))
		if err != nil {
			t.Fatal(err)
		}
		evidence, _ := snapshot.ValueCopy()
		if !bytes.Contains(evidence.PayloadCopy(), []byte("sensitive-subscription")) {
			t.Fatal("mutable native output aliased evidence")
		}
		drain(t, d, 1)
		if err := sub.Close(bounded(t)); err != nil || !sub.Closed() {
			t.Fatal(err)
		}
		drain(t, d, 1)
	}
	if _, _, err := first.Next(bounded(t)); err == nil {
		t.Fatal("closed subscription revived")
	}
	if err := retained.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	status, _ := d.Evidence.Inspect()
	drain(t, d, status.Outstanding)
}

func TestLiveSubscriptionNeverReadCancellationAndEarlyBreak(t *testing.T) {
	_, retained, d := subscriptionFixture(t, 32)
	ctx, cancel := context.WithCancel(retained.Context())
	sub, receipt, err := Subscribe(ctx, retained.Page(), runtime.ConsoleAPICalled)
	if err != nil {
		t.Fatal(err)
	}
	_ = sub.Events(bounded(t))
	cancel()
	if _, err := receipt.WaitReleased(bounded(t)); err != nil || !sub.Closed() {
		t.Fatal("unstarted iterator leaked subscription", err)
	}
	drain(t, d, 1)
	sub, receipt, err = Subscribe(retained.Context(), retained.Page(), runtime.ConsoleAPICalled)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`console.log("early"); 1`)); err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	for event, err := range sub.Events(bounded(t)) {
		if err != nil || len(event.Args) == 0 {
			t.Fatal(err)
		}
		break
	}
	if _, err := receipt.WaitReleased(bounded(t)); err != nil || !sub.Closed() {
		t.Fatal("early break failed to join native subscription", err)
	}
	drain(t, d, 2)
}

func TestLiveSubscriptionOverflowDoesNotCancelPage(t *testing.T) {
	_, retained, d := subscriptionFixture(t, 4)
	sub, receipt, err := Subscribe(retained.Context(), retained.Page(), runtime.ConsoleAPICalled)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`for(let i=0;i<12;i++) console.log(i); 19`)); err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Err(), ErrLimit) || !sub.Closed() {
		t.Fatal("overflow disappeared or producer stayed live", err, snapshot.Err())
	}
	if !errors.Is(snapshot.Primary(), ErrLimit) || snapshot.Cleanup() != nil {
		t.Fatal("producer failure was mislabeled cleanup failure", snapshot.Cleanup())
	}
	drain(t, d, 1)
	if got, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`23`)); err != nil || got != 23 {
		t.Fatal("optional queue overflow canceled page", err)
	}
	drain(t, d, 1)
}

func TestSubscriptionInvalidDescriptorAndCanceledReceive(t *testing.T) {
	var sub *Subscription[runtime.EventConsoleAPICalled]
	if _, _, err := sub.Next(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if err := sub.Close(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	_, retained, d := subscriptionFixture(t, 32)
	bad, receipt, err := Subscribe(retained.Context(), retained.Page(), cdp.Event[struct{}]{Method: runtime.ConsoleAPICalled.Method})
	if err == nil || bad != nil || receipt == nil {
		t.Fatal("forged native event type accepted", err)
	}
	drain(t, d, 1)
	live, _, err := Subscribe(retained.Context(), retained.Page(), runtime.ConsoleAPICalled)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, _, err := live.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	drain(t, d, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`console.log("after"); 1`)); err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	if _, _, err := live.Next(bounded(t)); err != nil {
		t.Fatal("one canceled receive destroyed subscription", err)
	}
	drain(t, d, 1)
	_ = live.Close(bounded(t))
}

func TestLiveSubscriptionCloseJoinsActualIteratorCallback(t *testing.T) {
	owner, retained, d := subscriptionFixture(t, 32)
	sub, receipt, err := Subscribe(retained.Context(), retained.Page(), runtime.ConsoleAPICalled)
	if err != nil {
		t.Fatal(err)
	}
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	go func() {
		defer close(exited)
		for _, err := range sub.Events(context.Background()) {
			if err != nil {
				return
			}
			close(entered)
			<-release
			break
		}
	}()
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`console.log("blocked callback"); 1`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-bounded(t).Done():
		t.Fatal("iterator did not enter callback")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := owner.Close(ctx); err == nil || owner.ShutdownComplete() || sub.Closed() {
		t.Fatal("cancellation falsely released actual callback", err)
	}
	if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("callback receipt released early")
	}
	once.Do(func() { close(release) })
	select {
	case <-exited:
	case <-bounded(t).Done():
		t.Fatal("iterator failed to join after returning")
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("source did not finish after actual callback exit", err)
	}
	status, _ := d.Evidence.Inspect()
	drain(t, d, status.Outstanding)
}

func TestLiveManagedConsoleUsesNativeOrderedFormatting(t *testing.T) {
	_, retained, dependencies := subscriptionFixture(t, 32)
	console, _, err := Console(retained.Context(), retained.Page())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = RunAction(bounded(t), retained.Page(), Evaluate[int](`console.log("value=%s %d","native",7);setTimeout(()=>{throw new Error("synthetic-exception")},0);1`))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	message, _, err := console.Next(bounded(t))
	if err != nil || message.Text != "value=native 7" || message.IsException() {
		t.Fatal("native console formatting changed", err)
	}
	drain(t, dependencies, 1)
	message, _, err = console.Next(bounded(t))
	if err != nil || !message.IsException() || !strings.Contains(message.Text, "synthetic-exception") {
		t.Fatal("exception ordering or formatting lost", err)
	}
	drain(t, dependencies, 1)
	if err := console.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
}
