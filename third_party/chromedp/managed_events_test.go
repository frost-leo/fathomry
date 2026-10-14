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
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"strings"
	"testing"
	"testing/synctest"
)

func TestManagedAllEventsShareOneBoundedQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		subs := subscribers{limits: EventBuffer{MaxEvents: 4, MaxBytes: 1024}}
		events, stop := subs.subscribeMany()
		defer stop()
		for _, method := range []string{"Debugger.paused", "Page.screencastFrame", "Page.fileChooserOpened"} {
			subs.publish(method, jsonv2.Value(`{}`))
			if raw := <-events; !strings.Contains(string(raw), method) {
				t.Fatal("target event omitted", string(raw))
			}
		}
		stop()
		stop()
		if _, ok := <-events; ok {
			t.Fatal("canceled producer remained open")
		}
	})
}

func TestManagedEventQueueRegression(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		subs := subscribers{limits: EventBuffer{MaxEvents: 4, MaxBytes: 1024}}
		_, stop := subs.subscribe("gate")
		defer stop()
		for i := 0; i < 32; i++ {
			subs.publish("gate", jsonv2.Value(`{"payload":"bounded"}`))
		}
		synctest.Wait()
		// The pump holds the first batch while its output is blocked. A second
		// burst must not hide behind that detached batch.
		for i := 0; i < 32; i++ {
			subs.publish("gate", jsonv2.Value(`{"payload":"bounded"}`))
		}
		subs.mu.Lock()
		for sub := range subs.subs["gate"] {
			sub.mu.Lock()
			n := len(sub.queue)
			sub.mu.Unlock()
			if n > 4 {
				t.Errorf("native queue retained %d pending events without a consumer (limit 4)", n)
			}
		}
		subs.mu.Unlock()
	})
}

func TestManagedWaitEventFailedTriggerRegression(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		target := &Target{}
		want := errors.New("trigger")
		_, err := WaitEvent(cdp.Event[int]{Method: "gate"}, nil, Action[Void](func(context.Context, *Target) (Void, error) { return Void{}, want }))(ctx, target)
		if !errors.Is(err, want) {
			t.Fatal(err)
		}
		synctest.Wait()
		target.events.mu.Lock()
		n := len(target.events.subs["gate"])
		target.events.mu.Unlock()
		if n != 0 {
			t.Errorf("failed trigger retained %d native subscribers", n)
		}
		target.events.mu.Lock()
		for sub := range target.events.subs["gate"] {
			sub.stop()
		}
		target.events.mu.Unlock()
	})
}
