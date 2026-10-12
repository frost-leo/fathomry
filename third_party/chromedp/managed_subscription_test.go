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
	"testing/synctest"

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
)

func TestManagedSubscriptionOverflowIsLocal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var global, local int
		subs := subscribers{limits: EventBuffer{MaxEvents: 8, MaxBytes: 1024, OnOverflow: func(error) { global++ }}}
		first, stopFirst := subs.subscribeBounded(EventBuffer{MaxEvents: 1, MaxBytes: 100, OnOverflow: func(err error) {
			if !errors.Is(err, ErrEventOverflow) {
				t.Error(err)
			}
			local++
		}}, "Runtime.consoleAPICalled")
		defer stopFirst()
		second, stopSecond := subs.subscribeMany("Runtime.consoleAPICalled")
		defer stopSecond()
		subs.publish("Runtime.consoleAPICalled", jsonv2.Value(`{}`))
		synctest.Wait()
		// The first subscription never receives its pump-held event.
		<-second
		subs.publish("Runtime.consoleAPICalled", jsonv2.Value(`{}`))
		<-second
		synctest.Wait()
		if local != 1 || global != 0 {
			t.Fatal("overflow changed another subscription", local, global)
		}
		if _, ok := <-first; ok {
			t.Fatal("overflow producer stayed live")
		}
		stopFirst()
		stopFirst()
		subs.mu.Lock()
		if len(subs.subs["Runtime.consoleAPICalled"]) != 1 {
			t.Error("canceled subscription remained registered")
		}
		subs.mu.Unlock()
	})
}

func TestManagedObserveSeesMethodBeforeDeliveryAndAfterAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tagged := range []bool{false, true} {
			observed := 0
			subs := subscribers{limits: EventBuffer{MaxEvents: 1, MaxBytes: 1024, Observe: func(method string, raw jsonv2.Value, sequence uint64) error {
				if method != "Page.loadEventFired" || string(raw) != `{}` || sequence != 1 {
					t.Error("observer origin changed", method, sequence)
				}
				observed++
				return nil
			}}}
			var events <-chan jsonv2.Value
			var stop func()
			if tagged {
				events, stop = subs.subscribeMany("Page.loadEventFired")
			} else {
				events, stop = subs.subscribe("Page.loadEventFired")
			}
			subs.publish("Page.loadEventFired", jsonv2.Value(`{}`))
			if observed != 1 {
				t.Fatal("observation waited for consumption")
			}
			synctest.Wait()
			subs.publish("Page.loadEventFired", jsonv2.Value(`{}`))
			if observed != 1 {
				t.Fatal("observation ran before queue admission")
			}
			stop()
			if _, ok := <-events; ok {
				t.Fatal("overflow retained producer")
			}
		}
	})
}
