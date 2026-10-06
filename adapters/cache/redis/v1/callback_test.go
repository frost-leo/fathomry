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

package redis

import (
	"context"
	"errors"
	"net"
	"runtime"
	"testing"
)

func TestCallbackGoexitRetainsLifecycleEvidence(t *testing.T) {
	for _, mode := range []string{"watch", "dedicated", "subscription"} {
		t.Run(mode, func(t *testing.T) {
			address := peer(t, func(net.Conn, []string) string { return "+OK\r\n" })
			owner, dependencies := openTest(t, testSettings(address))
			cleanup, stopCleanup := context.WithCancel(context.Background())
			stopCleanup()
			var captured *operation
			var escapedSession *Session
			var escapedSubscription *Subscription
			finished := make(chan struct{})
			returned := false
			go func() {
				defer close(finished)
				switch mode {
				case "subscription":
					_, _ = owner.Client().Messaging().Subscribe(testContext(t), cleanup,
						SubscriptionOptions{Mode: "channel", Channels: []string{"owned"}},
						func(_ context.Context, subscription *Subscription) error {
							captured, escapedSubscription = subscription.state.op, subscription
							runtime.Goexit()
							return nil
						})
				default:
					callback := func(_ context.Context, session *Session) error {
						captured, escapedSession = session.state.op, session
						runtime.Goexit()
						return nil
					}
					if mode == "watch" {
						_, _ = owner.Client().Cache().Watch(testContext(t), cleanup, []string{"owned"}, callback)
					} else {
						_, _ = owner.Client().Cache().Dedicated(testContext(t), cleanup, "owned", callback)
					}
				}
				returned = true
			}()
			select {
			case <-finished:
			case <-testContext(t).Done():
				t.Fatal("callback termination did not complete local cleanup")
			}
			if returned || captured == nil || captured.record == nil {
				t.Fatal("callback termination semantics changed or native custody was not claimed")
			}
			native, _ := captured.record.Receipt().Result()
			if !native.Final || !native.Released || native.Outcome.Primary == nil {
				t.Fatal("native termination control did not complete")
			}
			delivery, err := dependencies.Evidence.NextReleased(testContext(t))
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := delivery.Receipt()
			if err != nil {
				t.Fatal(err)
			}
			snapshot, available := receipt.Snapshot()
			if !available {
				t.Fatal("callback lifecycle outcome unavailable")
			}
			value, present := snapshot.ValueCopy()
			expected := ErrState
			if mode == "subscription" {
				expected = ErrMessagingState
			}
			if !snapshot.Info().Released || !present || value.Kind() != Lifecycle || !errors.Is(snapshot.Primary(), expected) {
				t.Fatal("completed native lifecycle evidence was dropped")
			}
			if mode != "subscription" && (!errors.Is(native.Outcome.Cleanup, context.Canceled) || !errors.Is(snapshot.Cleanup(), context.Canceled)) {
				t.Fatal("native cleanup evidence was lost during callback unwinding")
			}
			if captured.family.inbox.Usage().Outstanding != 0 || owner.PendingTransfers() != 0 {
				t.Fatal("normal transfer leaked native custody or public holds")
			}
			if escapedSession != nil {
				if _, err := escapedSession.Execute(testContext(t), command(t, Cache, "PING")); !errors.Is(err, ErrState) {
					t.Fatal("terminated session remained usable")
				}
			}
			if escapedSubscription != nil {
				if _, err := escapedSubscription.Receive(testContext(t)); !errors.Is(err, ErrMessagingState) {
					t.Fatal("terminated subscription remained usable")
				}
			}
			if err := delivery.Ack(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
				t.Fatal("source ownership remained after callback cleanup", err)
			}
		})
	}
}
