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
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func TestSubscriptionSaturatedEvidenceStillCloses(t *testing.T) {
	var reads atomic.Int32
	address := peer(t, func(args []string) string {
		reads.Add(1)
		return "*3\r\n$9\r\nsubscribe\r\n$5\r\nowned\r\n:1\r\n"
	})
	client, _, inbox, _ := bindTest(t, testOptions(address), 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var escaped *Subscription
	receipt, err := client.Subscribe(ctx, context.Background(), fault.Correlation{Call: "subscription"}, SubscriptionOptions{Mode: "channel", Channels: []string{"owned"}},
		func(ctx context.Context, subscription *Subscription) error {
			copy := *subscription
			escaped = &copy
			subscription.mu.Lock()
			_, copyErr := copy.Receive(ctx, fault.Correlation{Call: "copied", Parent: "subscription"})
			subscription.mu.Unlock()
			if !errors.Is(copyErr, ErrState) {
				t.Error("copied subscription bypassed its shared gate")
			}
			_, err := subscription.Receive(ctx, fault.Correlation{Call: "message", Parent: "subscription"})
			if !errors.Is(err, invocation.ErrEvidence) {
				t.Error("message read bypassed saturated evidence")
			}
			return nil
		})
	got := resolved(t, receipt, err)
	if got.Err() != nil || !got.Released || client.Stats().Sockets != 0 {
		t.Fatal("saturated subscription cleanup not owned")
	}
	if _, err := escaped.Receive(ctx, fault.Correlation{Call: "escape", Parent: "subscription"}); !errors.Is(err, ErrState) {
		t.Fatal("subscription escaped")
	}
	drain(t, inbox)
}

func TestMalformedPubSubRetiresAndReleasesEveryCall(t *testing.T) {
	address := peer(t, func([]string) string { return "*0\r\n" })
	client, assembly, inbox, _ := bindTest(t, testOptions(address), 4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	receipt, err := client.Subscribe(ctx, context.Background(), fault.Correlation{Call: "parent"}, SubscriptionOptions{Mode: "channel", Channels: []string{"owned"}},
		func(ctx context.Context, sub *Subscription) error {
			child, err := sub.Receive(ctx, fault.Correlation{Call: "child", Parent: "parent"})
			got := resolved(t, child, err)
			if !errors.Is(got.Err(), ErrProtocol) || !got.Released {
				t.Error("malformed reply did not finish child evidence")
			}
			if _, err := sub.Receive(ctx, fault.Correlation{Call: "late", Parent: "parent"}); !errors.Is(err, ErrState) {
				t.Error("retired subscription was reused")
			}
			return got.Err()
		})
	got := resolved(t, receipt, err)
	if !errors.Is(got.Err(), ErrProtocol) || !got.Released {
		t.Fatal("root not completed after native decoder panic")
	}
	drain(t, inbox)
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
