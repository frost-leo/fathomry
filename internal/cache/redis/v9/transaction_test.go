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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
)

func TestSessionCleanupAtEvidenceSaturation(t *testing.T) {
	var unwatch atomic.Int32
	address := peer(t, func(args []string) string {
		if strings.EqualFold(args[0], "unwatch") {
			unwatch.Add(1)
		}
		return "+OK\r\n"
	})
	options := testOptions(address)
	options.MaxActive = 1
	client, _, inbox, _ := bindTest(t, options, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var escaped *Session
	receipt, err := client.Watch(ctx, context.Background(), fault.Correlation{Call: "watch"}, []string{"owned"}, func(ctx context.Context, session *Session) error {
		copy := *session
		escaped = &copy
		session.mu.Lock()
		_, copyErr := copy.Execute(ctx, fault.Correlation{Call: "copied", Parent: "watch"}, NewCommand("GET", "owned"))
		session.mu.Unlock()
		if !errors.Is(copyErr, ErrState) {
			t.Error("copied session bypassed its shared gate")
		}
		child, err := session.Execute(ctx, fault.Correlation{Call: "child", Parent: "watch"}, NewCommand("SET", "owned", "value"))
		return resultError(child, err)
	})
	got := resolved(t, receipt, err)
	if got.Err() != nil || unwatch.Load() != 1 || client.Stats().Sockets != 0 {
		t.Fatal("cleanup needed another slot or retained sticky connection")
	}
	if _, err := escaped.Execute(ctx, fault.Correlation{Call: "escaped", Parent: "watch"}, NewCommand("GET", "owned")); !errors.Is(err, ErrState) {
		t.Fatal("session escaped lifetime")
	}
	drain(t, inbox)
}

func TestSessionExpiredCleanupStillDiscards(t *testing.T) {
	address := peer(t, func([]string) string { return "+OK\r\n" })
	client, _, inbox, _ := bindTest(t, testOptions(address), 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cleanup, stop := context.WithCancel(context.Background())
	stop()
	receipt, err := client.Watch(ctx, cleanup, fault.Correlation{Call: "watch"}, []string{"owned"}, func(context.Context, *Session) error { return nil })
	got := resolved(t, receipt, err)
	if !errors.Is(got.Outcome.Cleanup, context.Canceled) || !got.Released || client.Stats().Sockets != 0 {
		t.Fatal("cancelled cleanup leaked ownership")
	}
	drain(t, inbox)
}
