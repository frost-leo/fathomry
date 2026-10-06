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
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

type cancelCauseCycle struct{}

func (*cancelCauseCycle) Error() string       { return "cyclic cancellation" }
func (value *cancelCauseCycle) Unwrap() error { return value }

func TestCyclicCancellationCauseReleasesEvidence(t *testing.T) {
	if os.Getenv("FATHOMRY_REDIS_CANCEL_CYCLE_TEST") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCyclicCancellationCauseReleasesEvidence$", "-test.timeout=10s")
		child.Env = append(os.Environ(), "FATHOMRY_REDIS_CANCEL_CYCLE_TEST=1")
		output, err := child.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("projection does not terminate for a cyclic caller cancellation cause")
		}
		if err != nil {
			t.Fatalf("child failed: %v: %s", err, output)
		}
		return
	}
	entered, released := make(chan struct{}), make(chan struct{})
	address := peer(t, func(_ net.Conn, args []string) string {
		close(entered)
		<-released
		return ""
	})
	settings := testSettings(address)
	settings.Timeout = 50 * time.Millisecond
	owner, deps := openTest(t, settings)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	go func() { <-entered; cancel(&cancelCauseCycle{}); close(released) }()
	_, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "GET", "owned"))
	if err == nil {
		t.Fatal("canceled command succeeded")
	}
	ack(t, deps.Evidence, 1)
}
