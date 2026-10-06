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
	"testing"
	"time"
)

type indirectlyNonComparableCause struct{ data any }

func (indirectlyNonComparableCause) Error() string { return "foreign cancellation cause" }

func TestIndirectlyNonComparableCancellationReleasesEvidence(t *testing.T) {
	entered, released := make(chan struct{}), make(chan struct{})
	address := peer(t, func(_ net.Conn, _ []string) string {
		close(entered)
		<-released
		return ""
	})
	settings := testSettings(address)
	settings.Timeout = 50 * time.Millisecond
	owner, deps := openTest(t, settings)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	go func() { <-entered; cancel(indirectlyNonComparableCause{data: []int{1}}); close(released) }()
	defer func() {
		if cause := recover(); cause != nil {
			t.Errorf("public Execute panicked while transferring cancellation evidence: %v", cause)
		}
	}()
	_, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "GET", "owned"))
	if err == nil {
		t.Fatal("canceled command succeeded")
	}
	ack(t, deps.Evidence, 1)
}
