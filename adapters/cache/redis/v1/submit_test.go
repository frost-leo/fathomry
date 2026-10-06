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
	"sync"
	"testing"
)

func TestSubmitRetainsQueuedEffectsAfterOperationCancellation(t *testing.T) {
	operation, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstEntered := make(chan struct{})
	unblock := make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	defer release()
	secondEntered := make(chan bool, 1)
	address := peer(t, func(_ net.Conn, args []string) string {
		if args[1] == "first" {
			close(firstEntered)
			<-unblock
		} else {
			secondEntered <- operation.Err() != nil
		}
		return "+OK\r\n"
	})
	settings := testSettings(address)
	settings.ExperimentalAutoPipeline = true
	settings.MaxActive = 2
	owner, deps := openTest(t, settings)
	first, err := owner.Client().Cache().Submit(context.Background(), command(t, Cache, "GET", "first"))
	if err != nil {
		t.Fatal(err)
	}
	<-firstEntered
	second, err := owner.Client().Cache().Submit(operation, command(t, Cache, "SET", "second", "value"))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	release()
	firstResult, err := first.WaitReleased(testContext(t))
	if err != nil || firstResult.Err() != nil {
		t.Fatal("first command did not complete", err, firstResult.Err())
	}
	secondResult, err := second.WaitReleased(testContext(t))
	if err != nil || secondResult.Err() != nil {
		t.Fatal("second command did not complete", err, secondResult.Err())
	}
	if !<-secondEntered {
		t.Fatal("second command arrived before operation cancellation")
	}
	ack(t, deps.Evidence, 2)
	t.Log("confirmed: queued SET reached the peer after its supplied operation context was canceled; receipt completed successfully")
}
