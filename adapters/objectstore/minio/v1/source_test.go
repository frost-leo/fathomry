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

package minio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

type blockedReader struct {
	entered, release chan struct{}
	once             bool
	closed           bool
}

func (reader *blockedReader) Read(body []byte) (int, error) {
	if reader.once {
		return 0, io.EOF
	}
	reader.once = true
	close(reader.entered)
	<-reader.release
	body[0] = 'x'
	return 1, nil
}

func (reader *blockedReader) Close() error { reader.closed = true; return nil }

func TestPublicBlockedInputAndOwnerTimeout(t *testing.T) {
	peer := newPeer(t)
	test := newFixture(t, peer.options)
	ctx := testContext(t)
	reader := &blockedReader{entered: make(chan struct{}), release: make(chan struct{})}
	receipt, err := test.client.Put(ctx, ctx, WriteRequest{Key: "owned/blocked", Size: 1}, reader)
	if err != nil {
		t.Fatal(err)
	}
	<-reader.entered
	wait, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()
	if err := test.owner.Close(wait); err == nil {
		t.Fatal("blocked caller input released")
	}
	if test.owner.ShutdownComplete() {
		t.Fatal("shutdown fabricated")
	}
	if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("public guard ended early")
	}
	close(reader.release)
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || !errors.Is(snapshot.Err(), context.Canceled) {
		t.Fatal("late cancellation evidence", err)
	}
	if reader.closed {
		t.Fatal("caller reader closed")
	}
	if err := test.owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPublicPartialConstructionRetainsOwner(t *testing.T) {
	peer := newPeer(t)
	peer.mu.Lock()
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		peerError(writer, 403, "AccessDenied")
		return true
	}
	peer.mu.Unlock()
	policy, _ := Recommend(peer.options)
	runtime, _ := adapters.New(context.Background(), policy.Runtime)
	inbox, _ := adapters.NewInbox[Result](policy.Evidence)
	owner, err := Open(context.Background(), peer.options, Dependencies{Runtime: runtime, Evidence: inbox})
	if owner == nil || err == nil || owner.Client() != nil {
		t.Fatal("partial owner disappeared")
	}
	_ = owner.Close(testContext(t))
	if !owner.ShutdownComplete() {
		t.Fatal("partial owner not released")
	}
	if err := runtime.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, inbox)
	if strings.Contains(fmt.Sprint(err), "private-service-canary") {
		t.Fatal("diagnostic leak")
	}
}
