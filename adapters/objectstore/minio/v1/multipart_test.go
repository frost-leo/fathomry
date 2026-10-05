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
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestPublicPartInputRetainedAcrossClose(t *testing.T) {
	peer := newPeer(t)
	test := newFixture(t, peer.options)
	ctx := testContext(t)
	session, root, err := test.client.BeginMultipart(ctx, ctx, ctx, WriteRequest{Key: "owned/part-held", Size: 1})
	if err != nil || session == nil {
		t.Fatal("begin", err)
	}
	reader := &blockedReader{entered: make(chan struct{}), release: make(chan struct{})}
	part, err := session.Part(ctx, 1, 1, reader)
	if err != nil {
		t.Fatal(err)
	}
	<-reader.entered
	waiting, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := session.Close(waiting); err == nil {
		t.Fatal("session dropped borrowed reader")
	}
	if snapshot, _ := root.Snapshot(); snapshot.Info().Released {
		t.Fatal("source released while part active")
	}
	close(reader.release)
	if snapshot, err := part.WaitReleased(ctx); err != nil || !errors.Is(snapshot.Err(), context.Canceled) {
		t.Fatal("part cancellation", err)
	}
	final, err := root.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	facts, _ := final.ValueCopy()
	if !facts.Transfer().AbortAcknowledged || reader.closed {
		t.Fatal("part cleanup or input ownership")
	}
}

func TestPublicMultipartSaturatedFinalization(t *testing.T) {
	peer := newPeer(t)
	settings := peer.options
	settings.MaxActive = 1
	policy, err := Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	policy.Evidence.Capacity = 3
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := Open(context.Background(), settings, Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(testContext(t)); _ = runtime.Close(testContext(t)); drain(t, inbox) })
	session, root, err := owner.Client().BeginMultipart(testContext(t), testContext(t), testContext(t), WriteRequest{Key: "owned/saturated", Size: 1})
	if err != nil || session == nil {
		t.Fatal("begin", err)
	}
	part, err := session.Part(testContext(t), 1, 1, strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	if value, err := part.WaitReleased(testContext(t)); err != nil || value.Err() != nil {
		t.Fatal("part", err)
	}
	if status, _ := inbox.Inspect(); status.Outstanding != 3 {
		t.Fatal("not saturated")
	}
	if _, err := session.Complete(testContext(t)); err != nil {
		t.Fatal("finalization re-admitted", err)
	}
	final, err := root.WaitReleased(testContext(t))
	if err != nil || final.Err() != nil {
		t.Fatal("finalization", err, final.Err())
	}
}
