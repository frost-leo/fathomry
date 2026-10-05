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

	"github.com/frost-leo/fathomry/adapters/objectstore/v1"
)

type heldWriter struct {
	entered chan struct{}
	release chan struct{}
	cause   error
}

func (writer *heldWriter) Write(body []byte) (int, error) {
	close(writer.entered)
	<-writer.release
	return len(body), writer.cause
}

func TestPublicBlockedSinkRetainsLateCause(t *testing.T) {
	peer := newPeer(t)
	test := newFixture(t, peer.options)
	ctx := testContext(t)
	test.result(test.client.Put(ctx, ctx, WriteRequest{Key: "owned/sink", Size: 3}, strings.NewReader("abc")))
	cause := errors.New("private-sink-cause")
	sink := &heldWriter{entered: make(chan struct{}), release: make(chan struct{}), cause: cause}
	receipt, err := test.client.Download(ctx, ReadRequest{Address: Address{Key: "owned/sink"}}, sink)
	if err != nil {
		t.Fatal(err)
	}
	<-sink.entered
	waiting, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := receipt.WaitReleased(waiting); err == nil {
		t.Fatal("waiting timeout released sink")
	}
	if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("sink custody ended")
	}
	close(sink.release)
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || !errors.Is(snapshot.Err(), cause) {
		t.Fatal("late sink error lost")
	}
	result, present := snapshot.ValueCopy()
	if !present || result.Transfer().Effect != objectstore.Unknown || result.Transfer().Bytes != 3 {
		t.Fatal("partial sink evidence lost")
	}
}
