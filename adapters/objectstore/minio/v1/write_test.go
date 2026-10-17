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
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestPutBoundedSmallInputs(t *testing.T) {
	for _, entry := range []struct {
		name        string
		actual      int
		size, limit int64
	}{
		{"known-empty", 0, 0, 8 << 20},
		{"known-small", 64 << 10, 64 << 10, 8 << 20},
		{"unknown-small-bound", 64 << 10, -1, 64 << 10},
	} {
		t.Run(entry.name, func(t *testing.T) {
			peer := newPeer(t)
			options := peer.options
			options.MaxTransferBytes, options.MaxReadBytes = entry.limit, 1
			test := newFixture(t, options)
			ctx := testContext(t)
			body := bytes.Repeat([]byte("s"), entry.actual)
			key := "owned/small"
			result := test.result(test.client.Put(ctx, ctx, WriteRequest{Key: key, Size: entry.size}, bytes.NewReader(body)))
			object, present := result.Object()
			if !present || object.Size != int64(len(body)) || !bytes.Equal(peer.content(key), body) {
				t.Fatal("public upload content/size")
			}
			stat := test.result(test.client.Stat(ctx, Address{Key: key}))
			object, present = stat.Object()
			if !present || object.Size != int64(len(body)) {
				t.Fatal("independent object existence/size")
			}
		})
	}
}

func TestWriteRefusalPrecedesNativeDispatch(t *testing.T) {
	peer := newPeer(t)
	test := newFixture(t, peer.options)
	ctx := testContext(t)
	before := peer.requests()
	for _, entry := range []struct {
		run  func() (*adapters.Receipt[Result], error)
		want error
	}{
		{func() (*adapters.Receipt[Result], error) {
			return test.client.Copy(ctx, CopyRequest{Source: Address{Key: "owned/a"}, Key: "owned/b", IfAbsent: true})
		}, ErrUnsupported},
		{func() (*adapters.Receipt[Result], error) {
			return test.client.Put(ctx, ctx, WriteRequest{Key: "owned/a", Size: 1 << 30}, strings.NewReader("x"))
		}, ErrInput},
		{func() (*adapters.Receipt[Result], error) {
			return test.client.Put(ctx, ctx, WriteRequest{Key: "outside/a", Size: 1}, strings.NewReader("x"))
		}, ErrAuthority},
	} {
		receipt, err := entry.run()
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil || !errors.Is(snapshot.Err(), entry.want) {
			t.Fatal("incorrect refusal")
		}
		value, _ := snapshot.ValueCopy()
		if value.HasData() {
			t.Fatal("refusal manufactured native data")
		}
		if peer.requests() != before {
			t.Fatal("invalid write performed native I/O")
		}
		delivery, err := test.inbox.NextReleased(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
