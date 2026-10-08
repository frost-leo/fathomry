// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package quic

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nukilabs/quic-go/internal/utils"
	"github.com/nukilabs/quic-go/internal/wire"
)

func TestFathomryDatagramQueueCancellationDoesNotCloseSibling(t *testing.T) {
	queue := newDatagramQueue(func() {}, utils.DefaultLogger)
	for range maxDatagramSendQueueLen {
		if err := queue.Add(&wire.DatagramFrame{Data: []byte{1}}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("local tunnel canceled")
	done := make(chan error, 1)
	go func() { done <- queue.AddContext(ctx, &wire.DatagramFrame{Data: []byte{2}}) }()
	cancel(cause)
	select {
	case err := <-done:
		if !errors.Is(err, cause) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("local canceled enqueue stayed blocked")
	}
	queue.sendMx.Lock()
	count := queue.sendQueue.Len()
	queue.sendMx.Unlock()
	if count != maxDatagramSendQueueLen {
		t.Fatal("canceled frame was enqueued", count)
	}
	queue.Pop()
	if err := queue.AddContext(context.Background(), &wire.DatagramFrame{Data: []byte{3}}); err != nil {
		t.Fatal("healthy sibling queue closed", err)
	}
	queue.CloseWithError(cause)
	if err := queue.AddContext(context.Background(), &wire.DatagramFrame{}); !errors.Is(err, cause) {
		t.Fatal("closed queue accepted frame", err)
	}
}
