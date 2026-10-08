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

package quic

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sardanioss/quic-go/internal/utils"
	"github.com/sardanioss/quic-go/internal/wire"
)

type enteredDatagramContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *enteredDatagramContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

func TestFathomryDatagramQueueWaitCancellationAndAcceptance(t *testing.T) {
	queue := newDatagramQueue(func() {}, utils.DefaultLogger)
	for range maxDatagramSendQueueLen {
		if err := queue.Add(&wire.DatagramFrame{Data: []byte{1}}); err != nil {
			t.Fatal(err)
		}
	}
	original, cancel := context.WithCancelCause(context.Background())
	ctx := &enteredDatagramContext{Context: original, entered: make(chan struct{})}
	completed := make(chan error, 1)
	go func() { completed <- queue.AddContext(ctx, &wire.DatagramFrame{Data: []byte{2}}) }()
	select {
	case <-ctx.entered:
	case <-time.After(time.Second):
		t.Fatal("enqueue did not enter the full-queue wait")
	}
	cause := errors.New("this tunnel ended")
	cancel(cause)
	select {
	case err := <-completed:
		if !errors.Is(err, cause) {
			t.Fatal("cancel cause lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("entered queue wait did not cancel")
	}
	queue.sendMx.Lock()
	count := queue.sendQueue.Len()
	queue.sendMx.Unlock()
	if count != maxDatagramSendQueueLen {
		t.Fatal("failed enqueue changed queue", count)
	}
	queue.Pop()
	accepted, stop := context.WithCancel(context.Background())
	if err := queue.AddContext(accepted, &wire.DatagramFrame{Data: []byte{3}}); err != nil {
		t.Fatal("healthy sibling lost queue", err)
	}
	stop()
	for range maxDatagramSendQueueLen - 1 {
		queue.Pop()
	}
	if frame := queue.Peek(); frame == nil || len(frame.Data) != 1 || frame.Data[0] != 3 {
		t.Fatal("later cancellation erased accepted packet")
	}
	queue.Pop()
	queue.CloseWithError(cause)
	if err := queue.AddContext(context.Background(), &wire.DatagramFrame{}); !errors.Is(err, cause) {
		t.Fatal("closed empty queue accepted new packet", err)
	}
}
