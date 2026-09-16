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
	"testing"
	"time"

	"github.com/nukilabs/quic-go/internal/protocol"
	"go.uber.org/mock/gomock"
)

func TestFathomryCanceledControlWriteLeavesNoPartialInstruction(t *testing.T) {
	id := protocol.StreamID(42)
	sender := NewMockStreamSender(gomock.NewController(t))
	flow := newTestStreamFlowControllerWithSendWindow(id, 0)
	stream := newSendStream(context.Background(), id, sender, flow, false)
	ctx, cancel := context.WithCancelCause(context.Background())
	result := make(chan error, 1)
	go func() {
		count, err := stream.WriteAllContext(ctx, []byte{3, 0x84})
		if count != 0 {
			err = errors.New("partial instruction accepted")
		}
		result <- err
	}()
	cause := errors.New("synthetic-canceled-write")
	cancel(cause)
	select {
	case err := <-result:
		if !errors.Is(err, cause) {
			t.Fatal("cancellation cause lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("control write did not stop")
	}
	stream.mutex.Lock()
	queued := stream.nextFrame != nil || len(stream.dataForWriting) != 0
	stream.mutex.Unlock()
	if queued {
		t.Fatal("canceled instruction corrupted shared control stream")
	}
}
