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
	"errors"
	"io"
	"testing"
)

func TestFathomryReceiveAbortPreservesReliablePrefixAndEOF(t *testing.T) {
	for _, scenario := range []string{"local", "remote-reliable", "shutdown", "eof-priority"} {
		t.Run(scenario, func(t *testing.T) {
			stream := newReceiveStream(4, nil, newTestStreamFlowController(4))
			if stream.fathomryAbort != nil {
				t.Fatal("unused notification allocated")
			}
			changed, err := stream.FathomryReadAbort()
			if err != nil {
				t.Fatal(err)
			}
			cause := errors.New("connection ended")
			stream.mutex.Lock()
			switch scenario {
			case "local":
				stream.cancelReadImpl(7)
			case "remote-reliable":
				stream.cancelledRemotely = true
				stream.cancelErr = &StreamError{StreamID: 4, ErrorCode: 9, Remote: true}
				stream.reliableSize = 4
			case "shutdown":
				stream.closeForShutdownErr = cause
			case "eof-priority":
				stream.currentFrameIsLast = true
				stream.currentFrame = nil
				stream.closeForShutdownErr = cause
			}
			stream.publishFathomryAbort()
			stream.mutex.Unlock()
			if scenario == "remote-reliable" || scenario == "eof-priority" {
				select {
				case <-changed:
					t.Fatal("notification ended readable reliable/FIN state")
				default:
				}
				if scenario == "eof-priority" {
					_, _, _, err := stream.readImpl(nil)
					if err != io.EOF {
						t.Fatal("native EOF priority changed", err)
					}
					return
				}
				stream.mutex.Lock()
				stream.readPos = 4
				stream.publishFathomryAbort()
				stream.mutex.Unlock()
			}
			select {
			case <-changed:
			default:
				t.Fatal("effective receive failure was not published")
			}
			_, err = stream.FathomryReadAbort()
			if err == nil {
				t.Fatal("receive cause missing")
			}
		})
	}
}
