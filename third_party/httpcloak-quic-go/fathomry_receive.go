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

// FathomryReceiveAbort observes an effective receive-side abort without treating
// normal FIN or send-half completion as cancellation. It starts no worker and
// does not consume data or change native reliable-reset/EOF precedence.
func (s *ReceiveStream) FathomryReceiveAbort() <-chan struct{} {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.abortSignal == nil {
		s.abortSignal = make(chan struct{})
	}
	s.signalFathomryAbort()
	return s.abortSignal
}

// FathomryReceiveError returns an observed abort, or nil before one is effective.
func (s *ReceiveStream) FathomryReceiveError() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.signalFathomryAbort()
	return s.abortCause
}

func (s *ReceiveStream) signalFathomryAbort() {
	if s.abortCause != nil || s.abortSignal == nil {
		return
	}
	if s.currentFrameIsLast && s.currentFrame == nil {
		return
	}
	var cause error
	if s.cancelledLocally || s.isRemoteCancellationEffective() {
		cause = s.cancelErr
	} else {
		cause = s.closeForShutdownErr
	}
	if cause != nil {
		s.abortCause = cause
		close(s.abortSignal)
	}
}

func (s *Stream) FathomryReceiveAbort() <-chan struct{} { return s.receiveStr.FathomryReceiveAbort() }
func (s *Stream) FathomryReceiveError() error           { return s.receiveStr.FathomryReceiveError() }
