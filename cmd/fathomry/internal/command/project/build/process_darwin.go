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

package build

import (
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
)

func waitProcessExit(pid int) (err error) {
	syscall.ForkLock.RLock()
	queue, err := unix.Kqueue()
	if err == nil {
		unix.CloseOnExec(queue)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, unix.Close(queue)) }()
	var change unix.Kevent_t
	unix.SetKevent(&change, pid, unix.EVFILT_PROC, unix.EV_ADD|unix.EV_ENABLE|unix.EV_ONESHOT)
	change.Fflags = unix.NOTE_EXIT
	for {
		_, err = unix.Kevent(queue, []unix.Kevent_t{change}, nil, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		// The owned child may have exited before registration. Its unreaped
		// status still reserves its PID until runCommand calls Wait.
		if errors.Is(err, unix.ESRCH) {
			return nil
		}
		if err != nil {
			return err
		}
		break
	}
	for {
		var events [1]unix.Kevent_t
		count, err := unix.Kevent(queue, nil, events[:], nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if count != 1 || events[0].Ident != uint64(pid) || events[0].Filter != unix.EVFILT_PROC {
			return errors.New("unexpected process-exit event")
		}
		if events[0].Flags&unix.EV_ERROR != 0 {
			return syscall.Errno(events[0].Data)
		}
		if events[0].Fflags&unix.NOTE_EXIT == 0 {
			return errors.New("missing process-exit event")
		}
		return nil
	}
}
