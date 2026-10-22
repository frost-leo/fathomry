//go:build linux || darwin

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
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

const supportedHost = true

func executableReadFlags() int { return os.O_RDONLY | syscall.O_NONBLOCK }

func runCommand(ctx context.Context, process *exec.Cmd) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	process.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := process.Start(); err != nil {
		return err
	}
	group := &processGroup{pid: process.Process.Pid}
	cancellationDone := make(chan struct{})
	stopCancellation := context.AfterFunc(ctx, func() {
		defer close(cancellationDone)
		group.signal()
	})

	// Keep the leader unreaped so its PID/PGID cannot be reused. Seal every
	// group-signaling path before Wait releases that identity.
	observed := waitProcessExit(process.Process.Pid)
	cleanup := group.finish(!errors.Is(observed, syscall.ECHILD))
	if !stopCancellation() {
		<-cancellationDone
	}
	return errors.Join(observed, cleanup, process.Wait())
}

type processGroup struct {
	mutex sync.Mutex
	pid   int
	err   error
}

func (group *processGroup) signal() {
	group.mutex.Lock()
	defer group.mutex.Unlock()
	group.signalLocked()
}

func (group *processGroup) signalLocked() {
	if group.pid == 0 {
		return
	}
	if err := syscall.Kill(-group.pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		group.err = errors.Join(group.err, err)
	}
}

func (group *processGroup) finish(owned bool) error {
	group.mutex.Lock()
	defer group.mutex.Unlock()
	if owned {
		group.signalLocked()
	}
	group.pid = 0
	return group.err
}
