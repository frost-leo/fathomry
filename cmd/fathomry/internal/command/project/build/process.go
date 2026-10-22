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
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"time"
)

const maxToolOutput = 64 << 10

type processBuffer struct {
	data    bytes.Buffer
	maximum int
	cancel  context.CancelCauseFunc
	err     error
}

func (buffer *processBuffer) Write(data []byte) (int, error) {
	if buffer.err != nil {
		return 0, buffer.err
	}
	if len(data) > buffer.maximum-buffer.data.Len() {
		buffer.err = fail(ErrLimit)
		buffer.cancel(buffer.err)
		return 0, buffer.err
	}
	return buffer.data.Write(data)
}

func (buffer *processBuffer) Bytes() []byte { return buffer.data.Bytes() }

func runProcess(ctx context.Context, directory string, environment []string, maximum int, name string, args ...string) ([]byte, error) {
	child, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stdout := &processBuffer{maximum: maximum, cancel: cancel}
	stderr := &processBuffer{maximum: maxToolOutput, cancel: cancel}
	process := exec.Command(name, args...)
	process.Dir, process.Env = directory, environment
	process.Stdout, process.Stderr = stdout, stderr
	process.WaitDelay = time.Second
	err := runCommand(child, process)
	// Wait joins both copy goroutines before their independently observed errors
	// are read. A first-wins context cause cannot retain both stream failures.
	err = errors.Join(err, stdout.err, stderr.err, context.Cause(child))
	var exited *exec.ExitError
	if errors.As(err, &exited) {
		exited.Stderr = bytes.Clone(stderr.Bytes())
	}
	if ctx.Err() != nil {
		return nil, fail(ErrCanceled, ctx.Err(), context.Cause(ctx), err)
	}
	if err != nil {
		return nil, err
	}
	return bytes.Clone(stdout.Bytes()), nil
}

func buildEnvironment() []string {
	return append(os.Environ(), "GOWORK=off", "GOFLAGS=")
}
