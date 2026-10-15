/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package mail

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// LocalDelivery is explicit authority to execute exactly one sendmail-compatible
// program. No shell, ambient PATH lookup, -t or implicit executable is used.
// Arguments precede the provider-owned -oi, -f, -- and ordered envelope addresses.
type LocalDelivery struct {
	private
	Executable string
	Arguments  []string
}

// ProcessInfo describes local process facts, never SMTP or recipient acceptance.
// Started with unknown/nonzero exit may still have caused external effects.
// StderrCopy is deliberately sensitive; truncation never blocks pipe drainage.
type ProcessInfo struct {
	private
	Started, ExitKnown, StderrTruncated bool
	ExitCode                            int
	InputBytes                          int64
	stderr                              []byte
}

func (value ProcessInfo) StderrCopy() []byte { return bytes.Clone(value.stderr) }
func (result Result) ProcessInfo() ProcessInfo {
	value := result.process
	value.stderr = bytes.Clone(value.stderr)
	return value
}

func (client *Client) Sendmail(ctx context.Context, id fault.Correlation, message Message, command LocalDelivery) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil {
		return nil, failure(ErrInput, "sendmail")
	}
	if err := client.owner.settings.check([]Message{message}); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(command.Executable) || !line(command.Executable, 4096) || len(command.Arguments) > 32 {
		return nil, failure(ErrInput, "sendmail-command")
	}
	total := 0
	for _, argument := range command.Arguments {
		total += len(argument)
		if !line(argument, 4096) || total > 16<<10 || sendmailOverride(argument) {
			return nil, failure(ErrInput, "sendmail-arguments")
		}
	}
	command.Arguments = slices.Clone(command.Arguments)
	return client.execute(ctx, id, "sendmail", func(work context.Context) (Result, error, error) {
		wire, err := compose(work, message, client.owner.settings)
		if err != nil {
			return Result{}, err, nil
		}
		sender := message.sender
		if sender == "" {
			sender = "<>"
		}
		args := append(command.Arguments, "-oi", "-f", sender, "--")
		args = append(args, message.recipients...)
		process := exec.CommandContext(work, command.Executable, args...)
		process.WaitDelay = client.owner.settings.CloseTimeout
		captured := &captureWriter{limit: 16 << 10}
		process.Stderr, process.Stdout = captured, io.Discard
		stdin, err := process.StdinPipe()
		if err != nil {
			return Result{}, err, nil
		}
		if err = process.Start(); err != nil {
			return Result{}, err, stdin.Close()
		}
		type writing struct {
			count int64
			err   error
		}
		joined := make(chan writing, 1)
		go func() {
			count, writeErr := io.Copy(stdin, bytes.NewReader(wire))
			joined <- writing{count, errors.Join(writeErr, stdin.Close())}
		}()
		waitErr := process.Wait()
		written := <-joined
		observed := ProcessInfo{Started: true, InputBytes: written.count, stderr: bytes.Clone(captured.data), StderrTruncated: captured.truncated}
		if process.ProcessState != nil {
			observed.ExitKnown = true
			observed.ExitCode = process.ProcessState.ExitCode()
		}
		return Result{process: observed, written: written.count}, errors.Join(waitErr, written.err), nil
	})
}

func sendmailOverride(argument string) bool {
	if strings.HasPrefix(argument, "--") {
		return true
	}
	if len(argument) < 2 || argument[0] != '-' {
		return false
	}
	switch argument[1] {
	case 'f', 'r', 't', 'b':
		return true
	case 'i', 'v':
		return strings.ContainsAny(argument[2:], "frtb")
	}
	return false
}

type captureWriter struct {
	limit     int
	data      []byte
	truncated bool
}

func (writer *captureWriter) Write(input []byte) (int, error) {
	count := min(len(input), writer.limit-len(writer.data))
	writer.data = append(writer.data, input[:count]...)
	writer.truncated = writer.truncated || count < len(input)
	return len(input), nil
}
