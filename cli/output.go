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

package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/frost-leo/fathomry/failure/v1"
)

const (
	errInputs     = ErrInputs
	errDefinition = ErrDefinition
	errArguments  = ErrUsage
	errLanguage   = ErrLanguage
)

type checkedWriter struct {
	mutex  sync.Mutex
	writer io.Writer
	err    error
	stream streamKind
}

func (writer *checkedWriter) Write(data []byte) (int, error) {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	if writer.err != nil {
		return 0, writer.err
	}
	count, err := writer.writer.Write(data)
	if count < 0 || count > len(data) {
		count = 0
		err = errors.Join(err, io.ErrShortWrite)
	} else if count != len(data) {
		err = errors.Join(err, io.ErrShortWrite)
	}
	if err != nil {
		writer.err = &outputFailure{core: hostFailure(ErrOutput, err), stream: writer.stream}
	}
	return count, writer.err
}

type streamKind uint8

const (
	unknownStream streamKind = iota
	stdoutStream
	stderrStream
)

// outputFailure owns one immutable stream observation and the same exact core.
// It is not a project-effect receipt or a wrapper added at every package layer.
type outputFailure struct {
	core   *failure.Error
	stream streamKind
}

func (err *outputFailure) Failure() *failure.Error {
	if err == nil {
		return nil
	}
	return err.core
}
func (err *outputFailure) Error() string { return err.Failure().Error() }
func (err *outputFailure) Unwrap() error {
	if err == nil || err.core == nil {
		return nil
	}
	return err.core
}
func (err outputFailure) Format(state fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", (&err).Error())
	} else {
		_, _ = io.WriteString(state, (&err).Error())
	}
}
func (err *outputFailure) LogValue() slog.Value    { return slog.StringValue(err.Error()) }
func (outputFailure) MarshalJSON() ([]byte, error) { return nil, failure.ErrSerialization }
func (*outputFailure) UnmarshalJSON([]byte) error  { return failure.ErrSerialization }

func (writer *checkedWriter) failure() error {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	return writer.err
}

func exitStatus(err error) int {
	if err == nil {
		return 0
	}
	if onlyCancellation(err) {
		return 130
	}
	return 1
}

func onlyCancellation(err error) bool {
	if _, semantic := err.(failure.Occurrence); semantic {
		return false
	}
	if err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !onlyCancellation(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyCancellation(wrapped.Unwrap())
	}
	return false
}
