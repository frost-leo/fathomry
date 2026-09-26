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
	"io"
	"sync"
)

var (
	errInputs     = errors.New("cli: invalid invocation inputs")
	errDefinition = errors.New("cli: invalid command definition")
	errArguments  = errors.New("cli: invalid arguments")
	errLanguage   = errors.New("cli: unsupported language")
)

type usageError struct{ error }

func (err usageError) Unwrap() error { return err.error }

type outputError struct{ error }

func (err outputError) Unwrap() error { return err.error }

type checkedWriter struct {
	mutex  sync.Mutex
	writer io.Writer
	err    error
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
		writer.err = outputError{err}
	}
	return count, writer.err
}

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
	if onlyUsage(err) {
		return 2
	}
	return 1
}

func onlyCancellation(err error) bool {
	switch err.(type) {
	case usageError, outputError:
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

func onlyUsage(err error) bool {
	if _, ok := err.(usageError); ok {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !onlyUsage(cause) {
				return false
			}
		}
		return true
	}
	return false
}
