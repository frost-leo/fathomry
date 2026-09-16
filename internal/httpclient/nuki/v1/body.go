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

package nuki

import (
	"errors"
	"io"
	"reflect"
	"sync"
)

type byteBudget struct {
	mu    sync.Mutex
	used  int64
	limit int64
}

func (budget *byteBudget) count() int64 {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return budget.used
}

type body struct {
	raw           io.ReadCloser
	op            *operation
	budget        *byteBudget
	expected      int64
	input         bool
	count         int64
	eof           bool
	mu            sync.Mutex
	closed        bool
	reading       sync.WaitGroup
	once          sync.Once
	closeErr      error
	errorRecorded bool
}

func (op *operation) ownBody(raw io.ReadCloser, budget *byteBudget, expected int64, input bool) *body {
	wrapped := &body{raw: raw, op: op, budget: budget, expected: expected, input: input}
	op.mu.Lock()
	op.bodies = append(op.bodies, wrapped)
	finishing := op.finishing
	op.mu.Unlock()
	if op.ctx.Err() != nil || finishing {
		_ = wrapped.Close()
	}
	return wrapped
}
func (body *body) Read(data []byte) (count int, err error) {
	if len(data) == 0 {
		return 0, nil
	}
	body.mu.Lock()
	if body.closed {
		body.mu.Unlock()
		return 0, failure(ErrState, "body-closed")
	}
	body.reading.Add(1)
	body.mu.Unlock()
	defer body.reading.Done()
	body.budget.mu.Lock()
	defer body.budget.mu.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			count, err = 0, callbackFailure(recovered)
		}
		if err != nil && err != io.EOF && !body.errorRecorded {
			body.errorRecorded = true
			if body.input {
				body.op.recordInputError(err)
			} else {
				body.op.responseError(err)
			}
		}
	}()
	if body.budget.used > body.budget.limit {
		return 0, failure(ErrLimit, "body-bytes")
	}
	allowance := min(int64(len(data)), body.budget.limit-body.budget.used+1)
	count, err = body.raw.Read(data[:allowance])
	if count < 0 || int64(count) > allowance {
		return 0, failure(ErrRead, "reader-count", err)
	}
	body.budget.used += int64(count)
	body.count += int64(count)
	if body.budget.used > body.budget.limit {
		err = errors.Join(err, failure(ErrLimit, "body-bytes"))
	}
	if body.expected >= 0 && (body.count > body.expected || err == io.EOF && body.count != body.expected) {
		err = errors.Join(err, failure(ErrIntegrity, "content-length", io.ErrUnexpectedEOF))
	}
	if err == io.EOF {
		body.eof = true
	}
	return count, err
}
func (body *body) Close() error {
	body.once.Do(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				body.closeErr = callbackFailure(recovered)
			}
			if body.closeErr != nil {
				body.op.cleanupError(body.closeErr)
			}
		}()
		body.mu.Lock()
		body.closed = true
		body.mu.Unlock()
		body.closeErr = body.raw.Close()
	})
	return body.closeErr
}

func (body *body) reachedEOF() bool {
	body.budget.mu.Lock()
	defer body.budget.mu.Unlock()
	return body.eof
}
func nilObject(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Interface, reflect.Chan:
		return reflected.IsNil()
	}
	return false
}
func (op *operation) aliasesBody(reader io.ReadCloser) bool {
	op.mu.Lock()
	defer op.mu.Unlock()
	if !reflect.TypeOf(reader).Comparable() {
		return false
	}
	for _, existing := range op.bodies {
		if reflect.TypeOf(existing.raw) == reflect.TypeOf(reader) && existing.raw == reader {
			return true
		}
	}
	return false
}
