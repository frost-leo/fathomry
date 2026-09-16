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

package surf

import (
	"errors"
	"io"
	"sync"
)

type inputBody struct {
	raw      io.ReadCloser
	op       *operation
	mu       sync.Mutex
	once     sync.Once
	closeErr error
	count    int64
	closed   bool
}

func (op *operation) input(raw io.ReadCloser) *inputBody {
	body := &inputBody{raw: raw, op: op}
	op.mu.Lock()
	op.inputs = append(op.inputs, body)
	closing := op.closing
	op.mu.Unlock()
	if closing {
		_ = body.Close()
	}
	return body
}
func (body *inputBody) Read(buffer []byte) (int, error) {
	if !body.op.work.enter() {
		return 0, failure(ErrState, "input-ended")
	}
	defer body.op.work.leave()
	body.mu.Lock()
	if body.closed {
		body.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	raw := body.raw
	left := body.op.client.owner.settings.MaxRequestBytes - body.count
	body.mu.Unlock()
	if left < 0 {
		return 0, failure(ErrLimit, "request-body")
	}
	if int64(len(buffer)) > left+1 {
		buffer = buffer[:left+1]
	}
	var count int
	err := invoke("request-body-read", func() error {
		var err error
		count, err = raw.Read(buffer)
		return err
	})
	body.mu.Lock()
	body.count += int64(count)
	tooLarge := body.count > body.op.client.owner.settings.MaxRequestBytes
	body.mu.Unlock()
	body.op.mu.Lock()
	body.op.data.sent += int64(count)
	body.op.mu.Unlock()
	if tooLarge {
		return count, failure(ErrLimit, "request-body")
	}
	return count, err
}
func (body *inputBody) Close() error {
	body.once.Do(func() {
		body.mu.Lock()
		body.closed = true
		raw := body.raw
		body.mu.Unlock()
		body.closeErr = invoke("request-body-close", raw.Close)
		body.mu.Lock()
		body.raw = nil
		body.mu.Unlock()
	})
	return body.closeErr
}
func (op *operation) replay(factory func() (io.ReadCloser, error)) (io.ReadCloser, error) {
	if !op.work.enter() {
		return nil, failure(ErrState, "replay-ended")
	}
	defer op.work.leave()
	op.mu.Lock()
	if op.replays >= op.client.owner.settings.MaxReplays {
		op.mu.Unlock()
		return nil, failure(ErrLimit, "replays")
	}
	op.replays++
	op.mu.Unlock()
	if err := op.ctx.Err(); err != nil {
		return nil, err
	}
	var raw io.ReadCloser
	err := invoke("replay", func() error {
		var err error
		raw, err = factory()
		return err
	})
	if raw == nil || nilLike(raw) {
		if err == nil {
			err = failure(ErrInput, "replay-body")
		}
		return nil, err
	}
	body := op.input(raw)
	if err != nil {
		return nil, errors.Join(err, body.Close())
	}
	return body, nil
}
