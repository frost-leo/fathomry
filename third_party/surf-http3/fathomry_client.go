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

package http3

import (
	"context"
	"errors"
	"io"
	"sync"
)

type fathomryClientBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
	err     error
}

func (body *fathomryClientBody) Close() error {
	body.once.Do(func() { body.err = body.ReadCloser.Close(); body.release() })
	return body.err
}

func (t *Transport) releaseClient(client *roundTripperWithCount) {
	t.mutex.Lock()
	remaining := client.useCount.Add(-1)
	_, retired := t.retired[client]
	t.mutex.Unlock()
	if retired && remaining == 0 {
		t.closeRetired(client)
	}
}
func (t *Transport) closeRetired(client *roundTripperWithCount) {
	err := client.Close()
	t.mutex.Lock()
	delete(t.retired, client)
	t.cleanup = errors.Join(t.cleanup, err)
	t.mutex.Unlock()
}
func (t *Transport) clientWork(ctx context.Context, client *roundTripperWithCount) (context.Context, *fathomryRequestWork) {
	work := &fathomryRequestWork{done: make(chan struct{})}
	prior, _ := ctx.Value(fathomryWorkKey{}).(func(context.Context) (func(), error))
	return FathomryWithWork(ctx, func(ctx context.Context) (func(), error) {
		release := func() {}
		if prior != nil {
			var err error
			release, err = prior(ctx)
			if err != nil || release == nil {
				if release != nil {
					release()
				}
				if err == nil {
					err = errors.New("http3: missing parent work release")
				}
				return nil, err
			}
		}
		t.mutex.Lock()
		if t.closed {
			t.mutex.Unlock()
			release()
			return nil, ErrTransportClosed
		}
		work.mu.Lock()
		if work.sealed {
			work.mu.Unlock()
			t.mutex.Unlock()
			release()
			return nil, ErrTransportClosed
		}
		work.active++
		work.mu.Unlock()
		client.useCount.Add(1)
		t.mutex.Unlock()
		return func() { t.releaseClient(client); release(); work.leave() }, nil
	}), work
}

type fathomryRequestWork struct {
	mu     sync.Mutex
	active int
	sealed bool
	done   chan struct{}
}

func (work *fathomryRequestWork) seal() {
	work.mu.Lock()
	defer work.mu.Unlock()
	work.sealed = true
	if work.active == 0 {
		close(work.done)
	}
}
func (work *fathomryRequestWork) leave() {
	work.mu.Lock()
	defer work.mu.Unlock()
	work.active--
	if work.sealed && work.active == 0 {
		close(work.done)
	}
}
func (work *fathomryRequestWork) wait(ctx context.Context) error {
	select {
	case <-work.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
