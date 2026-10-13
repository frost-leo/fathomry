/*
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

package chromedp

import (
	"context"
	"errors"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"sync"
)

// ErrBindingLimit reports exhausted input, handler or output capacity.
var ErrBindingLimit = errors.New("binding capacity exhausted")

// BindingLimits bounds payload decoding and handler creation.
// Each listener routes the byte-bounded event name before taking a handler slot.
// OnError must not block, panic, or call the browser. It can cancel its owner.
// PrivateErrors hides handler, panic and codec text from the page.
type BindingLimits struct {
	MaxActive      int
	MaxInputBytes  int64
	MaxOutputBytes int64
	PrivateErrors  bool
	OnError        func(error)
}

// WithBindingLimits selects limits for each target. Zero preserves native defaults.
func WithBindingLimits(limits BindingLimits) BrowserOption {
	return func(b *Browser) { b.bindingLimits = limits }
}

type bindingState struct {
	mu     sync.Mutex
	active int
	sealed bool
	idle   chan struct{}
	limits BindingLimits
}

func (s *bindingState) matches(raw jsonv2.Value, name string) (bool, error) {
	if s.limits.MaxInputBytes > 0 && int64(len(raw)) > s.limits.MaxInputBytes {
		return false, ErrBindingLimit
	}
	var route struct {
		Name string `json:"name"`
	}
	if err := jsonv2.Unmarshal(raw, &route, DefaultUnmarshalOptions); err != nil {
		return false, err
	}
	return route.Name == name, nil
}

func (s *bindingState) acquire(size int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed || s.limits.MaxActive > 0 && s.active >= s.limits.MaxActive ||
		s.limits.MaxInputBytes > 0 && int64(size) > s.limits.MaxInputBytes {
		return false
	}
	if s.active == 0 {
		s.idle = make(chan struct{})
	}
	s.active++
	return true
}
func (s *bindingState) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if s.active == 0 {
		close(s.idle)
	}
}
func (s *bindingState) report(err error) {
	if err != nil && s.limits.OnError != nil {
		s.limits.OnError(err)
	}
}

// JoinBindings seals new handlers and waits for actual handler and reply exit.
// A canceled wait does not free the handlers. Call it again to observe completion.
func (t *Target) JoinBindings(ctx context.Context) error {
	t.exposedMu.Lock()
	for _, binding := range t.exposed {
		binding.Cancel()
	}
	t.exposedMu.Unlock()
	s := &t.bindings
	s.mu.Lock()
	s.sealed = true
	active, idle := s.active, s.idle
	s.mu.Unlock()
	if active == 0 {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type bindingBuffer struct {
	data  []byte
	limit int64
}

func marshalBinding(buffer *bindingBuffer, value any) error {
	var encoder jsonv2.Encoder
	encoder.Reset(buffer, DefaultMarshalOptions)
	return jsonv2.MarshalEncode(&encoder, value, DefaultMarshalOptions)
}

func (b *bindingBuffer) Write(data []byte) (int, error) {
	if int64(len(data)) > b.limit-int64(len(b.data)) {
		return 0, ErrBindingLimit
	}
	b.data = append(b.data, data...)
	return len(data), nil
}
