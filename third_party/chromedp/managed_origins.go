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
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/chromedp/cdproto"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/runtime"
)

type executionOrigins struct {
	mu        sync.RWMutex
	contexts  map[runtime.ExecutionContextID]string
	bytes     int64
	uncertain error
}

// ExecutionOrigin identifies a context that this target observed.
// ID can be reused. UniqueID names one context across process changes.
type ExecutionOrigin struct {
	ID       runtime.ExecutionContextID `json:"id"`
	UniqueID string                     `json:"uniqueId"`
}

// ExecutionOrigins returns a copy of the current identities, sorted by ID.
// It refuses an uncertain table or a count above maximum before it copies.
// A snapshot does not keep a context alive. It sends no browser command.
func (t *Target) ExecutionOrigins(maximum int) ([]ExecutionOrigin, error) {
	if t == nil {
		return nil, fmt.Errorf("reading execution origins: %w", ErrInvalidContext)
	}
	select {
	case <-t.stopped:
		return nil, fmt.Errorf("reading execution origins: %w", ErrInvalidContext)
	default:
	}
	t.origins.mu.RLock()
	defer t.origins.mu.RUnlock()
	if t.origins.uncertain != nil {
		return nil, fmt.Errorf("reading execution origins: %w", t.origins.uncertain)
	}
	if maximum < 0 || len(t.origins.contexts) > maximum {
		return nil, fmt.Errorf("reading execution origins: %w", ErrEventOverflow)
	}
	origins := make([]ExecutionOrigin, 0, len(t.origins.contexts))
	for id, unique := range t.origins.contexts {
		origins = append(origins, ExecutionOrigin{ID: id, UniqueID: unique})
	}
	slices.SortFunc(origins, func(left, right ExecutionOrigin) int { return cmp.Compare(left.ID, right.ID) })
	return origins, nil
}

// trackExecutionOrigin runs in wire order before event publication. The frame
// cache updates on another goroutine and cannot supply an event-time identity.
func (t *Target) trackExecutionOrigin(msg *cdproto.Message) (err error) {
	switch string(msg.Method) {
	case runtime.ExecutionContextCreated.Method, runtime.ExecutionContextDestroyed.Method, runtime.ExecutionContextsCleared.Method:
	default:
		return nil
	}
	t.origins.mu.Lock()
	defer func() {
		if err != nil && t.origins.uncertain == nil {
			t.origins.uncertain = err
		}
		t.origins.mu.Unlock()
	}()
	if t.events.limits.MaxBytes > 0 && int64(len(msg.Params)) > t.events.limits.MaxBytes {
		return ErrEventOverflow
	}
	switch string(msg.Method) {
	case runtime.ExecutionContextCreated.Method:
		var event struct {
			Context *struct {
				ID       runtime.ExecutionContextID `json:"id"`
				UniqueID string                     `json:"uniqueId"`
			} `json:"context"`
		}
		if err := jsonv2.Unmarshal(msg.Params, &event, DefaultUnmarshalOptions); err != nil {
			return err
		}
		if event.Context == nil || event.Context.ID == 0 || event.Context.UniqueID == "" {
			return errors.New("execution context has no identity")
		}
		previous, exists := t.origins.contexts[event.Context.ID]
		size := t.origins.bytes - int64(len(previous)) + int64(len(event.Context.UniqueID))
		if !exists {
			size += 64
		}
		if t.events.limits.MaxBytes > 0 && size > t.events.limits.MaxBytes {
			if exists {
				delete(t.origins.contexts, event.Context.ID)
				t.origins.bytes -= int64(len(previous)) + 64
			}
			return ErrEventOverflow
		}
		if t.origins.contexts == nil {
			t.origins.contexts = make(map[runtime.ExecutionContextID]string)
		}
		t.origins.contexts[event.Context.ID] = event.Context.UniqueID
		t.origins.bytes = size
	case runtime.ExecutionContextDestroyed.Method:
		var event runtime.EventExecutionContextDestroyed
		if err := jsonv2.Unmarshal(msg.Params, &event, DefaultUnmarshalOptions); err != nil {
			return err
		}
		if event.ExecutionContextUniqueID == "" {
			return errors.New("execution context removal has no identity")
		}
		for id, origin := range t.origins.contexts {
			if origin == event.ExecutionContextUniqueID {
				delete(t.origins.contexts, id)
				t.origins.bytes -= int64(len(origin)) + 64
			}
		}
	case runtime.ExecutionContextsCleared.Method:
		if len(msg.Params) != 0 {
			if value := bytes.TrimSpace(msg.Params); len(value) == 0 || value[0] != '{' {
				return errors.New("execution context reset is not an object")
			}
			var event runtime.EventExecutionContextsCleared
			if err := jsonv2.Unmarshal(msg.Params, &event, DefaultUnmarshalOptions); err != nil {
				return err
			}
		}
		clear(t.origins.contexts)
		t.origins.bytes = 0
		t.origins.uncertain = nil
	}
	return nil
}

func (t *Target) executionOriginError() error {
	t.origins.mu.RLock()
	defer t.origins.mu.RUnlock()
	return t.origins.uncertain
}

func (t *Target) executionOrigin(id runtime.ExecutionContextID) string {
	t.origins.mu.RLock()
	defer t.origins.mu.RUnlock()
	return t.origins.contexts[id]
}
func (t *Target) executionOriginCopy() []string {
	t.origins.mu.RLock()
	defer t.origins.mu.RUnlock()
	origins := make([]string, 0, len(t.origins.contexts))
	for _, origin := range t.origins.contexts {
		origins = append(origins, origin)
	}
	return origins
}
func (t *Target) hasExecutionOrigin(origin string) bool {
	t.origins.mu.RLock()
	defer t.origins.mu.RUnlock()
	for _, current := range t.origins.contexts {
		if current == origin {
			return true
		}
	}
	return false
}
