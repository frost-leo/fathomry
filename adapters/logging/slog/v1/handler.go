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

package slog

import (
	"context"
	stdslog "log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Status retains bounded family diagnostics independently of provider evidence.
// Counters saturate. Invalid describes the queried derived handler, while all
// counters cover its family. Successful calls never erase LastError. Refused
// means no provider receipt was admitted; Failed means admitted with an error.
type Status struct {
	private
	Handled, Admitted, Refused, Failed, InvalidDerivations uint64
	Views                                                  int
	RetainedBytes                                          int64
	Active                                                 int
	Invalid, Closing, Closed                               bool
	LastError                                              error
}

// Handler implements only the documented safe slog subset. Copies/clones share
// one lifetime/status/release authority; Close affects the complete family.
type Handler struct {
	private
	family     *family
	fields     []logging.Field
	path       []string
	namespaces map[string]bool
	invalid    error
}
type family struct {
	options  Options
	binding  Binding
	ctx      context.Context
	cancel   context.CancelCauseFunc
	mu       sync.Mutex
	status   Status
	stopping bool
	idle     chan struct{}
	gate     chan struct{}
	complete bool
	final    error
	poison   map[failure.Code]*Handler
}
type activeKey struct{}

func New(options Options, binding Binding) (*Handler, error) {
	if binding.Lifetime == nil || binding.Emit == nil || options.Limits.Validate() != nil ||
		options.Timeout < time.Millisecond || options.Timeout > time.Minute || options.MaxMessageBytes < 1 || options.MaxMessageBytes > 1<<20 ||
		options.MaxViews < 1 || options.MaxViews > 4096 || options.MaxActive < 1 || options.MaxActive > 1024 || options.MaxRetainedBytes < 256 || options.MaxRetainedBytes > 1<<30 ||
		len(options.Levels) < 1 || len(options.Levels) > 16 {
		return nil, fail(logging.ErrInput, "ingress-new")
	}
	if binding.Lifetime.Err() != nil {
		return nil, fail(logging.ErrState, "ingress-new", binding.Lifetime.Err(), context.Cause(binding.Lifetime))
	}
	levels := slices.Clone(options.Levels)
	slices.Sort(levels)
	for index, level := range levels {
		if level < -128 || level > 127 || index > 0 && levels[index-1] == level {
			return nil, fail(logging.ErrInput, "ingress-levels")
		}
	}
	options.Levels = levels
	ctx, cancel := context.WithCancelCause(binding.Lifetime)
	state := &family{options: options, binding: binding, ctx: ctx, cancel: cancel, idle: make(chan struct{}), gate: make(chan struct{}, 1), poison: make(map[failure.Code]*Handler)}
	state.status.Views, state.status.RetainedBytes = 1, 256
	for _, code := range []failure.Code{logging.ErrInput, logging.ErrUnsupported, logging.ErrLimit, logging.ErrState} {
		state.poison[code] = &Handler{family: state, invalid: fail(code, "ingress-derived")}
	}
	return &Handler{family: state}, nil
}
func (handler *Handler) level(level stdslog.Level) bool {
	return slices.Contains(handler.family.options.Levels, int(level))
}
func saturate(value uint64) uint64 {
	if value == math.MaxUint64 {
		return value
	}
	return value + 1
}
func (state *family) issue(err error, admitted, derived bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if derived {
		state.status.InvalidDerivations = saturate(state.status.InvalidDerivations)
	} else if admitted {
		state.status.Failed = saturate(state.status.Failed)
	} else {
		state.status.Refused = saturate(state.status.Refused)
	}
	state.status.LastError = err
}
func (state *family) enter() error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.stopping || state.ctx.Err() != nil {
		return fail(logging.ErrState, "ingress-closed", state.ctx.Err(), context.Cause(state.ctx))
	}
	if state.status.Active >= state.options.MaxActive {
		return fail(logging.ErrLimit, "ingress-active")
	}
	state.status.Active++
	return nil
}
func (state *family) leave() {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.status.Active--
	if state.stopping && state.status.Active == 0 {
		close(state.idle)
	}
}
func (state *family) execution(association context.Context) (context.Context, func()) {
	if association == nil {
		association = context.Background()
	}
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(association))
	stop := context.AfterFunc(state.ctx, func() { cancel(context.Cause(state.ctx)) })
	if state.ctx.Err() != nil {
		cancel(context.Cause(state.ctx))
	}
	work, timeout := context.WithTimeout(ctx, state.options.Timeout)
	work = context.WithValue(work, activeKey{}, state)
	return work, func() { timeout(); stop(); cancel(nil) }
}

// Enabled is advisory filtering for admitted levels. Invalid/closed views and
// unsupported levels return true so slog.Logger reaches observable Handle refusal.
func (handler *Handler) Enabled(ctx context.Context, level stdslog.Level) bool {
	if handler == nil || handler.family == nil {
		return true
	}
	if handler.invalid != nil || !handler.level(level) {
		return true
	}
	state := handler.family
	if ctx != nil && ctx.Value(activeKey{}) == state {
		return true
	}
	if err := state.enter(); err != nil {
		return true
	}
	defer state.leave()
	if state.binding.Enabled == nil {
		return true
	}
	work, stop := state.execution(ctx)
	defer stop()
	if work.Err() != nil {
		return true
	}
	return state.binding.Enabled(work, level)
}

func (handler *Handler) Handle(ctx context.Context, record stdslog.Record) error {
	if handler == nil || handler.family == nil {
		return fail(logging.ErrState, "ingress-handle")
	}
	state := handler.family
	state.mu.Lock()
	state.status.Handled = saturate(state.status.Handled)
	state.mu.Unlock()
	refuse := func(err error) error { err = safe(err, "ingress-handle"); state.issue(err, false, false); return err }
	if handler.invalid != nil {
		return refuse(handler.invalid)
	}
	if !handler.level(record.Level) {
		return refuse(fail(logging.ErrUnsupported, "ingress-level"))
	}
	if ctx != nil && ctx.Value(activeKey{}) == state {
		return refuse(fail(logging.ErrState, "ingress-recursion"))
	}
	if err := state.enter(); err != nil {
		return refuse(err)
	}
	defer state.leave()
	work, stop := state.execution(ctx)
	defer stop()
	if work.Err() != nil {
		return refuse(fail(logging.ErrState, "ingress-lifetime", work.Err(), context.Cause(work)))
	}
	if len(record.Message) > state.options.MaxMessageBytes || record.NumAttrs() > state.options.Limits.MaxFields {
		return refuse(fail(logging.ErrLimit, "ingress-record"))
	}
	if !utf8.ValidString(record.Message) {
		return refuse(fail(logging.ErrInput, "ingress-message"))
	}
	attrs := make([]stdslog.Attr, 0, record.NumAttrs())
	record.Attrs(func(attr stdslog.Attr) bool { attrs = append(attrs, attr); return true })
	decoded, err := decode(attrs, state.options.Limits)
	if err != nil {
		return refuse(err)
	}
	namespaces := cloneNamespaces(handler.namespaces)
	fields, err := appendPath(handler.fields, handler.path, decoded, namespaces, nil)
	if err != nil {
		return refuse(err)
	}
	frozen, err := logging.FreezeFields(fields, state.options.Limits)
	if err != nil {
		return refuse(err)
	}
	if state.binding.Validate != nil {
		if err := state.binding.Validate(frozen); err != nil {
			return refuse(err)
		}
	}
	if work.Err() != nil {
		return refuse(fail(logging.ErrState, "ingress-lifetime", work.Err(), context.Cause(work)))
	}
	attempt := state.binding.Emit(work, Record{Time: record.Time, PC: record.PC, Level: record.Level, Message: strings.Clone(record.Message), Fields: frozen})
	if attempt.Admitted {
		state.mu.Lock()
		state.status.Admitted = saturate(state.status.Admitted)
		state.mu.Unlock()
	}
	if !attempt.Admitted && attempt.Err == nil {
		attempt.Err = fail(logging.ErrState, "ingress-missing-admission")
	}
	if attempt.Err != nil {
		err := safe(attempt.Err, "ingress-emit")
		state.issue(err, attempt.Admitted, false)
		return err
	}
	return nil
}

func (handler *Handler) poisoned(err error) stdslog.Handler {
	if handler == nil || handler.family == nil {
		return &Handler{invalid: fail(logging.ErrState, "ingress-derived")}
	}
	state := handler.family
	err = safe(err, "ingress-derived")
	state.issue(err, false, true)
	code := logging.ErrInput
	if core, ok := failure.Inspect(err); ok {
		candidate := core.Diagnostic().Definition.Code
		if state.poison[candidate] != nil {
			code = candidate
		}
	}
	return state.poison[code]
}
func (handler *Handler) retain(fields []logging.Field, path []string, namespaces map[string]bool) (*Handler, error) {
	state := handler.family
	frozen, err := logging.FreezeFields(fields, state.options.Limits)
	if err != nil {
		return nil, err
	}
	if state.binding.Validate != nil {
		if err := state.binding.Validate(frozen); err != nil {
			return nil, err
		}
	}
	usage, err := logging.MeasureFields(frozen, state.options.Limits)
	if err != nil {
		return nil, err
	}
	bytes := usage.Bytes + 256
	for _, part := range path {
		bytes += int64(len(part)) + 16
	}
	for part := range namespaces {
		bytes += int64(len(part)) + 32
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.stopping || state.ctx.Err() != nil {
		return nil, fail(logging.ErrState, "ingress-derived")
	}
	if state.status.Views == state.options.MaxViews || bytes > state.options.MaxRetainedBytes-state.status.RetainedBytes {
		return nil, fail(logging.ErrLimit, "ingress-views")
	}
	state.status.Views++
	state.status.RetainedBytes += bytes
	return &Handler{family: state, fields: frozen, path: slices.Clone(path), namespaces: namespaces}, nil
}
func (handler *Handler) WithAttrs(attrs []stdslog.Attr) stdslog.Handler {
	if handler == nil || handler.family == nil {
		return handler.poisoned(fail(logging.ErrState, "ingress-derived"))
	}
	if handler.invalid != nil {
		return handler
	}
	if len(attrs) == 0 {
		return handler
	}
	if err := handler.family.enter(); err != nil {
		return handler.poisoned(err)
	}
	defer handler.family.leave()
	decoded, err := decode(attrs, handler.family.options.Limits)
	if err != nil {
		return handler.poisoned(err)
	}
	if len(decoded) == 0 {
		return handler
	}
	namespaces := cloneNamespaces(handler.namespaces)
	fields, err := appendPath(handler.fields, handler.path, decoded, namespaces, nil)
	if err != nil {
		return handler.poisoned(err)
	}
	derived, err := handler.retain(fields, handler.path, namespaces)
	if err != nil {
		return handler.poisoned(err)
	}
	return derived
}
func (handler *Handler) WithGroup(name string) stdslog.Handler {
	if handler == nil || handler.family == nil {
		return handler.poisoned(fail(logging.ErrState, "ingress-derived"))
	}
	if handler.invalid != nil || name == "" {
		return handler
	}
	if err := handler.family.enter(); err != nil {
		return handler.poisoned(err)
	}
	defer handler.family.leave()
	if len(handler.path) >= handler.family.options.Limits.MaxDepth {
		return handler.poisoned(fail(logging.ErrLimit, "ingress-group"))
	}
	probe := []logging.Field{{Key: name, Value: logging.Group()}}
	if _, err := logging.FreezeFields(probe, handler.family.options.Limits); err != nil {
		return handler.poisoned(err)
	}
	if handler.family.binding.Validate != nil {
		if err := handler.family.binding.Validate(probe); err != nil {
			return handler.poisoned(err)
		}
	}
	path := append(slices.Clone(handler.path), strings.Clone(name))
	derived, err := handler.retain(handler.fields, path, cloneNamespaces(handler.namespaces))
	if err != nil {
		return handler.poisoned(err)
	}
	return derived
}
func cloneNamespaces(values map[string]bool) map[string]bool {
	result := make(map[string]bool, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
func pathKey(path []string) string {
	var result strings.Builder
	for _, part := range path {
		result.WriteString(strconv.Itoa(len(part)))
		result.WriteByte(':')
		result.WriteString(part)
	}
	return result.String()
}
func appendPath(fields []logging.Field, path []string, addition []logging.Field, namespaces map[string]bool, prefix []string) ([]logging.Field, error) {
	if len(addition) == 0 {
		return fields, nil
	}
	if len(path) == 0 {
		return append(slices.Clone(fields), addition...), nil
	}
	name := path[0]
	prefix = append(slices.Clone(prefix), name)
	key := pathKey(prefix)
	index := -1
	var children []logging.Field
	for current, field := range fields {
		if field.Key == name {
			index = current
			if field.Value.Kind() != logging.GroupKind || !namespaces[key] {
				return nil, fail(logging.ErrInput, "ingress-group-collision")
			}
			children = field.Value.FieldsCopy()
			break
		}
	}
	nested, err := appendPath(children, path[1:], addition, namespaces, prefix)
	if err != nil {
		return nil, err
	}
	namespaces[key] = true
	result := slices.Clone(fields)
	field := logging.Field{Key: name, Value: logging.Group(nested...)}
	if index < 0 {
		result = append(result, field)
	} else {
		result[index] = field
	}
	return result, nil
}

// Status is a bounded out-of-band observation, not an accepted logging receipt.
func (handler *Handler) Status() (Status, error) {
	if handler == nil || handler.family == nil {
		return Status{}, fail(logging.ErrState, "ingress-status")
	}
	state := handler.family
	state.mu.Lock()
	defer state.mu.Unlock()
	status := state.status
	status.Invalid = handler.invalid != nil
	status.Closing = state.stopping || state.ctx.Err() != nil
	status.Closed = state.complete
	return status, nil
}
func (handler *Handler) ShutdownComplete() bool {
	if handler == nil || handler.family == nil {
		return false
	}
	state := handler.family
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.complete
}

// Close stops the family, joins its actual callback stacks, then releases its
// retained provider view. It never closes an output/source/Runtime. Interrupted
// waiting and incomplete Release retain this same family for explicit continuation.
func (handler *Handler) Close(ctx context.Context) error {
	if handler == nil || handler.family == nil || ctx == nil {
		return fail(logging.ErrInput, "ingress-close")
	}
	state := handler.family
	state.mu.Lock()
	if !state.stopping {
		state.stopping = true
		if state.status.Active == 0 {
			close(state.idle)
		}
	}
	state.mu.Unlock()
	state.cancel(nil)
	state.mu.Lock()
	complete, final := state.complete, state.final
	state.mu.Unlock()
	if complete {
		return final
	}
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return fail(logging.ErrState, "ingress-close", ctx.Err(), context.Cause(ctx))
	}
	state.mu.Lock()
	complete, final = state.complete, state.final
	state.mu.Unlock()
	if complete {
		return final
	}
	select {
	case <-state.idle:
	case <-ctx.Done():
		return fail(logging.ErrState, "ingress-close", ctx.Err(), context.Cause(ctx))
	}
	result := resource.ReleaseResult{Complete: true}
	if state.binding.Release != nil {
		result = state.binding.Release(ctx)
	}
	err := safe(result.Err, "ingress-release")
	if !result.Complete && err == nil {
		err = fail(logging.ErrState, "ingress-release-pending")
	}
	if err != nil {
		state.mu.Lock()
		state.status.LastError = err
		state.mu.Unlock()
	}
	if !result.Complete {
		return err
	}
	state.mu.Lock()
	state.complete = true
	state.final = err
	state.mu.Unlock()
	return err
}
