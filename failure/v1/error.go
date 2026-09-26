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

package failure

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
)

// MaxCauses bounds supplied direct cause slots, including literal nil slots.
// It does not bound the reachable storage of retained foreign errors.
const MaxCauses = 32

var (
	// ErrCondition reports invalid required identity without retaining its text.
	ErrCondition = errors.New("failure: invalid condition")
	// ErrCauses reports too many supplied cause slots. Nothing is accepted.
	ErrCauses = errors.New("failure: cause limit exceeded")
	// ErrSerialization refuses runtime error encoding and reconstruction.
	ErrSerialization = errors.New("failure: runtime serialization is unsupported")
)

// Diagnostic is a comparable value-only projection of one current occurrence.
// Zero means invalid/absent, not success. CauseCount counts retained direct
// non-nil interfaces (including typed nils), not recursively reachable causes.
// The projection is not a durable error schema.
type Diagnostic struct {
	Condition  Condition
	CauseCount int
}

// Error is an opaque runtime occurrence. Use *Error as an error; Error values
// support safe fmt and refuse JSON but do not implement error. A zero Error is
// invalid. A nil *Error is absent, but a typed-nil error interface is still non-nil.
// Copies share immutable state and are comparable by that state, not condition.
// Do not overwrite an Error after sharing it. Accepted foreign causes are borrowed
// for their lifetime; their mutation, concurrency and traversal remain owner duties.
type Error struct{ state *errorState }

type errorState struct {
	diagnostic Diagnostic
	causes     []error
}

// New validates required identity and the supplied slot count before allocating
// owned storage. On rejection it returns (nil, ErrCondition/ErrCauses), retains
// nothing and does not claim the requested condition. Rejected inputs stay owned
// by the caller. On success it clones condition storage and copies cause slots,
// omitting only literal nil. Typed nils, duplicates and exact foreign objects are
// retained unchanged, without invoking any of their methods or traversing graphs.
// Cause selection is an operation-owned API/privacy commitment, not a type filter.
// Inputs must remain unchanged during the call.
func New(condition Condition, causes ...error) (*Error, error) {
	if !condition.Valid() {
		return nil, ErrCondition
	}
	if len(causes) > MaxCauses {
		return nil, ErrCauses
	}
	kept := make([]error, 0, len(causes))
	for _, cause := range causes {
		if cause != nil {
			kept = append(kept, cause)
		}
	}
	return &Error{state: &errorState{
		diagnostic: Diagnostic{Condition: Condition(strings.Clone(string(condition))), CauseCount: len(kept)},
		causes:     kept,
	}}, nil
}

// Diagnostic returns an independent, immutable-data projection; nil/zero returns
// its zero value. It neither traverses causes nor invokes foreign methods.
func (err *Error) Diagnostic() Diagnostic {
	if err == nil || err.state == nil {
		return Diagnostic{}
	}
	return err.state.diagnostic
}

// Error emits only the condition, or a fixed absence/invalid marker. It never
// prints causes. Its wording is not a parsing contract.
func (err *Error) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.state == nil {
		return "failure: invalid occurrence"
	}
	return string(err.state.diagnostic.Condition)
}

// Is matches only a valid Condition equal to this occurrence's own identity.
// Standard errors.Is additionally supplies pointer identity and recursively
// traverses declared causes; other occurrences do not match merely by equal code.
// Foreign matching hooks may panic, block or cycle; traversal is not sandboxed.
func (err *Error) Is(target error) bool {
	condition, ok := target.(Condition)
	return err != nil && err.state != nil && ok && condition.Valid() && err.state.diagnostic.Condition == condition
}

// Unwrap returns independently mutable slice storage containing exact retained
// cause objects. Raw inspection is deliberate and not sanitized or deep-copied.
// Even a single cause uses Go's multi-cause contract, not errors.Unwrap(error).
func (err *Error) Unwrap() []error {
	if err == nil || err.state == nil {
		return nil
	}
	return append([]error(nil), err.state.causes...)
}

// Format protects Error values and pointers on fmt's Formatter-dispatched paths.
// It follows Condition.Format, including its exclusions for type/pointer and
// malformed-format diagnostics; nil pointers are handled by fmt as absence.
func (err Error) Format(state fmt.State, verb rune) {
	formatText(state, verb, (&err).Error())
}

// LogValue returns code-only output for *Error, including nil. Logging an Error
// value through a standard slog text handler uses Format; its JSON handler meets
// the explicit serialization refusal instead. Prefer the canonical *Error.
func (err *Error) LogValue() slog.Value {
	return slog.StringValue(err.Error())
}

// MarshalJSON refuses both non-nil pointer and value runtime serialization.
// encoding/json emits null for a nil pointer without calling this method.
func (Error) MarshalJSON() ([]byte, error) { return nil, ErrSerialization }

// UnmarshalJSON refuses reconstruction, including into zero/nil receivers.
func (*Error) UnmarshalJSON([]byte) error { return ErrSerialization }

// Occurrence is the explicit direct-extension contract, not a recursive search.
// Failure must return the same immutable current *Error on each call, or nil/an
// invalid Error when there is no valid occurrence. It must not select a descendant
// by traversal. The extension owns its facts, bounds, copying, concurrency,
// Error/Format/slog/serialization and intentional Go Unwrap/Is/As behavior.
// Embedding alone does not establish those guarantees; test actual method sets.
// Inspect invokes only this accessor, not arbitrary presentation/matching hooks.
type Occurrence interface {
	error
	Failure() *Error
}

// Failure exposes this occurrence directly, including nil/invalid states.
func (err *Error) Failure() *Error { return err }

// Inspect returns only the directly supplied valid occurrence. Ordinary wrappers,
// joins, foreign errors, nil/typed-nil values and invalid extension results return
// (nil, false), with no cause-tree fallback. Extensions are discovered solely by
// the Occurrence interface; reflection only rejects typed-nil values before the
// accessor call. A non-nil extension's accessor must obey its contract; it is not
// protected from arbitrary panics, side effects or nontermination by a sandbox.
// Bind presentation identity and typed facts to this same supplied extension,
// never to an independently discovered errors.As descendant with an equal code.
func Inspect(err error) (*Error, bool) {
	occurrence, ok := err.(Occurrence)
	if !ok {
		return nil, false
	}
	value := reflect.ValueOf(occurrence)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return nil, false
		}
	}
	current := occurrence.Failure()
	if current == nil || current.state == nil {
		return nil, false
	}
	return current, true
}
