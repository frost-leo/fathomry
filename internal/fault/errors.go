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

package fault

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
)

// Kind is a code-owned technical classification, not a public semantic version
// or a universal SDK error catalog. Its owner declares the supported meaning.
type Kind string

const Invalid Kind = "fathomry.internal.invalid"

func (kind Kind) Error() string {
	if !label(string(kind), 128, false) {
		return string(Invalid)
	}
	return string(kind)
}

// Correlation identifies one technical call and its parent. Owner is an optional
// opaque association supplied by the caller, not authorization, quota or proof
// of effect ownership. IDs use 1-128 ASCII letters, digits, dots, underscores or
// hyphens when present. Higher-level Run/Item/attempt meaning stays at the boundary.
type Correlation struct {
	Call   string
	Parent string
	Owner  string
}

// Valid checks representation, not uniqueness or the existence of an owner.
// Empty correlation is valid outside a call; invocation additionally requires Call.
func (correlation Correlation) Valid() bool {
	for _, value := range []string{correlation.Call, correlation.Parent, correlation.Owner} {
		if value != "" && !label(value, 128, true) {
			return false
		}
	}
	return correlation.Parent == "" || correlation.Parent != correlation.Call
}

// Context holds technical location and correlation only. Scope identifies the
// original resource composition scope, not a business Run or a transaction.
// Location labels use at most 64 lowercase ASCII letters, digits, dots,
// underscores or hyphens. Empty means absent; callers must exclude secrets.
type Context struct {
	Operation   string
	Provider    string
	Scope       string
	Source      string
	Correlation Correlation
}

func (context Context) Valid() bool {
	for _, value := range []string{context.Operation, context.Provider, context.Scope, context.Source} {
		if value != "" && !label(value, 64, false) {
			return false
		}
	}
	return context.Correlation.Valid()
}

// Diagnostic is a value-only technical projection. It excludes native text and
// cause graphs and does not report retryability, remote effects or business state.
type Diagnostic struct {
	Kind      Kind
	Context   Context
	HasCauses bool
}

// Error owns immutable context and cause-slice storage. Native error objects are
// retained, not cloned; their lifetime and concurrent use follow their SDK contract.
type Error struct{ state *errorState }

type errorState struct {
	diagnostic Diagnostic
	causes     []error
}

// New retains every non-nil original cause without invoking its presentation.
// Invalid kind/context produces Invalid with empty context, still keeping causes.
// The caller's public boundary may wrap this error using its own semantic identity.
func (kind Kind) New(context Context, causes ...error) *Error {
	if !label(string(kind), 128, false) || !context.Valid() {
		kind, context = Invalid, Context{}
	}
	kept := make([]error, 0, len(causes))
	for _, cause := range causes {
		if cause != nil {
			kept = append(kept, cause)
		}
	}
	return &Error{state: &errorState{
		diagnostic: Diagnostic{Kind: kind, Context: context, HasCauses: len(kept) != 0},
		causes:     kept,
	}}
}

func (err *Error) Diagnostic() Diagnostic {
	if err == nil || err.state == nil {
		return Diagnostic{Kind: Invalid}
	}
	return err.state.diagnostic
}

func (err *Error) Error() string {
	if err == nil {
		return "<nil>"
	}
	return string(err.Diagnostic().Kind)
}

func (err *Error) Is(target error) bool {
	kind, ok := target.(Kind)
	return err != nil && ok && label(string(kind), 128, false) && err.Diagnostic().Kind == kind
}

// Unwrap returns independent slice storage while preserving exact native causes.
// Deliberate cause inspection is not sanitized or an SDK abstraction guarantee.
func (err *Error) Unwrap() []error {
	if err == nil || err.state == nil {
		return nil
	}
	return append([]error(nil), err.state.causes...)
}

func (err Error) Format(state fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", err.Error())
	} else {
		_, _ = io.WriteString(state, err.Error())
	}
}

func (err *Error) LogValue() slog.Value {
	if err == nil {
		return slog.StringValue("<nil>")
	}
	return slog.StringValue(err.Error())
}

func (err Error) MarshalJSON() ([]byte, error) {
	return nil, errors.New("fault: runtime error serialization is unsupported")
}

func (err *Error) UnmarshalJSON([]byte) error {
	return errors.New("fault: runtime error reconstruction is unsupported")
}

func label(value string, limit int, uppercase bool) bool {
	if value == "" || len(value) > limit {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || uppercase && char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
