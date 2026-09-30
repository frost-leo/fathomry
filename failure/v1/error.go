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
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
)

// Error owns immutable occurrence metadata and cause-slice storage. Use *Error as
// error; do not overwrite a shared handle. Foreign causes retain their own mutation,
// concurrency and traversal rules. This package never calls their Error methods.
type Error struct{ state *errorState }

type errorState struct {
	diagnostic Diagnostic
	causes     []error
}

// New constructs the common occurrence core, including a declared detail-contract
// reference when present. A component may compose it into its own Occurrence type;
// NewDetailed is a convenience, not the only extension path. The component owns
// satisfying its runtime detail contract. Metadata alone does not prove data exists.
// New performs no catalog lookup. Rejection returns no occurrence and leaves every
// input with the caller; it never invents the requested identity.
func New(definition Definition, location Location, causes ...error) (*Error, error) {
	if problem := validateDefinition(definition); problem != 0 {
		return nil, reject(problem)
	}
	if !location.Valid() {
		return nil, reject(ErrLocation)
	}
	if len(causes) > MaxCauses {
		return nil, reject(ErrLimit)
	}
	kept := make([]error, 0, len(causes))
	for _, cause := range causes {
		if cause != nil {
			kept = append(kept, cause)
		}
	}
	return &Error{state: &errorState{
		diagnostic: Diagnostic{
			Definition: copyDefinition(definition),
			Location:   Location{Operation: strings.Clone(location.Operation), Instance: strings.Clone(location.Instance)},
			CauseCount: len(kept),
		},
		causes: kept,
	}}, nil
}

// Diagnostic returns frozen safe metadata, without native text or typed details.
// Nil and zero errors have an invalid zero-code diagnostic.
func (err *Error) Diagnostic() Diagnostic {
	if err == nil || err.state == nil {
		return Diagnostic{}
	}
	return err.state.diagnostic
}

// Error returns numeric identity, symbolic identity and the code-owned developer
// explanation. It never selects a locale or formats details/native causes.
func (err *Error) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.state == nil {
		return "failure: invalid occurrence"
	}
	definition := err.state.diagnostic.Definition
	return definition.Code.String() + " (" + string(definition.Identifier) + "): " + definition.Message
}

// Is matches only this occurrence's Code. Standard errors.Is separately traverses
// deliberately retained causes; matching a descendant does not identify a primary.
func (err *Error) Is(target error) bool {
	code, ok := target.(Code)
	return ok && code.Valid() && err != nil && err.state != nil && code == err.state.diagnostic.Definition.Code
}

// Unwrap copies the slots but retains exact native error objects. Deliberate cause
// access is sensitive, not a redacted diagnostic operation. A single cause still
// uses the multi-cause contract; errors.Is/As support it, errors.Unwrap does not.
func (err *Error) Unwrap() []error {
	if err == nil || err.state == nil {
		return nil
	}
	return append([]error(nil), err.state.causes...)
}

func (err Error) Format(state fmt.State, verb rune) {
	formatError(state, verb, (&err).Error())
}

func formatError(state fmt.State, verb rune, text string) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", text)
	} else {
		_, _ = io.WriteString(state, text)
	}
}

// LogValue emits separate stable fields, never native text or runtime details.
func (err *Error) LogValue() slog.Value {
	if err == nil || err.state == nil {
		return slog.StringValue(err.Error())
	}
	value := err.state.diagnostic
	return slog.GroupValue(
		slog.String("code", value.Definition.Code.String()),
		slog.String("identifier", string(value.Definition.Identifier)),
		slog.String("module", value.Definition.Module),
		slog.String("component", value.Definition.Component),
		slog.String("operation", value.Location.Operation),
		slog.String("instance", value.Location.Instance),
		slog.String("message", value.Definition.Message),
		slog.Int("cause_count", value.CauseCount),
	)
}

// MarshalJSON refuses to turn runtime causes/details into an implicit wire schema.
func (Error) MarshalJSON() ([]byte, error) { return nil, reject(ErrSerialization) }
func (*Error) UnmarshalJSON([]byte) error  { return reject(ErrSerialization) }

// Occurrence is the direct-extension seam. Failure must always return the same
// current core, never a core found by traversing causes. Extensions own their
// additional data and runtime formatting. NewDetailed provides this composition
// without requiring a component to reimplement forwarding methods.
type Occurrence interface {
	error
	Failure() *Error
}

// Failure makes the core itself a directly inspectable occurrence.
func (err *Error) Failure() *Error { return err }

// Inspect selects the supplied occurrence only. Wrappers, joins and bare Codes
// do not become occurrences by containing one. Typed nils are rejected before
// invoking a foreign accessor. Valid extensions must keep their accessor bounded.
func Inspect(err error) (*Error, bool) {
	occurrence, ok := err.(Occurrence)
	if !ok {
		return nil, false
	}
	value := reflect.ValueOf(occurrence)
	switch value.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface, reflect.Func, reflect.Chan:
		if value.IsNil() {
			return nil, false
		}
	}
	core := occurrence.Failure()
	if core == nil || core.state == nil {
		return nil, false
	}
	return core, true
}
