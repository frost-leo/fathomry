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
	"io"
	"log/slog"
	"reflect"
	"strings"
)

// MaxCodeBytes bounds an identity in bytes.
const MaxCodeBytes = 128

// Code is a semantic identity and errors.Is target, not an occurrence, message
// resource key, classification hierarchy or retry instruction. Use constant codes
// owned by the defining capability. The fathomry namespace is reserved by convention
// for the framework; namespaces are not authorization. Never put secrets in codes.
//
// Adding a code is not permission to reclassify an existing operation's failures.
// Preserve its promised code/matching and typed detail behavior or version the
// operation contract. Namespace prefixes and causes imply no classification.
type Code string

// Invalid identifies a malformed construction or zero Error, never success.
// It is not the fallback for unfamiliar codes, native errors or unknown effects.
const Invalid Code = "fathomry.failure.invalid"

// Valid reports whether code has at least two dot-separated segments and at most
// MaxCodeBytes bytes. Each segment starts with a lowercase ASCII letter, followed
// by lowercase letters, digits, underscores or hyphens. It consults no registry.
func (code Code) Valid() bool {
	value := string(code)
	if len(value) == 0 || len(value) > MaxCodeBytes {
		return false
	}
	start, dots := true, 0
	for index := range len(value) {
		char := value[index]
		if char == '.' {
			if start {
				return false
			}
			start = true
			dots++
			continue
		}
		if start {
			if char < 'a' || char > 'z' {
				return false
			}
		} else if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
		start = false
	}
	return !start && dots > 0
}

// Error returns a bounded identity, replacing malformed values with Invalid.
func (code Code) Error() string {
	if !code.Valid() {
		return string(Invalid)
	}
	return string(code)
}

// Format uses the same safe identity for every verb; q quotes it.
// Width, precision and flags cannot enlarge the output or reveal raw fields.
func (code Code) Format(state fmt.State, verb rune) { format(state, verb, code.Error()) }

// LogValue returns only the safe identity.
func (code Code) LogValue() slog.Value { return slog.StringValue(code.Error()) }

// Error is an immutable value handle. Its zero value denotes Invalid. Copying it
// shares only immutable owned storage and the intentionally borrowed cause.
// There are no setters or transforming methods. Use Code for semantic
// comparisons, not equality of separately constructed Error handles.
type Error struct{ state *state }

type state struct {
	code       Code
	cause      error
	attributes []Attribute
	omitted    bool
}

// New always returns a failure, even for an invalid code (which becomes Invalid).
// Unknown valid codes are preserved. Cause is intentionally public through Unwrap;
// pass nil to withhold native internals while retaining their evidence elsewhere.
// A direct nil or typed-nil cause is absent. Other cause graphs are borrowed intact,
// not traversed, normalized, copied or bounded. Callers own their lifetime, size,
// acyclicity and concurrent-use rules. Use errors.Join for deliberate multiple causes.
//
// Attributes are optional public diagnostics, never required machine details.
// Invalid, duplicate or oversized sets are entirely omitted with Diagnostic.Omitted
// set; identity and cause survive. Valid attributes (including string bytes) are
// copied and sorted. Caller storage must not be concurrently mutated during New.
func New(code Code, cause error, attributes ...Attribute) Error {
	if !code.Valid() {
		code = Invalid
	}
	if nilError(cause) {
		cause = nil
	}
	frozen, valid := freezeAttributes(attributes)
	return Error{state: &state{
		code: Code(strings.Clone(string(code))), cause: cause,
		attributes: frozen, omitted: !valid,
	}}
}

// Code identifies this occurrence only, not any cause.
func (err Error) Code() Code {
	if err.state == nil {
		return Invalid
	}
	return err.state.code
}

// Error returns only the code: a bounded, catalog-free human fallback.
// Neither optional attributes, typed details nor native causes are formatted.
func (err Error) Error() string { return string(err.Code()) }

// Is matches an exact valid Code. Standard errors.Is may additionally find codes
// in causes; a tree match does not identify the described occurrence.
func (err Error) Is(target error) bool {
	code, ok := target.(Code)
	return ok && code.Valid() && err.Code() == code
}

// As exposes the common Error value when this method is promoted by a typed
// extension. It preserves that inspection contract without making the embedded
// occurrence a fake cause. Like errors.Is, errors.As still searches a whole tree.
func (err Error) As(target any) bool {
	value, ok := target.(*Error)
	if !ok || value == nil {
		return false
	}
	*value = err
	return true
}

// Unwrap deliberately exposes the borrowed cause, including its raw inspection
// and formatting behavior. errors.Is/As on external graphs can run arbitrary code.
func (err Error) Unwrap() error {
	if err.state == nil {
		return nil
	}
	return err.state.cause
}

// Failure returns this occurrence. Typed capability errors may promote this
// method through an unexported embedded alias of Error for Inspect.
func (err Error) Failure() Error { return err }

// Inspect describes err itself if it implements Failure() Error. It does not
// unwrap, invoke Is/As, or search joins. Nil, direct typed nil, bare Code targets,
// and unrecognized errors return a zero Error and false; ignore the value then.
// An extension's Failure method must describe itself, not search its causes.
// Owned methods are pure; arbitrary extension callbacks remain the caller's trust.
func Inspect(err error) (Error, bool) {
	if nilError(err) {
		return Error{}, false
	}
	described, ok := err.(interface{ Failure() Error })
	if !ok {
		return Error{}, false
	}
	return described.Failure(), true
}

// Format protects both value and pointer formatting, including sharp formatting.
func (err Error) Format(state fmt.State, verb rune) { format(state, verb, err.Error()) }

// LogValue intentionally excludes attributes, typed details and causes.
// Explicit projections of public details require the caller's privacy policy.
func (err Error) LogValue() slog.Value {
	return slog.GroupValue(slog.String("code", err.Error()),
		slog.Bool("diagnostics_omitted", err.state != nil && err.state.omitted))
}

// MarshalJSON refuses to invent a runtime cause-graph or durable error protocol.
// As with any nil pointer, encoding/json may encode a nil *Error as null without
// calling this method; that is not an occurrence round trip.
func (Error) MarshalJSON() ([]byte, error) {
	return nil, errors.New("failure: runtime error serialization is unsupported")
}

// UnmarshalJSON always refuses, including null, without mutating the receiver.
func (*Error) UnmarshalJSON([]byte) error {
	return errors.New("failure: runtime error reconstruction is unsupported")
}

func nilError(err error) bool {
	if err == nil {
		return true
	}
	value := reflect.ValueOf(err)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func format(state fmt.State, verb rune, value string) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", value)
	} else {
		_, _ = io.WriteString(state, value)
	}
}
