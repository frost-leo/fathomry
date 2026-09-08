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

// Package failure supplies public, process-local error identities and occurrences
// for composition, internal implementations, adapters, and external consumers.
// It has no Provider, localization, retry, orchestration, or wire-format dependency.
// Native causes are retained for deliberate inspection, never ordinary diagnostics.
package failure

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
)

// Definition is a code-owned semantic identity. Version changes when its meaning
// changes, independently of module, configuration, and any future wire versions.
// Code is namespaced (for example, fathomry.source.invalid_configuration).
// Definitions are explicit values; there is no mutable global catalog.
type Definition struct {
	Code      string
	Version   uint32
	Component string
}

// Identity is a declaration, not an occurrence. Its zero value is invalid.
type Identity struct{ definition Definition }

// Define validates a declaration. It does not register it or perform I/O.
func Define(definition Definition) (Identity, error) {
	if !label(definition.Code, 128) || !label(definition.Component, 64) || definition.Version == 0 {
		return Identity{}, errors.New("failure: invalid definition")
	}
	return Identity{definition: definition}, nil
}

// MustDefine is for tested static declarations only.
func MustDefine(definition Definition) Identity {
	identity, err := Define(definition)
	if err != nil {
		panic("failure: invalid definition")
	}
	return identity
}

func (identity Identity) Definition() Definition { return identity.definition }
func (identity Identity) Error() string {
	if identity.definition.Code == "" {
		return "fathomry.failure.invalid"
	}
	return identity.definition.Code
}

// Attribution holds explicitly supplied process-local source attribution.
// Empty fields mean absent. Assembly is the composition scope, not a business impact,
// quota, transaction, or execution scope. Labels confer no authorization.
// Composition must supply non-sensitive labels; syntax validation is not redaction.
type Attribution struct {
	Operation string
	Provider  string
	Assembly  string
	Source    string
	Execution Execution
}

// Execution associates one logical call with its caller-owned execution. Call and
// Parent are logical call IDs, not SDK attempt IDs; Parent cannot equal Call.
// Run and Item are optional;
// Item requires Run. Attempt is the execution attempt (zero means unspecified).
// Owner is an optional opaque effect-ownership scope, not authorization or quota.
// IDs contain 1–128 ASCII letters, digits, dots, underscores or hyphens when set.
// Their uniqueness and non-sensitive meaning belong to the calling boundary.
type Execution struct {
	Call    string
	Parent  string
	Run     string
	Item    string
	Owner   string
	Attempt uint32
}

// Valid checks representation and the Item/Run relationship, not authenticity,
// existence, global uniqueness, or business permission. Empty execution is valid
// for errors outside a call; controlled operations additionally require Call.
func (execution Execution) Valid() bool {
	for _, value := range []string{execution.Call, execution.Parent, execution.Run, execution.Item, execution.Owner} {
		if len(value) > 128 {
			return false
		}
		for _, char := range value {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
				return false
			}
		}
	}
	return (execution.Item == "" || execution.Run != "") &&
		(execution.Parent == "" || execution.Parent != execution.Call)
}

// Valid checks bounded, explicitly supplied attribution without performing I/O.
func (attribution Attribution) Valid() bool { return validAttribution(attribution) }

// Diagnostic is a bounded process-local root projection, not a persisted or wire
// contract. It deliberately excludes causes, native text, and arbitrary arguments.
// Complete failures remain available through Go cause inspection and owner results.
type Diagnostic struct {
	Definition  Definition
	Attribution Attribution
	HasCauses   bool
}

// Error is an immutable occurrence. Copies and concurrent reads are safe; retained
// native causes themselves remain subject to their authors' concurrency contracts.
type Error struct{ state *errorState }

type errorState struct {
	diagnostic Diagnostic
	causes     []error
}

// New creates an occurrence without guessing facts from causes. A cancellation
// cause never changes identity, proves resource release, or sets retry policy.
// Invalid attribution produces a distinct invalid occurrence while retaining causes.
func (identity Identity) New(attribution Attribution, causes ...error) *Error {
	definition := identity.definition
	if definition.Code == "" || !validAttribution(attribution) {
		definition = Definition{Code: "fathomry.failure.invalid", Component: "failure", Version: 1}
		attribution = Attribution{}
	}
	kept := make([]error, 0, len(causes))
	for _, cause := range causes {
		if cause != nil {
			kept = append(kept, cause)
		}
	}
	return &Error{state: &errorState{
		diagnostic: Diagnostic{Definition: definition, Attribution: attribution, HasCauses: len(kept) != 0},
		causes:     kept,
	}}
}

// Diagnostic returns a value-only projection, not the original cause graph.
func (err *Error) Diagnostic() Diagnostic {
	if err == nil || err.state == nil {
		return Diagnostic{Definition: Definition{Code: "fathomry.failure.invalid", Version: 1, Component: "failure"}}
	}
	return err.state.diagnostic
}

func (err *Error) Error() string {
	if err == nil {
		return "<nil>"
	}
	return err.Diagnostic().Definition.Code
}

// Is matches declarations by full definition, not occurrences merely because they
// share a code. errors.As retrieves the actual occurrence and its attribution.
func (err *Error) Is(target error) bool {
	identity, ok := target.(Identity)
	return err != nil && ok && identity.definition.Code != "" && err.Diagnostic().Definition == identity.definition
}

// Unwrap returns an independent slice of original causes, preserving errors.Is/As.
// This is only a Go contract: Temporal's default converter does not preserve this
// multi-cause graph. No cross-process or historical reconstruction is implemented.
func (err *Error) Unwrap() []error {
	if err == nil || err.state == nil {
		return nil
	}
	return append([]error(nil), err.state.causes...)
}

// Format prevents fmt, including %#v and copied values, from traversing raw causes.
func (err Error) Format(state fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", err.Error())
	} else {
		_, _ = io.WriteString(state, err.Error())
	}
}

// MarshalJSON rejects accidental use of runtime errors as durable/wire payloads.
// A future versioned protocol must explicitly select which facts it preserves.
func (err Error) MarshalJSON() ([]byte, error) {
	return nil, errors.New("failure: runtime error serialization is unsupported")
}

// UnmarshalJSON rejects diagnostic data rather than pretending to reconstruct an
// occurrence, its identity, or its missing native causes.
func (err *Error) UnmarshalJSON([]byte) error {
	return errors.New("failure: diagnostic decoding is not error reconstruction")
}

// LogValue emits fixed identity fields only; source attribution is an explicit
// projection, not an automatic metric label or an exporter dependency.
func (err *Error) LogValue() slog.Value {
	if err == nil {
		return slog.StringValue("<nil>")
	}
	definition := err.Diagnostic().Definition
	return slog.GroupValue(slog.String("code", definition.Code), slog.Uint64("definition_version", uint64(definition.Version)))
}

func validAttribution(value Attribution) bool {
	for _, field := range []string{value.Operation, value.Provider, value.Assembly, value.Source} {
		if field != "" && !label(field, 64) {
			return false
		}
	}
	return value.Execution.Valid()
}

func label(value string, limit int) bool {
	if value == "" || len(value) > limit {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-' || char == '.') {
			return false
		}
	}
	return true
}
