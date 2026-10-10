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

package temporal

import (
	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
)

// Error separates safe public presentation from the exact scoped native failure.
// Returning this wrapper to a native FailureConverter is not wire-transparent.
type Error struct {
	private
	safe, semantic error
}

func (err *Error) Error() string {
	if err == nil || err.safe == nil {
		return "temporal: failure"
	}
	return err.safe.Error()
}
func (err *Error) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.safe
}

// Failure forwards only the already-classified safe core for localization.
func (err *Error) Failure() *failure.Error {
	if err == nil {
		return nil
	}
	core, _ := failure.Inspect(err.safe)
	return core
}

// NativeError deliberately exposes the exact error captured at a known native
// return boundary. It does not search arbitrary graphs or rewrite user errors.
// Native errors can contain sensitive data. Lazy Details remain use-scoped.
func NativeError(err error) (error, bool) {
	value, ok := err.(*Error)
	if !ok || value == nil || value.semantic == nil {
		return nil, false
	}
	return value.semantic, true
}

func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}
func translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*Error); ok {
		return err
	}
	semantic, hasSemantic := native.NativeError(err)
	if errorbridge.Classified(err) && !hasSemantic {
		return err
	}
	codes, forwarded := errorbridge.Inspect(err, 128, codeForKind)
	if forwarded != nil && !hasSemantic {
		return forwarded
	}
	code := ErrExecution
	if operation == "prepare" || operation == "validate" {
		code = ErrInput
	}
	if operation == "close" || operation == "cleanup" {
		code = ErrCleanup
	}
	causes := []error{err}
	if len(codes) > 0 {
		code = codes[0]
		for _, nested := range codes[1:] {
			causes = append(causes, nested)
		}
	}
	var safe error
	if code.Facility() == failure.FacilityOperation {
		for _, def := range adapters.Definitions() {
			if def.Code == code {
				safe, _ = failure.New(def, failure.Location{Operation: operation}, causes...)
				break
			}
		}
	} else {
		safe = fail(code, operation, causes...)
	}
	if hasSemantic {
		return &Error{safe: safe, semantic: semantic}
	}
	return safe
}
func codeForKind(kind fault.Kind) failure.Code {
	switch kind {
	case native.ErrInput, source.ErrConfiguration:
		return ErrInput
	case native.ErrAuthority:
		return ErrAuthority
	case native.ErrConnect:
		return ErrConnect
	case native.ErrRPC:
		return ErrRPC
	case native.ErrLimit, source.ErrCapacity:
		return ErrLimit
	case native.ErrCleanup, source.ErrCleanup, source.ErrIncomplete, invocation.ErrCleanup:
		return ErrCleanup
	case native.ErrExecution:
		return ErrExecution
	case native.ErrWorker:
		return ErrWorker
	case native.ErrTask:
		return ErrTask
	case invocation.ErrEvidence:
		return adapters.ErrEvidence
	case invocation.ErrWait:
		return adapters.ErrWait
	case invocation.ErrState:
		return adapters.ErrClosed
	case invocation.ErrInvalid:
		return adapters.ErrOptions
	}
	return 0
}
