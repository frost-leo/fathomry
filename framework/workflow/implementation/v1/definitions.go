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

package implementation

import (
	"context"

	"github.com/frost-leo/fathomry/failure/v1"
)

// Stable codes describe binding/inspection failures, never retry or deployment policy.
const (
	ErrInvalid     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowImplementation)<<16 | 1
	ErrUnsupported failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowImplementation)<<16 | 2
	ErrLimit       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowImplementation)<<16 | 3
	ErrCanceled    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowImplementation)<<16 | 4
	ErrHandle      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowImplementation)<<16 | 5
)

// Diagnostic has fixed reasons and bounded numeric paths, not caller data or types.
type Diagnostic struct {
	Code   failure.Code `json:"code"`
	Reason string       `json:"reason"`
	Path   string       `json:"path,omitempty"`
}

func definition(code failure.Code) failure.Definition {
	name, message := "invalid_binding", "The implementation binding is invalid."
	switch code {
	case ErrUnsupported:
		name, message = "unsupported_profile", "The implementation ABI or manifest profile is unsupported."
	case ErrLimit:
		name, message = "limit_exceeded", "An implementation inspection limit was exceeded."
	case ErrCanceled:
		name, message = "canceled", "Implementation inspection was canceled."
	case ErrHandle:
		name, message = "invalid_handle", "The implementation handle or access route is invalid."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.workflow_implementation." + name), Module: "fathomry", Component: "workflow_implementation", Revision: 1, Message: message, Details: failure.Contract{ID: "fathomry.workflow_implementation.diagnostic", Version: 1}}
}

// Definitions returns independent component-owned definitions for explicit composition.
func Definitions() []failure.Definition {
	result := []failure.Definition{}
	for _, code := range []failure.Code{ErrInvalid, ErrUnsupported, ErrLimit, ErrCanceled, ErrHandle} {
		result = append(result, definition(code))
	}
	return result
}
func problem(code failure.Code, reason, path string, causes ...error) error {
	result, err := failure.NewDetailed(definition(code), failure.Location{Operation: "implementation"}, Diagnostic{code, reason, path}, func(value Diagnostic) Diagnostic { return value }, causes...)
	if err != nil {
		return err
	}
	return result
}
func canceled(ctx context.Context) error {
	if ctx == nil {
		return problem(ErrHandle, "context", "")
	}
	if ctx.Err() != nil {
		return problem(ErrCanceled, "canceled", "", ctx.Err(), context.Cause(ctx))
	}
	return nil
}

// Diagnose selects a direct, identity-consistent completed validation failure,
// not a cause inside a wrapper or join.
func Diagnose(err error) (Diagnostic, bool) {
	value, ok := err.(*failure.Detailed[Diagnostic])
	if !ok {
		return Diagnostic{}, false
	}
	detail, ok := value.Details()
	if !ok {
		return Diagnostic{}, false
	}
	core, ok := failure.Inspect(err)
	if !ok || core.Diagnostic().Definition.Code != detail.Code || core.Diagnostic().Definition.Component != "workflow_implementation" {
		return Diagnostic{}, false
	}
	switch detail.Code {
	case ErrInvalid, ErrUnsupported, ErrLimit:
		return detail, true
	default:
		return Diagnostic{}, false
	}
}
