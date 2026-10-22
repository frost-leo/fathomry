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

package declaration

import (
	"context"

	"github.com/frost-leo/fathomry/failure/v1"
)

// Stable workflow-declaration failure identities, independent of SDK/layer versions.
const (
	ErrInvalid     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflow)<<16 | 0x0001
	ErrUnsupported failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflow)<<16 | 0x0002
	ErrIntegrity   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflow)<<16 | 0x0003
	ErrLimit       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflow)<<16 | 0x0004
	ErrCanceled    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflow)<<16 | 0x0005
	ErrHandle      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflow)<<16 | 0x0006
)

// Diagnostic is a bounded safe reason and JSON Pointer, never a source value,
// native error or locale-selected message. It is detached from the occurrence.
type Diagnostic struct {
	Code   failure.Code `json:"code"`
	Reason string       `json:"reason"`
	Path   string       `json:"path"`
}

// Definitions returns component-owned declarations for explicit catalog assembly.
func Definitions() []failure.Definition {
	return []failure.Definition{
		definition(ErrInvalid), definition(ErrUnsupported), definition(ErrIntegrity),
		definition(ErrLimit), definition(ErrCanceled), definition(ErrHandle),
	}
}

func definition(code failure.Code) failure.Definition {
	var name, message string
	switch code {
	case ErrInvalid:
		name, message = "invalid_declaration", "The workflow declaration is invalid."
	case ErrUnsupported:
		name, message = "unsupported_profile", "The workflow document format or profile is unsupported."
	case ErrIntegrity:
		name, message = "integrity_failed", "The workflow document fingerprint does not match its content."
	case ErrLimit:
		name, message = "limit_exceeded", "A workflow declaration admission limit was exceeded."
	case ErrCanceled:
		name, message = "canceled", "Workflow declaration processing was canceled."
	case ErrHandle:
		name, message = "invalid_handle", "A workflow declaration handle or input set is invalid."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.workflow." + name),
		Module: "fathomry", Component: "workflow", Revision: 1, Message: message,
		Details: failure.Contract{ID: "fathomry.workflow.diagnostic", Version: 1}}
}

func problem(code failure.Code, reason, path string, causes ...error) error {
	if path == "/" {
		path = ""
	}
	value, err := failure.NewDetailed(definition(code), failure.Location{Operation: "declaration"},
		Diagnostic{Code: code, Reason: reason, Path: path}, func(value Diagnostic) Diagnostic { return value }, causes...)
	if err != nil {
		return err
	}
	return value
}

func invalid(reason, path string) error { return problem(ErrInvalid, reason, path) }

// Diagnose selects only a directly supplied workflow validation occurrence.
// It does not traverse wrapped/joined causes or turn cancellation/handle failures
// into completed checks. Callers retain the original error independently.
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
	if !ok || core.Diagnostic().Definition.Code != detail.Code || core.Diagnostic().Definition.Component != "workflow" {
		return Diagnostic{}, false
	}
	switch detail.Code {
	case ErrInvalid, ErrUnsupported, ErrIntegrity, ErrLimit:
		return detail, true
	default:
		return Diagnostic{}, false
	}
}

func canceled(ctx context.Context) error {
	if ctx == nil {
		return problem(ErrHandle, "context", "/")
	}
	if err := ctx.Err(); err != nil {
		return problem(ErrCanceled, "canceled", "/", err, context.Cause(ctx))
	}
	return nil
}
