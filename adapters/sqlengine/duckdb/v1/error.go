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

package duckdb

import (
	"errors"

	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

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
	if errorbridge.Classified(err) {
		return err
	}
	code := ErrNative
	if operation == "validate" {
		code = ErrInput
	}
	if operation == "cleanup" || operation == "close" {
		code = ErrCleanup
	}
	classifications, forwarded := inspectNative(err)
	if forwarded != nil {
		return forwarded
	}
	causes := []error{err}
	if len(classifications) != 0 {
		code = classifications[0]
		for _, nested := range classifications[1:] {
			causes = append(causes, nested)
		}
	}
	return fail(code, operation, causes...)
}

// Specific native frames keep precedence; a neutral configuration failure maps
// to input only when no recognized native or already-public boundary owns it.
func inspectNative(err error) ([]failure.Code, error) {
	configuration := false
	codes, forwarded := errorbridge.Inspect(err, 128, func(kind fault.Kind) failure.Code {
		configuration = configuration || kind == source.ErrConfiguration
		return codeForKind(kind)
	})
	if len(codes) == 0 && forwarded == nil && configuration {
		codes = append(codes, ErrInput)
	}
	return codes, forwarded
}

func codeForKind(kind fault.Kind) failure.Code {
	switch kind {
	case native.ErrInput, invocation.ErrInvalid:
		return ErrInput
	case native.ErrUnsupported:
		return ErrUnsupported
	case native.ErrNative:
		return ErrNative
	case native.ErrLimit, source.ErrCapacity, invocation.ErrEvidence:
		return ErrLimit
	case native.ErrState, invocation.ErrState:
		return ErrState
	case native.ErrCleanup, invocation.ErrCleanup, source.ErrCleanup, source.ErrIncomplete:
		return ErrCleanup
	}
	return 0
}

func outcomeError(value adapters.Snapshot[Result]) error {
	return combineResultErrors(value.Info().Operation, value.Primary(), value.Cleanup())
}

// Preserve independent phase facts while selecting the primary failure for
// presentation. A sole occurrence keeps its exact identity and typed details.
func combineResultErrors(operation string, primary, secondary error) error {
	if primary == nil {
		return secondary
	}
	if secondary == nil {
		return primary
	}
	public := primary
	core, classified := failure.Inspect(public)
	if !classified {
		_, public = inspectNative(primary)
		core, classified = failure.Inspect(public)
	}
	if classified {
		diagnostic := core.Diagnostic()
		location := diagnostic.Location
		location.Operation = operation
		if diagnostic.Definition.Details.ID == "" {
			combined, err := failure.New(diagnostic.Definition, location, primary, secondary)
			if err == nil {
				return combined
			}
		} else if diagnostic.Definition.Details == (failure.Contract{ID: "fathomry.operation.details", Version: 1}) {
			if details, present := errorbridge.Details[adapters.Details](primary, core, 128); present {
				combined, err := failure.NewDetailed(diagnostic.Definition, location, details, func(value adapters.Details) adapters.Details { return value }, primary, secondary)
				if err == nil {
					return combined
				}
			}
		}
		return errorbridge.Forward(errors.Join(primary, secondary), public)
	}
	if public != nil {
		return errorbridge.Forward(errors.Join(primary, secondary), errors.Join(public, translate(secondary, "cleanup")))
	}
	return fail(ErrNative, operation, primary, secondary)
}
