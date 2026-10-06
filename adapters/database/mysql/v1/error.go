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

package mysql

import (
	"errors"

	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/database/mysql/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/go-sql-driver/mysql"
)

func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}

// InspectError finds the first native server error without erasing its identity.
// The returned error is borrowed and sensitive; do not mutate it or log it blindly.
// Absence proves neither no dispatch nor no remote effects.
// Translated errors keep their nearest native classification as the outer public
// occurrence, expose recognized nested identities through errors.Is, and retain
// the original native graph as the first cause. No identity is retry/effect policy.
func InspectError(err error) (*sdk.MySQLError, bool) {
	var value *sdk.MySQLError
	ok := errors.As(err, &value) && value != nil
	return value, ok
}

func translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	if errorbridge.Classified(err) {
		return err
	}
	code := ErrQuery
	if operation == "open" || operation == "validate" {
		code = ErrInput
	}
	if operation == "cleanup" || operation == "close" {
		code = ErrCleanup
	}
	classifications, forwarded := errorbridge.Inspect(err, 128, codeForKind)
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

func codeForKind(kind fault.Kind) failure.Code {
	switch kind {
	case native.ErrInput:
		return ErrInput
	case native.ErrUnsupported:
		return ErrUnsupported
	case native.ErrConnect:
		return ErrConnect
	case native.ErrQuery:
		return ErrQuery
	case native.ErrLimit, source.ErrCapacity:
		return ErrLimit
	case native.ErrState:
		return ErrState
	case native.ErrCleanup, invocation.ErrCleanup, source.ErrCleanup, source.ErrIncomplete:
		return ErrCleanup
	case native.ErrProtocol:
		return ErrProtocol
	}
	return 0
}

// outcomeError preserves independent phase facts while selecting primary failure
// for direct presentation. A sole occurrence keeps its identity and typed details.
func outcomeError(value adapters.Snapshot[Result]) error {
	return combineResultErrors(value.Info().Operation, value.Primary(), value.Cleanup())
}

func combineResultErrors(operation string, primary, secondary error) error {
	if primary == nil {
		return secondary
	}
	if secondary == nil {
		return primary
	}
	if core, ok := failure.Inspect(primary); ok {
		diagnostic := core.Diagnostic()
		location := diagnostic.Location
		location.Operation = operation
		if diagnostic.Definition.Details.ID == "" {
			combined, err := failure.New(diagnostic.Definition, location, primary, secondary)
			if err == nil {
				return combined
			}
		} else if details, present := errorbridge.Details[adapters.Details](primary, core, 128); present {
			combined, err := failure.NewDetailed(diagnostic.Definition, location, details, func(value adapters.Details) adapters.Details { return value }, primary, secondary)
			if err == nil {
				return combined
			}
		}
		return errorbridge.Forward(errors.Join(primary, secondary), primary)
	}
	if _, public := errorbridge.Inspect(primary, 128, codeForKind); public != nil {
		return errorbridge.Forward(errors.Join(primary, secondary), errors.Join(public, translate(secondary, "cleanup")))
	}
	return fail(ErrQuery, operation, primary, secondary)
}
