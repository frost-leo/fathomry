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

package doris

import (
	"errors"

	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	sdk "github.com/go-sql-driver/mysql"
)

func fail(code failure.Code, operation string, causes ...error) error {
	location := failure.Location{Operation: operation}
	if code.Facility() == failure.FacilityOperation {
		for _, def := range adapters.Definitions() {
			if def.Code == code {
				value, err := failure.NewDetailed(def, location, adapters.Details{}, func(v adapters.Details) adapters.Details { return v }, causes...)
				if err != nil {
					return err
				}
				return value
			}
		}
	}
	if code.Facility() == failure.FacilityResource {
		for _, def := range resource.Definitions() {
			if def.Code == code {
				value, err := failure.NewDetailed(def, location, resource.Details{}, func(v resource.Details) resource.Details { return v }, causes...)
				if err != nil {
					return err
				}
				return value
			}
		}
	}
	value, err := failure.New(definition(code), location, causes...)
	if err != nil {
		return err
	}
	return value
}

// InspectError deliberately exposes a borrowed sensitive driver error. The
// MySQL wire error type does not confer MySQL/InnoDB transaction semantics.
func InspectError(err error) (*sdk.MySQLError, bool) {
	var value *sdk.MySQLError
	ok := errors.As(err, &value)
	return value, ok && value != nil
}

func translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	if errorbridge.Classified(err) {
		return err
	}
	codes, forwarded := inspectNative(err)
	if forwarded != nil {
		return forwarded
	}
	code := adapters.ErrOperation
	if len(codes) > 0 {
		code = codes[0]
	}
	causes := []error{err}
	if len(codes) > 1 {
		identities := make([]error, 0, len(codes)-1)
		for _, nested := range codes[1:] {
			identities = append(identities, nested)
		}
		causes = append(causes, errors.Join(identities...))
	}
	return fail(code, operation, causes...)
}

func inspectNative(err error) ([]failure.Code, error) {
	var fallback failure.Code
	codes, forwarded := errorbridge.Inspect(err, 128, func(kind fault.Kind) failure.Code {
		switch kind {
		case source.ErrConfiguration, source.ErrSelection:
			fallback = resource.ErrSelection
		case source.ErrAssembly, source.ErrInitialization:
			if fallback == 0 {
				fallback = resource.ErrBuild
			}
		}
		return codeForKind(kind)
	})
	if len(codes) == 0 && forwarded == nil && fallback != 0 {
		codes = append(codes, fallback)
	}
	return codes, forwarded
}

func codeForKind(kind fault.Kind) failure.Code {
	switch kind {
	case native.ErrInput:
		return ErrInput
	case native.ErrUnsupported:
		return ErrUnsupported
	case native.ErrLimit:
		return ErrLimit
	case native.ErrProtocol:
		return ErrProtocol
	case native.ErrTransport:
		return ErrTransport
	case native.ErrSQL:
		return ErrSQL
	case native.ErrLoad:
		return ErrLoad
	case native.ErrUncertain:
		return ErrUncertain
	case native.ErrRowQuality:
		return ErrRowQuality
	case native.ErrDuplicate:
		return ErrDuplicate
	case native.ErrCleanup:
		return ErrCleanup
	case native.ErrState:
		return ErrState
	case invocation.ErrEvidence:
		return adapters.ErrEvidence
	case invocation.ErrAttempts:
		return adapters.ErrLimit
	case invocation.ErrInvalid, invocation.ErrBudget:
		return adapters.ErrRequest
	case invocation.ErrWait:
		return adapters.ErrWait
	case invocation.ErrPending:
		return adapters.ErrPending
	case invocation.ErrState:
		return adapters.ErrHandle
	case source.ErrCapacity:
		return resource.ErrLimit
	case source.ErrCleanup, source.ErrIncomplete:
		return resource.ErrCleanup
	}
	return 0
}
