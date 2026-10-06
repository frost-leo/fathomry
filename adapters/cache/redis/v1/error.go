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

package redis

import (
	"errors"

	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/redis/go-redis/v9"
)

func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}
func ownedCode(capability Capability, code failure.Code) failure.Code {
	if capability == Messaging {
		return code&^(failure.Code(failure.MaxFacility)<<16) | failure.Code(failure.FacilityRedisMessaging)<<16
	}
	return code
}
func problem(capability Capability, code failure.Code, operation string, causes ...error) error {
	return fail(ownedCode(capability, code), operation, causes...)
}

// InspectError deliberately returns a native server error; its text may be
// sensitive. Absence says nothing about external effects.
func InspectError(err error) (sdk.Error, bool) {
	var value sdk.Error
	ok := errors.As(err, &value)
	return value, ok
}

// IsNull and IsWatchConflict inspect native sentinels without requiring an SDK
// import. They do not imply successful output, retry safety or rollback.
func IsNull(err error) bool          { return errors.Is(err, sdk.Nil) }
func IsWatchConflict(err error) bool { return errors.Is(err, sdk.TxFailedErr) }

func translate(err error, operation string, capability Capability) error {
	remaining := 8192
	return translateWithin(err, operation, capability, &remaining)
}
func translateWithin(err error, operation string, capability Capability, budget *int) error {
	if err == nil {
		return nil
	}
	if *budget <= 0 {
		return problem(capability, ErrState, operation, err)
	}
	*budget--
	if errorbridge.Classified(err) {
		return err
	}
	// Preserve each public cause of a mixed native/callback join rather than
	// assigning a new provider owner to an existing public occurrence.
	internal, isInternal := err.(*fault.Error)
	transparent := !isInternal || internal.Diagnostic().Kind == invocation.ErrFailed
	if single, ok := err.(interface{ Unwrap() error }); ok && !isInternal {
		if cause := single.Unwrap(); cause != nil {
			return errorbridge.Forward(err, translateWithin(cause, operation, capability, budget))
		}
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok && transparent {
		causes := joined.Unwrap()
		result := make([]error, 0, min(len(causes), *budget+1))
		for _, cause := range causes {
			if cause != nil {
				if *budget <= 0 {
					result = append(result, problem(capability, ErrState, operation, err))
					break
				}
				result = append(result, translateWithin(cause, operation, capability, budget))
			}
		}
		translated := combine(result...)
		if translated == nil {
			return problem(capability, ErrState, operation, err)
		}
		if !isInternal && translated != nil {
			return errorbridge.Forward(err, translated)
		}
		return translated
	}
	codes, forwarded := errorbridge.Inspect(err, 128, codeForKind)
	if forwarded != nil {
		return forwarded
	}
	code := ErrCommand
	if len(codes) > 0 {
		code = codes[0]
	}
	causes := []error{err}
	if len(codes) > 1 {
		for _, nested := range codes[1:] {
			causes = append(causes, ownedCode(capability, nested))
		}
	}
	return problem(capability, code, operation, causes...)
}

func codeForKind(kind fault.Kind) failure.Code {
	switch kind {
	case native.ErrInput, source.ErrConfiguration:
		return ErrInput
	case native.ErrAuthority:
		return ErrAuthority
	case native.ErrUnsupported:
		return ErrUnsupported
	case native.ErrLimit, source.ErrCapacity, invocation.ErrEvidence:
		return ErrLimit
	case native.ErrProtocol:
		return ErrProtocol
	case native.ErrCommand:
		return ErrCommand
	case native.ErrState:
		return ErrState
	case native.ErrCleanup, source.ErrIncomplete, source.ErrCleanup, invocation.ErrCleanup:
		return ErrCleanup
	}
	return 0
}
