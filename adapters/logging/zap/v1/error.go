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

package zap

import (
	"errors"
	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/logging/zap/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
)

func fail(code failure.Code, operation string, causes ...error) error {
	def := definition(code)
	if code.Facility() == failure.FacilityOperation {
		for _, candidate := range adapters.Definitions() {
			if candidate.Code == code {
				def = candidate
				break
			}
		}
	}
	value, err := failure.New(def, failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}
func translate(err error, operation string) error {
	if err == nil || errorbridge.Classified(err) {
		return err
	}
	codes, forwarded := errorbridge.Inspect(err, 128, codeForKind)
	if forwarded != nil {
		return forwarded
	}
	code := adapters.ErrOperation
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
	return fail(code, operation, causes...)
}
func joinErrors(operation string, left, right error) error {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return translate(errors.Join(left, right), operation)
}
func codeForKind(kind fault.Kind) failure.Code {
	switch kind {
	case native.ErrInput, source.ErrConfiguration:
		return ErrInput
	case native.ErrUnsupported:
		return ErrUnsupported
	case native.ErrLimit:
		return ErrLimit
	case native.ErrState:
		return ErrState
	case native.ErrWrite:
		return ErrWrite
	case native.ErrSync:
		return ErrSync
	case native.ErrFile:
		return ErrFile
	case native.ErrRecursion:
		return ErrRecursion
	case native.ErrCleanup, source.ErrCleanup, source.ErrIncomplete, invocation.ErrCleanup:
		return ErrCleanup
	case source.ErrCapacity, invocation.ErrAttempts:
		return adapters.ErrLimit
	case invocation.ErrEvidence:
		return adapters.ErrEvidence
	case invocation.ErrWait:
		return adapters.ErrWait
	case invocation.ErrInvalid:
		return adapters.ErrOptions
	case invocation.ErrState:
		return adapters.ErrClosed
	}
	return 0
}
