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

package minio

import (
	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
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
	if err == nil {
		return nil
	}
	if errorbridge.Classified(err) {
		return err
	}
	codes, forwarded := errorbridge.Inspect(err, 128, codeForKind)
	if forwarded != nil {
		return forwarded
	}
	code := adapters.ErrOperation
	if operation == "validate" || operation == "open" {
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

func codeForKind(kind fault.Kind) failure.Code {
	switch kind {
	case native.ErrInput:
		return ErrInput
	case native.ErrUnsupported:
		return ErrUnsupported
	case native.ErrAuthority:
		return ErrAuthority
	case native.ErrConnect:
		return ErrConnect
	case native.ErrRead:
		return ErrRead
	case native.ErrWrite:
		return ErrWrite
	case native.ErrList:
		return ErrList
	case native.ErrRemove:
		return ErrRemove
	case native.ErrMissing:
		return ErrMissing
	case native.ErrDenied:
		return ErrDenied
	case native.ErrExpired:
		return ErrExpired
	case native.ErrCondition:
		return ErrCondition
	case native.ErrIntegrity:
		return ErrIntegrity
	case native.ErrLimit:
		return ErrLimit
	case native.ErrProtocol:
		return ErrProtocol
	case native.ErrCleanup:
		return ErrCleanup
	case native.ErrState:
		return ErrState
	case invocation.ErrEvidence:
		return adapters.ErrEvidence
	case source.ErrCapacity, invocation.ErrAttempts:
		return adapters.ErrLimit
	case source.ErrIncomplete, source.ErrCleanup, invocation.ErrCleanup:
		return ErrCleanup
	}
	return 0
}
