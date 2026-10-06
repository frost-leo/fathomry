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

package kafka

import (
	"errors"

	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	"github.com/twmb/franz-go/pkg/kerr"
)

func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}

// InspectError deliberately exposes a borrowed native broker cause. Its text may
// contain sensitive data; do not log it blindly. Absence proves no effect state.
func InspectError(err error) (*kerr.Error, bool) {
	var value *kerr.Error
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
	codes, forwarded := errorbridge.Inspect(err, 128, codeForKind)
	if forwarded != nil {
		return forwarded
	}
	code := ErrState
	if len(codes) > 0 {
		code = codes[0]
	}
	causes := []error{err}
	if len(codes) > 1 {
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
	case native.ErrIdentity:
		return ErrIdentity
	case native.ErrProduce:
		return ErrProduce
	case native.ErrRead:
		return ErrRead
	case native.ErrMissing:
		return ErrMissing
	case native.ErrUnavailable:
		return ErrUnavailable
	case native.ErrExpired:
		return ErrExpired
	case native.ErrLimit:
		return ErrLimit
	case native.ErrState:
		return ErrState
	case native.ErrTransaction:
		return ErrTransaction
	case native.ErrOffsets:
		return ErrOffsets
	case native.ErrCleanup:
		return ErrCleanup
	case source.ErrCapacity, invocation.ErrEvidence:
		return ErrLimit
	case source.ErrConfiguration:
		return ErrInput
	case source.ErrIncomplete, source.ErrCleanup, invocation.ErrCleanup:
		return ErrCleanup
	}
	return 0
}
