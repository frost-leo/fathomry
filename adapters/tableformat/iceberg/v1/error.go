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

package iceberg

import (
	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
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
	codes, forward := errorbridge.Inspect(err, 128, codeForKind)
	if forward != nil {
		return forward
	}
	code := ErrOperation
	if operation == "cleanup" || operation == "close" {
		code = ErrCleanup
	}
	if len(codes) == 1 {
		code = codes[0]
	}
	if code.Facility() != failure.FacilityIceberg {
		code = ErrOperation
	}
	causes := []error{err}
	for _, kind := range codes {
		causes = append(causes, kind)
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
	case native.ErrProtocol:
		return ErrProtocol
	case native.ErrLimit:
		return ErrLimit
	case native.ErrOperation:
		return ErrOperation
	case native.ErrCleanup, source.ErrCleanup, source.ErrIncomplete, invocation.ErrCleanup:
		return ErrCleanup
	case source.ErrCapacity:
		return adapters.ErrLimit
	case invocation.ErrEvidence:
		return adapters.ErrEvidence
	}
	return 0
}
