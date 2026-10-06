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

package viper

import (
	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
	"github.com/frost-leo/fathomry/internal/fault"
)

const errorTraversalLimit = 128

var nativeErrorMappings = [...]struct {
	native fault.Kind
	public failure.Code
}{
	{native.ErrInput, ErrInput}, {native.ErrLimit, ErrLimit}, {native.ErrRead, ErrRead},
	{native.ErrDecode, ErrDecode}, {native.ErrClose, ErrClose}, {native.ErrClosed, ErrClosed}, {native.ErrState, ErrState},
}

func codeForKind(kind fault.Kind) failure.Code {
	for _, entry := range nativeErrorMappings {
		if entry.native == kind {
			return entry.public
		}
	}
	return 0
}

func translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	if errorbridge.Classified(err) {
		return err
	}
	code := ErrRead
	if operation == "close" {
		code = ErrClose
	}
	direct, hasNativeIdentity := err.(interface{ Is(error) bool })
	if hasNativeIdentity {
		for _, entry := range nativeErrorMappings {
			if direct.Is(entry.native) {
				return fail(entry.public, operation, err)
			}
		}
	}
	codes, forwarded := errorbridge.Inspect(err, errorTraversalLimit, codeForKind)
	if forwarded != nil {
		return forwarded
	}
	if !hasNativeIdentity {
		for _, candidate := range codes {
			if candidate == ErrClose {
				code = ErrClose
				break
			}
		}
	}
	return fail(code, operation, err)
}

func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}
