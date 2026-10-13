/*
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

package chromedp

import (
	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	"github.com/frost-leo/fathomry/internal/fault"
	source "github.com/frost-leo/fathomry/internal/resource"
)

func receiptError(receipt *adapters.Receipt[Result], primary error) error {
	if primary != nil || receipt == nil {
		return primary
	}
	value, resolved := receipt.Snapshot()
	if !resolved {
		return nil
	}
	return value.Err()
}

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
	codes, forward := errorbridge.Inspect(err, 128, func(kind fault.Kind) failure.Code {
		switch kind {
		case native.ErrInput:
			return ErrInput
		case native.ErrState:
			return ErrState
		case native.ErrLimit, source.ErrCapacity:
			return ErrLimit
		case native.ErrCleanup, source.ErrCleanup, source.ErrIncomplete:
			return ErrCleanup
		case native.ErrUnsupported:
			return ErrUnsupported
		}
		return 0
	})
	if forward != nil {
		return forward
	}
	code := ErrOperation
	if operation == "close" || operation == "cleanup" {
		code = ErrCleanup
	}
	if len(codes) == 1 {
		code = codes[0]
	}
	return fail(code, operation, err)
}
