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

package errorcatalog

import (
	"strings"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

type listOptions struct {
	owners command.OwnerFilter
	query  string
	locale string
}

type explanationOptions struct {
	code       failure.Code
	identifier failure.Identifier
	locale     string
}

func prepareList(input listOptions) (listOptions, error) {
	if err := input.owners.Validate(); err != nil {
		return listOptions{}, err
	}
	if err := (i18n.Preferences{Locale: input.locale}).Validate(); err != nil {
		return listOptions{}, command.Fail(command.ErrUsage, err)
	}
	input.query = strings.ToLower(input.query)
	return input, nil
}

func prepareExplanation(identity, locale string) (explanationOptions, error) {
	if err := (i18n.Preferences{Locale: locale}).Validate(); err != nil {
		return explanationOptions{}, command.Fail(command.ErrUsage, err)
	}
	result := explanationOptions{locale: locale}
	if failure.Identifier(identity).Valid() {
		result.identifier = failure.Identifier(identity)
		return result, nil
	}
	code, err := failure.ParseCode(identity)
	if err != nil {
		return explanationOptions{}, command.Fail(command.ErrUsage, err)
	}
	result.code = code
	return result, nil
}
