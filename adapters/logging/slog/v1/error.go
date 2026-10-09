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

package slog

import (
	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/failure/v1"
)

func fail(code failure.Code, operation string, causes ...error) error {
	var definition failure.Definition
	for _, candidate := range logging.Definitions() {
		if candidate.Code == code {
			definition = candidate
			break
		}
	}
	err, invalid := failure.New(definition, failure.Location{Operation: operation}, causes...)
	if invalid != nil {
		return invalid
	}
	return err
}
func safe(err error, operation string) error {
	if err == nil || errorbridge.Classified(err) {
		return err
	}
	if _, forwarded := errorbridge.Inspect(err, 128, nil); forwarded != nil {
		return forwarded
	}
	return fail(logging.ErrState, operation, err)
}
