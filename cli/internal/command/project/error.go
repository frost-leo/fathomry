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

package project

import (
	"errors"

	"github.com/frost-leo/fathomry/failure/v1"
)

// Conditions describe facts established by this command, not native error text
// or process status. Root cli exposes selected values for independent callers.
const (
	ErrArguments    failure.Condition = "fathomry.cli.project.invalid_arguments"
	ErrIdentity     failure.Condition = "fathomry.cli.project.conflicting_identity"
	ErrSource       failure.Condition = "fathomry.cli.project.source_unusable"
	ErrDestination  failure.Condition = "fathomry.cli.project.destination_unusable"
	ErrExists       failure.Condition = "fathomry.cli.project.destination_exists"
	ErrOverlap      failure.Condition = "fathomry.cli.project.source_destination_overlap"
	ErrPreparation  failure.Condition = "fathomry.cli.project.preparation_failed"
	ErrCreation     failure.Condition = "fathomry.cli.project.creation_incomplete"
	ErrPresentation failure.Condition = "fathomry.cli.project.invalid_presentation"
)

func failed(condition failure.Condition, causes ...error) *failure.Error {
	current, err := failure.New(condition, causes...)
	if err != nil {
		panic(err)
	}
	return current
}

func combine(errs ...error) error {
	var first error
	count := 0
	for _, err := range errs {
		if err != nil {
			first = err
			count++
		}
	}
	if count == 0 {
		return nil
	}
	if count == 1 {
		return first
	}
	return errors.Join(errs...)
}
