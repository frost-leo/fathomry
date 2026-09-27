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

package cli

import (
	"errors"

	"github.com/frost-leo/fathomry/cli/internal/command/project"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/spf13/pflag"
)

// Host conditions identify observed invocation or delivery failures. They do not
// imply rollback, authorize retry, or replace the separately returned exit status.
const (
	ErrInputs     failure.Condition = "fathomry.cli.invalid_inputs"
	ErrDefinition failure.Condition = "fathomry.cli.invalid_definition"
	ErrUsage      failure.Condition = "fathomry.cli.invalid_usage"
	ErrLanguage   failure.Condition = "fathomry.cli.unsupported_language"
	ErrOutput     failure.Condition = "fathomry.cli.output_failed"
)

// Project conditions are owned by the existing project command. These values
// expose its semantic contract without exposing its private implementation.
// Effects and raw causes remain separate; in particular, an incomplete creation
// may have written some or all bytes and does not authorize deletion or retry.
const (
	ErrProjectArguments    = project.ErrArguments
	ErrProjectIdentity     = project.ErrIdentity
	ErrProjectSource       = project.ErrSource
	ErrProjectDestination  = project.ErrDestination
	ErrProjectExists       = project.ErrExists
	ErrProjectOverlap      = project.ErrOverlap
	ErrProjectPreparation  = project.ErrPreparation
	ErrProjectCreation     = project.ErrCreation
	ErrProjectPresentation = project.ErrPresentation
)

func hostFailure(condition failure.Condition, causes ...error) *failure.Error {
	current, err := failure.New(condition, causes...)
	if err != nil {
		panic(err)
	}
	return current
}

func usageFailure(err error) error {
	if _, ok := err.(failure.Occurrence); ok {
		return err
	}
	// This is the native wrapper around our own language selector, not a search
	// through arbitrary causes for a preferred condition or another error's facts.
	if invalid, ok := err.(*pflag.InvalidValueError); ok && invalid.Unwrap() == errLanguage {
		return hostFailure(ErrLanguage, err)
	}
	return hostFailure(ErrUsage, err)
}

// combine preserves an opaque singleton and joins independent supplied slots.
// It does not inspect, flatten or deduplicate any caller-owned graph.
func combine(errs ...error) error {
	var first error
	count := 0
	for _, err := range errs {
		if err != nil {
			first = err
			count++
		}
	}
	if count == 1 {
		return first
	}
	if count == 0 {
		return nil
	}
	return errors.Join(errs...)
}

func diagnosticKey(err error, status int) string {
	if output, ok := err.(*outputFailure); ok && output != nil && output.core != nil && output.core.Diagnostic().Condition == ErrOutput {
		return "output_failed"
	}
	if current, ok := err.(*failure.Error); ok && current != nil {
		switch current.Diagnostic().Condition {
		case ErrUsage, ErrLanguage:
			return string(current.Diagnostic().Condition)[len("fathomry.cli."):]
		case ErrInputs, ErrDefinition, ErrOutput:
			return string(current.Diagnostic().Condition)[len("fathomry.cli."):]
		}
	}
	if status == 2 {
		return "invalid"
	}
	if status == 130 {
		return "canceled"
	}
	return "failed"
}
