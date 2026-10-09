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

package zerologotel

import (
	"context"
	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

func refuse(operation string) error {
	for _, definition := range zerolog.Definitions() {
		if definition.Code == zerolog.ErrUnsupported {
			value, err := failure.New(definition, failure.Location{Operation: "otel-bridge." + operation})
			if err != nil {
				return err
			}
			return value
		}
	}
	panic("unreachable: zerolog unsupported definition")
}
func classify(original error) zerolog.RecordState {
	remaining := 128
	var inspect func(error) bool
	inspect = func(err error) bool {
		if err == nil || remaining == 0 {
			return false
		}
		remaining--
		code := failure.Code(0)
		if core, ok := failure.Inspect(err); ok {
			code = core.Diagnostic().Definition.Code
		} else if value, ok := err.(failure.Code); ok {
			code = value
		}
		if code != 0 {
			switch code {
			case otel.ErrInput, otel.ErrLimit, adapters.ErrLimit, adapters.ErrEvidence, adapters.ErrWait, resource.ErrUnavailable, resource.ErrLimit, resource.ErrWait:
				return true
			case otel.ErrState:
				return otel.AdmissionCanceled(err)
			case adapters.ErrSource:
				// The source frame may contain a temporary unavailable generation; only a
				// completely inspected known public refusal may keep this target healthy.
			default:
				return false
			}
		}
		if err == context.Canceled || err == context.DeadlineExceeded {
			return true
		}
		if safe, ok := errorbridge.PublicProjection(err); ok {
			return inspect(safe)
		}
		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			children := value.Unwrap()
			if len(children) == 0 || len(children) > remaining {
				return false
			}
			seen := false
			for _, child := range children {
				if child != nil && !inspect(child) {
					return false
				}
				seen = seen || child != nil
			}
			return seen
		case interface{ Unwrap() error }:
			return inspect(value.Unwrap())
		}
		return false
	}
	if inspect(original) {
		return zerolog.RecordRejected
	}
	return zerolog.RecordStopped
}
