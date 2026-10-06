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

package errorbridge

import (
	"fmt"
	"log/slog"

	"github.com/frost-leo/fathomry/failure/v1"
)

func (value *redactedCause) Format(state fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", value.Error())
	} else {
		_, _ = state.Write([]byte(value.Error()))
	}
}
func (value *redactedCause) LogValue() slog.Value   { return slog.StringValue(value.Error()) }
func (*redactedCause) MarshalJSON() ([]byte, error) { return nil, failure.ErrSerialization }
func (*redactedCause) UnmarshalJSON([]byte) error   { return failure.ErrSerialization }

func (value *forwardedOccurrence) Format(state fmt.State, verb rune) { value.core.Format(state, verb) }
func (value *forwardedOccurrence) LogValue() slog.Value              { return value.core.LogValue() }
