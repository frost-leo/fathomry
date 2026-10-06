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

package broker

import (
	"errors"
	"fmt"
	"log/slog"
)

type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("broker[restricted]")) }
func (private) LogValue() slog.Value           { return slog.StringValue("broker[restricted]") }
func (*Info) LogValue() slog.Value             { return private{}.LogValue() }
func (*Attribution) LogValue() slog.Value      { return private{}.LogValue() }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("broker: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("broker: runtime reconstruction unsupported")
}
