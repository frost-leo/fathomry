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
	"errors"
	"fmt"
	"log/slog"

	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
)

func translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	code := ErrRead
	if operation == "close" {
		code = ErrClose
	}
	if direct, ok := err.(interface{ Is(error) bool }); ok {
		for _, entry := range []struct {
			native error
			public failure.Code
		}{
			{native.ErrInput, ErrInput}, {native.ErrLimit, ErrLimit}, {native.ErrRead, ErrRead},
			{native.ErrDecode, ErrDecode}, {native.ErrClose, ErrClose}, {native.ErrClosed, ErrClosed}, {native.ErrState, ErrState},
		} {
			if direct.Is(entry.native) {
				code = entry.public
				break
			}
		}
	} else if errors.Is(err, native.ErrClose) {
		// Owned file reads can join a read failure and a close failure. Do not
		// search through an already-classified outer native occurrence.
		code = ErrClose
	}
	return fail(code, operation, err)
}

type private struct{}

func (private) String() string                 { return "viper[restricted]" }
func (private) GoString() string               { return "viper[restricted]" }
func (private) Format(state fmt.State, _ rune) { restricted(state) }
func (private) MarshalJSON() ([]byte, error)   { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error    { return fail(ErrSerialization, "unmarshal") }
func restricted(state fmt.State)               { _, _ = state.Write([]byte("viper[restricted]")) }

func (*Client) Format(state fmt.State, _ rune)       { restricted(state) }
func (*Client) LogValue() slog.Value                 { return slog.StringValue("viper[restricted]") }
func (*Input) Format(state fmt.State, _ rune)        { restricted(state) }
func (*Input) LogValue() slog.Value                  { return slog.StringValue("viper[restricted]") }
func (*Document) Format(state fmt.State, _ rune)     { restricted(state) }
func (*Document) LogValue() slog.Value               { return slog.StringValue("viper[restricted]") }
func (*Snapshot) Format(state fmt.State, _ rune)     { restricted(state) }
func (*Snapshot) LogValue() slog.Value               { return slog.StringValue("viper[restricted]") }
func (*Subscription) Format(state fmt.State, _ rune) { restricted(state) }
func (*Subscription) LogValue() slog.Value           { return slog.StringValue("viper[restricted]") }
func (*Change) Format(state fmt.State, _ rune)       { restricted(state) }
func (*Change) LogValue() slog.Value                 { return slog.StringValue("viper[restricted]") }
func (Settings) Format(state fmt.State, _ rune)      { restricted(state) }
func (Settings) LogValue() slog.Value                { return slog.StringValue("viper[restricted]") }
func (Scalar) Format(state fmt.State, _ rune)        { restricted(state) }
func (Scalar) LogValue() slog.Value                  { return slog.StringValue("viper[restricted]") }
func (Default) Format(state fmt.State, _ rune)       { restricted(state) }
func (Default) LogValue() slog.Value                 { return slog.StringValue("viper[restricted]") }
func (Binding) Format(state fmt.State, _ rune)       { restricted(state) }
func (Binding) LogValue() slog.Value                 { return slog.StringValue("viper[restricted]") }
func (Replacement) Format(state fmt.State, _ rune)   { restricted(state) }
func (Replacement) LogValue() slog.Value             { return slog.StringValue("viper[restricted]") }
func (WatchSettings) Format(state fmt.State, _ rune) { restricted(state) }
func (WatchSettings) LogValue() slog.Value           { return slog.StringValue("viper[restricted]") }
