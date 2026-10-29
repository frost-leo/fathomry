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

package configuration

import (
	"fmt"
	"log/slog"
)

func restricted(state fmt.State, name string) { _, _ = state.Write([]byte("configuration." + name)) }

func (Bootstrap) Format(state fmt.State, _ rune) { restricted(state, "Bootstrap") }
func (Bootstrap) LogValue() slog.Value           { return slog.StringValue("configuration.Bootstrap") }

func (BootstrapInput) Format(state fmt.State, _ rune) { restricted(state, "BootstrapInput") }
func (BootstrapInput) LogValue() slog.Value           { return slog.StringValue("configuration.BootstrapInput") }

func (File) Format(state fmt.State, _ rune) { restricted(state, "File") }
func (File) LogValue() slog.Value           { return slog.StringValue("configuration.File") }

func (Remote) Format(state fmt.State, _ rune) { restricted(state, "Remote") }
func (Remote) LogValue() slog.Value           { return slog.StringValue("configuration.Remote") }

func (Options) Format(state fmt.State, _ rune) { restricted(state, "Options") }
func (Options) LogValue() slog.Value           { return slog.StringValue("configuration.Options") }
func (Options) MarshalJSON() ([]byte, error)   { return nil, at(ErrSerialization, "marshal", -1) }
func (*Options) UnmarshalJSON([]byte) error    { return at(ErrSerialization, "unmarshal", -1) }

func (Dependencies) Format(state fmt.State, _ rune) { restricted(state, "Dependencies") }
func (Dependencies) LogValue() slog.Value           { return slog.StringValue("configuration.Dependencies") }
func (Dependencies) MarshalJSON() ([]byte, error)   { return nil, at(ErrSerialization, "marshal", -1) }
func (*Dependencies) UnmarshalJSON([]byte) error    { return at(ErrSerialization, "unmarshal", -1) }

func (Update) Format(state fmt.State, _ rune) { restricted(state, "Update") }
func (Update) LogValue() slog.Value           { return slog.StringValue("configuration.Update") }
func (Update) MarshalJSON() ([]byte, error)   { return nil, at(ErrSerialization, "marshal", -1) }
func (*Update) UnmarshalJSON([]byte) error    { return at(ErrSerialization, "unmarshal", -1) }

func (Status) Format(state fmt.State, _ rune) { restricted(state, "Status") }
func (Status) LogValue() slog.Value           { return slog.StringValue("configuration.Status") }
func (Status) MarshalJSON() ([]byte, error)   { return nil, at(ErrSerialization, "marshal", -1) }
func (*Status) UnmarshalJSON([]byte) error    { return at(ErrSerialization, "unmarshal", -1) }

func (Session[T]) Format(state fmt.State, _ rune) { restricted(state, "Session") }
func (Session[T]) LogValue() slog.Value           { return slog.StringValue("configuration.Session") }
func (Session[T]) MarshalJSON() ([]byte, error)   { return nil, at(ErrSerialization, "marshal", -1) }
func (*Session[T]) UnmarshalJSON([]byte) error    { return at(ErrSerialization, "unmarshal", -1) }
