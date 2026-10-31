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

func (Schema[T]) Format(state fmt.State, _ rune) { restricted(state, "Schema") }
func (Schema[T]) LogValue() slog.Value           { return slog.StringValue("configuration.Schema") }
func (Schema[T]) MarshalJSON() ([]byte, error) {
	return nil, at(ErrSerialization, "marshal_schema", -1)
}
func (*Schema[T]) UnmarshalJSON([]byte) error   { return at(ErrSerialization, "unmarshal_schema", -1) }
func (Variable) Format(state fmt.State, _ rune) { restricted(state, "Variable") }
func (Variable) LogValue() slog.Value           { return slog.StringValue("configuration.Variable") }

func (BootstrapInput) Format(state fmt.State, _ rune) { restricted(state, "BootstrapInput") }
func (BootstrapInput) LogValue() slog.Value           { return slog.StringValue("configuration.BootstrapInput") }

func (Bootstrap) Format(state fmt.State, _ rune) { restricted(state, "Bootstrap") }
func (Bootstrap) LogValue() slog.Value           { return slog.StringValue("configuration.Bootstrap") }

func (SourcePlan) Format(state fmt.State, _ rune) { restricted(state, "SourcePlan") }
func (SourcePlan) LogValue() slog.Value           { return slog.StringValue("configuration.SourcePlan") }

func (Remote) Format(state fmt.State, _ rune) { restricted(state, "Remote") }
func (Remote) LogValue() slog.Value           { return slog.StringValue("configuration.Remote") }

func (Input) Format(state fmt.State, _ rune) { restricted(state, "Input") }
func (Input) LogValue() slog.Value           { return slog.StringValue("configuration.Input") }
func (Input) MarshalJSON() ([]byte, error)   { return nil, at(ErrSerialization, "marshal", -1) }
func (*Input) UnmarshalJSON([]byte) error    { return at(ErrSerialization, "unmarshal", -1) }

func (Policy) Format(state fmt.State, _ rune) { restricted(state, "Policy") }
func (Policy) LogValue() slog.Value           { return slog.StringValue("configuration.Policy") }
func (Policy) MarshalJSON() ([]byte, error)   { return nil, at(ErrSerialization, "marshal", -1) }
func (*Policy) UnmarshalJSON([]byte) error    { return at(ErrSerialization, "unmarshal", -1) }

func (Evidence) Format(state fmt.State, _ rune) { restricted(state, "Evidence") }
func (Evidence) LogValue() slog.Value           { return slog.StringValue("configuration.Evidence") }
func (Evidence) MarshalJSON() ([]byte, error)   { return nil, at(ErrSerialization, "marshal", -1) }
func (*Evidence) UnmarshalJSON([]byte) error    { return at(ErrSerialization, "unmarshal", -1) }

func (Loaded) Format(state fmt.State, _ rune) { restricted(state, "Loaded") }
func (Loaded) LogValue() slog.Value           { return slog.StringValue("configuration.Loaded") }
func (Loaded) MarshalJSON() ([]byte, error)   { return nil, at(ErrSerialization, "marshal", -1) }
func (*Loaded) UnmarshalJSON([]byte) error    { return at(ErrSerialization, "unmarshal", -1) }

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
