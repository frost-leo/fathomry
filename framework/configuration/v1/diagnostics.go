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

type private struct{}

func (Declaration[T]) Format(state fmt.State, _ rune) { restricted(state) }
func (Declaration[T]) LogValue() slog.Value           { return slog.StringValue("configuration.Declaration") }
func (Declaration[T]) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal_declaration")
}
func (*Declaration[T]) UnmarshalJSON([]byte) error {
	return fail(ErrSerialization, "unmarshal_declaration")
}

func (private) String() string                      { return "configuration[restricted]" }
func (private) GoString() string                    { return "configuration[restricted]" }
func (private) Format(state fmt.State, _ rune)      { restricted(state) }
func (private) MarshalJSON() ([]byte, error)        { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error         { return fail(ErrSerialization, "unmarshal") }
func restricted(state fmt.State)                    { _, _ = state.Write([]byte("configuration[restricted]")) }
func (*State[T]) Format(state fmt.State, _ rune)    { restricted(state) }
func (*State[T]) LogValue() slog.Value              { return slog.StringValue("configuration.State") }
func (*Accepted[T]) Format(state fmt.State, _ rune) { restricted(state) }
func (*Accepted[T]) LogValue() slog.Value           { return slog.StringValue("configuration.Accepted") }
func (*Watcher[T]) Format(state fmt.State, _ rune)  { restricted(state) }
func (*Watcher[T]) LogValue() slog.Value            { return slog.StringValue("configuration.Watcher") }
func (*Event) Format(state fmt.State, _ rune)       { restricted(state) }
func (*Event) LogValue() slog.Value                 { return slog.StringValue("configuration.Event") }

func (NacosConnection) Format(state fmt.State, _ rune) { restricted(state) }
func (NacosConnection) LogValue() slog.Value {
	return slog.StringValue("configuration.NacosConnection")
}
func (NacosOptions) Format(state fmt.State, _ rune) { restricted(state) }
func (NacosOptions) LogValue() slog.Value           { return slog.StringValue("configuration.NacosOptions") }
func (InputOptions) Format(state fmt.State, _ rune) { restricted(state) }
func (InputOptions) LogValue() slog.Value           { return slog.StringValue("configuration.InputOptions") }
func (InputBinding) Format(state fmt.State, _ rune) { restricted(state) }
func (InputBinding) LogValue() slog.Value           { return slog.StringValue("configuration.InputBinding") }
func (Variable) Format(state fmt.State, _ rune)     { restricted(state) }
func (Variable) LogValue() slog.Value               { return slog.StringValue("configuration.Variable") }

func (NacosBootstrap) Format(state fmt.State, _ rune)        { restricted(state) }
func (NacosBootstrap) LogValue() slog.Value                  { return slog.StringValue("configuration.NacosBootstrap") }
func (NacosBootstrapOptions) Format(state fmt.State, _ rune) { restricted(state) }
func (NacosBootstrapOptions) LogValue() slog.Value {
	return slog.StringValue("configuration.NacosBootstrapOptions")
}
