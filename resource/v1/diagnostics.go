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

package resource

import (
	"fmt"
	"log/slog"
)

func formatHandle(state fmt.State, verb rune, name string) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", name)
		return
	}
	_, _ = state.Write([]byte(name))
}

func (Scope) Format(state fmt.State, verb rune) { formatHandle(state, verb, "resource.Scope") }
func (Scope) String() string                    { return "resource.Scope" }
func (Scope) GoString() string                  { return "resource.Scope" }
func (*Scope) LogValue() slog.Value             { return slog.StringValue("resource.Scope") }
func (Scope) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal", "", Details{})
}
func (*Scope) UnmarshalJSON([]byte) error { return fail(ErrSerialization, "unmarshal", "", Details{}) }

func (Ref[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "resource.Ref") }
func (Ref[T]) String() string                    { return "resource.Ref" }
func (Ref[T]) GoString() string                  { return "resource.Ref" }
func (*Ref[T]) LogValue() slog.Value             { return slog.StringValue("resource.Ref") }
func (Ref[T]) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal", "", Details{})
}
func (*Ref[T]) UnmarshalJSON([]byte) error { return fail(ErrSerialization, "unmarshal", "", Details{}) }

func (Lease[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "resource.Lease") }
func (Lease[T]) String() string                    { return "resource.Lease" }
func (Lease[T]) GoString() string                  { return "resource.Lease" }
func (*Lease[T]) LogValue() slog.Value             { return slog.StringValue("resource.Lease") }
func (Lease[T]) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal", "", Details{})
}
func (*Lease[T]) UnmarshalJSON([]byte) error {
	return fail(ErrSerialization, "unmarshal", "", Details{})
}

func (Binding[C, T]) Format(state fmt.State, verb rune) {
	formatHandle(state, verb, "resource.Binding")
}
func (Binding[C, T]) String() string        { return "resource.Binding" }
func (Binding[C, T]) GoString() string      { return "resource.Binding" }
func (*Binding[C, T]) LogValue() slog.Value { return slog.StringValue("resource.Binding") }
func (Binding[C, T]) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal", "", Details{})
}
func (*Binding[C, T]) UnmarshalJSON([]byte) error {
	return fail(ErrSerialization, "unmarshal", "", Details{})
}

func (Instance[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "resource.Instance") }
func (Instance[T]) String() string                    { return "resource.Instance" }
func (Instance[T]) GoString() string                  { return "resource.Instance" }
func (*Instance[T]) LogValue() slog.Value             { return slog.StringValue("resource.Instance") }
func (Instance[T]) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal", "", Details{})
}
func (*Instance[T]) UnmarshalJSON([]byte) error {
	return fail(ErrSerialization, "unmarshal", "", Details{})
}

func (Update) Format(state fmt.State, verb rune) { formatHandle(state, verb, "resource.Update") }
func (Update) String() string                    { return "resource.Update" }
func (Update) GoString() string                  { return "resource.Update" }
func (*Update) LogValue() slog.Value             { return slog.StringValue("resource.Update") }
func (Update) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal", "", Details{})
}
func (*Update) UnmarshalJSON([]byte) error { return fail(ErrSerialization, "unmarshal", "", Details{}) }

func (Watch) Format(state fmt.State, verb rune) { formatHandle(state, verb, "resource.Watch") }
func (Watch) String() string                    { return "resource.Watch" }
func (Watch) GoString() string                  { return "resource.Watch" }
func (*Watch) LogValue() slog.Value             { return slog.StringValue("resource.Watch") }
func (Watch) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal", "", Details{})
}
func (*Watch) UnmarshalJSON([]byte) error { return fail(ErrSerialization, "unmarshal", "", Details{}) }

func (Observation) Format(state fmt.State, verb rune) {
	formatHandle(state, verb, "resource.Observation")
}
func (Observation) String() string        { return "resource.Observation" }
func (Observation) GoString() string      { return "resource.Observation" }
func (*Observation) LogValue() slog.Value { return slog.StringValue("resource.Observation") }
func (Observation) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal", "", Details{})
}
func (*Observation) UnmarshalJSON([]byte) error {
	return fail(ErrSerialization, "unmarshal", "", Details{})
}

func (Status) Format(state fmt.State, verb rune) { formatHandle(state, verb, "resource.Status") }
func (Status) String() string                    { return "resource.Status" }
func (Status) GoString() string                  { return "resource.Status" }
func (*Status) LogValue() slog.Value             { return slog.StringValue("resource.Status") }
func (Status) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal", "", Details{})
}
func (*Status) UnmarshalJSON([]byte) error { return fail(ErrSerialization, "unmarshal", "", Details{}) }

func (ReleaseResult) Format(state fmt.State, verb rune) {
	formatHandle(state, verb, "resource.ReleaseResult")
}
func (ReleaseResult) String() string        { return "resource.ReleaseResult" }
func (ReleaseResult) GoString() string      { return "resource.ReleaseResult" }
func (*ReleaseResult) LogValue() slog.Value { return slog.StringValue("resource.ReleaseResult") }
func (ReleaseResult) MarshalJSON() ([]byte, error) {
	return nil, fail(ErrSerialization, "marshal", "", Details{})
}
func (*ReleaseResult) UnmarshalJSON([]byte) error {
	return fail(ErrSerialization, "unmarshal", "", Details{})
}
