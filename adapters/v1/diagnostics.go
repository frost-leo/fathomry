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

package adapters

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

func (Runtime) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Runtime") }
func (Runtime) String() string                    { return "adapters.Runtime" }
func (Runtime) GoString() string                  { return "adapters.Runtime" }
func (*Runtime) LogValue() slog.Value             { return slog.StringValue("adapters.Runtime") }
func (Runtime) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Runtime) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Endpoint[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Endpoint") }
func (Endpoint[T]) String() string                    { return "adapters.Endpoint" }
func (Endpoint[T]) GoString() string                  { return "adapters.Endpoint" }
func (*Endpoint[T]) LogValue() slog.Value             { return slog.StringValue("adapters.Endpoint") }
func (Endpoint[T]) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Endpoint[T]) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Call[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Call") }
func (Call[T]) String() string                    { return "adapters.Call" }
func (Call[T]) GoString() string                  { return "adapters.Call" }
func (*Call[T]) LogValue() slog.Value             { return slog.StringValue("adapters.Call") }
func (Call[T]) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Call[T]) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Scope) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Scope") }
func (Scope) String() string                    { return "adapters.Scope" }
func (Scope) GoString() string                  { return "adapters.Scope" }
func (*Scope) LogValue() slog.Value             { return slog.StringValue("adapters.Scope") }
func (Scope) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Scope) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Guard) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Guard") }
func (Guard) String() string                    { return "adapters.Guard" }
func (Guard) GoString() string                  { return "adapters.Guard" }
func (*Guard) LogValue() slog.Value             { return slog.StringValue("adapters.Guard") }
func (Guard) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Guard) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Receipt[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Receipt") }
func (Receipt[T]) String() string                    { return "adapters.Receipt" }
func (Receipt[T]) GoString() string                  { return "adapters.Receipt" }
func (*Receipt[T]) LogValue() slog.Value             { return slog.StringValue("adapters.Receipt") }
func (Receipt[T]) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Receipt[T]) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Snapshot[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Snapshot") }
func (Snapshot[T]) String() string                    { return "adapters.Snapshot" }
func (Snapshot[T]) GoString() string                  { return "adapters.Snapshot" }
func (*Snapshot[T]) LogValue() slog.Value             { return slog.StringValue("adapters.Snapshot") }
func (Snapshot[T]) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Snapshot[T]) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Inbox[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Inbox") }
func (Inbox[T]) String() string                    { return "adapters.Inbox" }
func (Inbox[T]) GoString() string                  { return "adapters.Inbox" }
func (*Inbox[T]) LogValue() slog.Value             { return slog.StringValue("adapters.Inbox") }
func (Inbox[T]) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Inbox[T]) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Delivery[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Delivery") }
func (Delivery[T]) String() string                    { return "adapters.Delivery" }
func (Delivery[T]) GoString() string                  { return "adapters.Delivery" }
func (*Delivery[T]) LogValue() slog.Value             { return slog.StringValue("adapters.Delivery") }
func (Delivery[T]) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Delivery[T]) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Declaration[T]) Format(state fmt.State, verb rune) {
	formatHandle(state, verb, "adapters.Declaration")
}
func (Declaration[T]) String() string        { return "adapters.Declaration" }
func (Declaration[T]) GoString() string      { return "adapters.Declaration" }
func (*Declaration[T]) LogValue() slog.Value { return slog.StringValue("adapters.Declaration") }
func (Declaration[T]) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Declaration[T]) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Outcome[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Outcome") }
func (Outcome[T]) String() string                    { return "adapters.Outcome" }
func (Outcome[T]) GoString() string                  { return "adapters.Outcome" }
func (*Outcome[T]) LogValue() slog.Value             { return slog.StringValue("adapters.Outcome") }
func (Outcome[T]) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Outcome[T]) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Observer) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Observer") }
func (Observer) String() string                    { return "adapters.Observer" }
func (Observer) GoString() string                  { return "adapters.Observer" }
func (*Observer) LogValue() slog.Value             { return slog.StringValue("adapters.Observer") }
func (Observer) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Observer) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Info) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Info") }
func (Info) String() string                    { return "adapters.Info" }
func (Info) GoString() string                  { return "adapters.Info" }
func (*Info) LogValue() slog.Value             { return slog.StringValue("adapters.Info") }
func (Info) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Info) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}

func (Request) Format(state fmt.State, verb rune) { formatHandle(state, verb, "adapters.Request") }
func (Request) String() string                    { return "adapters.Request" }
func (Request) GoString() string                  { return "adapters.Request" }
func (*Request) LogValue() slog.Value             { return slog.StringValue("adapters.Request") }
func (Request) MarshalJSON() ([]byte, error) {
	return nil, failureOf(ErrSerialization, "marshal", "", Details{})
}
func (*Request) UnmarshalJSON([]byte) error {
	return failureOf(ErrSerialization, "unmarshal", "", Details{})
}
