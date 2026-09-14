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

package duckdb

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies this provider independently of the SDK module version.
const ProviderID = "sqlengine.duckdb.v2"

// Errors are technical identities, never retry or business-effect decisions.
// Native causes remain available to deliberate errors.Is/As inspection.
const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrNative      fault.Kind = "fathomry." + ProviderID + ".native"
	ErrState       fault.Kind = "fathomry." + ProviderID + ".state"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

func joined(kind fault.Kind, operation string, causes ...error) error {
	for _, cause := range causes {
		if cause != nil {
			return failure(kind, operation, causes...)
		}
	}
	return nil
}

type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "duckdb[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("duckdb[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("duckdb: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("duckdb: runtime reconstruction unsupported")
}
func (*OptionsV1) LogValue() slog.Value { return slog.StringValue("duckdb[restricted]") }
func (*Source) LogValue() slog.Value    { return slog.StringValue("duckdb[restricted]") }
func (*Database) LogValue() slog.Value  { return slog.StringValue("duckdb[restricted]") }
func (*Request) LogValue() slog.Value   { return slog.StringValue("duckdb[restricted]") }
func (*Result) LogValue() slog.Value    { return slog.StringValue("duckdb[restricted]") }
func (*Step) LogValue() slog.Value      { return slog.StringValue("duckdb[restricted]") }
func (*Progress) LogValue() slog.Value  { return slog.StringValue("duckdb[restricted]") }
