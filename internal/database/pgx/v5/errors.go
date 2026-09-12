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

package pgx

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies this PostgreSQL implementation, independently of options,
// source names, consuming module versions and service versions.
const ProviderID = "database.pgx.v5"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrEnvironment fault.Kind = "fathomry." + ProviderID + ".environment"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrConnect     fault.Kind = "fathomry." + ProviderID + ".connect"
	ErrQuery       fault.Kind = "fathomry." + ProviderID + ".query"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrState       fault.Kind = "fathomry." + ProviderID + ".state"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

type private struct{}

func (private) String() string   { return "pgx[restricted]" }
func (private) GoString() string { return "pgx[restricted]" }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("pgx: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("pgx: runtime reconstruction unsupported")
}

// Value formatters also guard populated values inside containers and numeric
// verbs. Ordinary nil-pointer fmt uses its native <nil> fallback; pointer slog
// handlers below are explicitly nil-safe. Invalid verbs bypassing fmt.Formatter
// (for example %w on a non-error) are not supported diagnostic operations.
func restricted(state fmt.State)                   { _, _ = io.WriteString(state, "pgx[restricted]") }
func (OptionsV1) Format(state fmt.State, _ rune)   { restricted(state) }
func (Source) Format(state fmt.State, _ rune)      { restricted(state) }
func (Database) Format(state fmt.State, _ rune)    { restricted(state) }
func (Transaction) Format(state fmt.State, _ rune) { restricted(state) }
func (Result) Format(state fmt.State, _ rune)      { restricted(state) }
func (Row) Format(state fmt.State, _ rune)         { restricted(state) }
func (Column) Format(state fmt.State, _ rune)      { restricted(state) }
func (*OptionsV1) LogValue() slog.Value            { return slog.StringValue("pgx[restricted]") }
func (*Source) LogValue() slog.Value               { return slog.StringValue("pgx[restricted]") }
func (*Database) LogValue() slog.Value             { return slog.StringValue("pgx[restricted]") }
func (*Transaction) LogValue() slog.Value          { return slog.StringValue("pgx[restricted]") }
func (*Result) LogValue() slog.Value               { return slog.StringValue("pgx[restricted]") }
func (*Row) LogValue() slog.Value                  { return slog.StringValue("pgx[restricted]") }
func (*Column) LogValue() slog.Value               { return slog.StringValue("pgx[restricted]") }
