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

package trino

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies this native SDK-major integration, not a server version.
const ProviderID = "sqlengine.trino.v0"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrAuthority   fault.Kind = "fathomry." + ProviderID + ".authority"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrProtocol    fault.Kind = "fathomry." + ProviderID + ".protocol"
	ErrOperation   fault.Kind = "fathomry." + ProviderID + ".operation"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

func failed(kind fault.Kind, operation string, causes ...error) error {
	if errors.Join(causes...) == nil {
		return nil
	}
	return failure(kind, operation, causes...)
}

type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "trino[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("trino[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("trino: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("trino: runtime reconstruction unsupported")
}
func (*OptionsV1) LogValue() slog.Value   { return slog.StringValue("trino[restricted]") }
func (*Source) LogValue() slog.Value      { return slog.StringValue("trino[restricted]") }
func (*Client) LogValue() slog.Value      { return slog.StringValue("trino[restricted]") }
func (*Result) LogValue() slog.Value      { return slog.StringValue("trino[restricted]") }
func (*Statement) LogValue() slog.Value   { return slog.StringValue("trino[restricted]") }
func (*BatchInsert) LogValue() slog.Value { return slog.StringValue("trino[restricted]") }
func (*Column) LogValue() slog.Value      { return slog.StringValue("trino[restricted]") }
