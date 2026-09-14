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

package iceberg

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

const ProviderID = "tableformat.iceberg.v0"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrAuthority   fault.Kind = "fathomry." + ProviderID + ".authority"
	ErrProtocol    fault.Kind = "fathomry." + ProviderID + ".protocol"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrOperation   fault.Kind = "fathomry." + ProviderID + ".operation"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "iceberg[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("iceberg[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("iceberg: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("iceberg: runtime reconstruction unsupported")
}

func (*OptionsV1) LogValue() slog.Value       { return slog.StringValue("iceberg[restricted]") }
func (*Source) LogValue() slog.Value          { return slog.StringValue("iceberg[restricted]") }
func (*Client) LogValue() slog.Value          { return slog.StringValue("iceberg[restricted]") }
func (*Result) LogValue() slog.Value          { return slog.StringValue("iceberg[restricted]") }
func (*FileEffect) LogValue() slog.Value      { return slog.StringValue("iceberg[restricted]") }
func (*SchemaChange) LogValue() slog.Value    { return slog.StringValue("iceberg[restricted]") }
func (*PartitionChange) LogValue() slog.Value { return slog.StringValue("iceberg[restricted]") }
func (*Predicate) LogValue() slog.Value       { return slog.StringValue("iceberg[restricted]") }
func (*BatchWrite) LogValue() slog.Value      { return slog.StringValue("iceberg[restricted]") }
func (*BatchRead) LogValue() slog.Value       { return slog.StringValue("iceberg[restricted]") }
