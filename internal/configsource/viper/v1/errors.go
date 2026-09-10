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
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

const (
	ErrInput  fault.Kind = errorNamespace + ".input"
	ErrLimit  fault.Kind = errorNamespace + ".limit"
	ErrRead   fault.Kind = errorNamespace + ".read"
	ErrDecode fault.Kind = errorNamespace + ".decode"
	ErrClose  fault.Kind = errorNamespace + ".close"
)

// ProviderID identifies the configsource/Viper/SDK-v1 technical implementation. It is
// neither a Go module version, an input/resource identity, nor OptionsV1's version.
const ProviderID = "configsource.viper.v1"

const errorNamespace = "fathomry." + ProviderID

func fail(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Operation: operation, Provider: ProviderID}, causes...)
}

type private struct{}

func (private) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, "viper[redacted]")
}

// Outer pointer methods avoid the nil dereference in promoted method wrappers.
// Value forms retain fmt redaction and the JSON serialization refusal below.
func (*Document) LogValue() slog.Value  { return redactedLogValue() }
func (*OptionsV1) LogValue() slog.Value { return redactedLogValue() }
func (*LoadInput) LogValue() slog.Value { return redactedLogValue() }
func (*Default) LogValue() slog.Value   { return redactedLogValue() }
func (*Binding) LogValue() slog.Value   { return redactedLogValue() }

func redactedLogValue() slog.Value {
	return slog.StringValue("viper[redacted]")
}
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("viper: runtime configuration serialization is unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("viper: runtime configuration reconstruction is unsupported")
}
