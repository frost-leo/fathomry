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

package nethttp

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies integration major v1, not an instance or Go version.
const ProviderID = "httpclient.nethttp.v1"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrState       fault.Kind = "fathomry." + ProviderID + ".state"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrTransport   fault.Kind = "fathomry." + ProviderID + ".transport"
	ErrRead        fault.Kind = "fathomry." + ProviderID + ".read"
	ErrIntegrity   fault.Kind = "fathomry." + ProviderID + ".integrity"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "nethttp[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("nethttp[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("nethttp: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("nethttp: runtime reconstruction unsupported")
}

func (*OptionsV1) LogValue() slog.Value        { return private{}.LogValue() }
func (*RequestOptionsV1) LogValue() slog.Value { return private{}.LogValue() }
func (*NativeOptionsV1) LogValue() slog.Value  { return private{}.LogValue() }
func (*Source) LogValue() slog.Value           { return private{}.LogValue() }
func (*Client) LogValue() slog.Value           { return private{}.LogValue() }
func (*Stream) LogValue() slog.Value           { return private{}.LogValue() }
func (*Connection) LogValue() slog.Value       { return private{}.LogValue() }
func (*Metadata) LogValue() slog.Value         { return private{}.LogValue() }
func (*Result) LogValue() slog.Value           { return private{}.LogValue() }
