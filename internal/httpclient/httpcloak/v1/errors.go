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

package httpcloak

import (
	"errors"
	"fmt"
	"github.com/frost-leo/fathomry/internal/fault"
	"io"
	"log/slog"
)

const ProviderID = "httpclient.httpcloak.v1"
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

func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "httpcloak[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("httpcloak[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("httpcloak: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("httpcloak: runtime reconstruction unsupported")
}
