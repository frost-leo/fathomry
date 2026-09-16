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

package nuki

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

const ProviderID = "httpclient.nuki.v1"

const (
	ErrInput       fault.Kind = "fathomry.httpclient.nuki.input"
	ErrState       fault.Kind = "fathomry.httpclient.nuki.state"
	ErrUnsupported fault.Kind = "fathomry.httpclient.nuki.unsupported"
	ErrCapacity    fault.Kind = "fathomry.httpclient.nuki.capacity"
	ErrTransport   fault.Kind = "fathomry.httpclient.nuki.transport"
	ErrRead        fault.Kind = "fathomry.httpclient.nuki.read"
	ErrIntegrity   fault.Kind = "fathomry.httpclient.nuki.integrity"
	ErrLimit       fault.Kind = "fathomry.httpclient.nuki.limit"
	ErrCallback    fault.Kind = "fathomry.httpclient.nuki.callback"
	ErrCleanup     fault.Kind = "fathomry.httpclient.nuki.cleanup"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "nuki[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("nuki[restricted]") }
func (private) MarshalJSON() ([]byte, error)   { return nil, failure(ErrUnsupported, "serialize") }
func (private) UnmarshalJSON([]byte) error     { return failure(ErrUnsupported, "deserialize") }
