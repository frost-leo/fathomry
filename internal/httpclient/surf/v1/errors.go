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

package surf

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies this integration major, not an instance or SDK version.
const ProviderID = "httpclient.surf.v1"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrState       fault.Kind = "fathomry." + ProviderID + ".state"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrTransport   fault.Kind = "fathomry." + ProviderID + ".transport"
	ErrRead        fault.Kind = "fathomry." + ProviderID + ".read"
	ErrIntegrity   fault.Kind = "fathomry." + ProviderID + ".integrity"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
	ErrCallback    fault.Kind = "fathomry." + ProviderID + ".callback"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

func invoke(phase string, callback func() error) (err error) {
	returned := false
	defer func() {
		if !returned {
			cause, _ := recover().(error)
			err = failure(ErrCallback, phase, cause)
		}
	}()
	err = callback()
	returned = true
	return err
}

type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "surf[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("surf[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, failure(ErrUnsupported, "runtime-serialization")
}
func (*private) UnmarshalJSON([]byte) error    { return failure(ErrUnsupported, "runtime-reconstruction") }
func (*OptionsV1) LogValue() slog.Value        { return private{}.LogValue() }
func (*NativeOptionsV1) LogValue() slog.Value  { return private{}.LogValue() }
func (*RequestOptionsV1) LogValue() slog.Value { return private{}.LogValue() }
func (*Source) LogValue() slog.Value           { return private{}.LogValue() }
func (*Client) LogValue() slog.Value           { return private{}.LogValue() }
func (*Stream) LogValue() slog.Value           { return private{}.LogValue() }
func (*Metadata) LogValue() slog.Value         { return private{}.LogValue() }
func (*Result) LogValue() slog.Value           { return private{}.LogValue() }
