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

package temporal

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies this SDK-major integration, not its configuration format.
const ProviderID = "orchestration.temporal.v1"

const (
	ErrInput     fault.Kind = "fathomry." + ProviderID + ".input"
	ErrAuthority fault.Kind = "fathomry." + ProviderID + ".authority"
	ErrConnect   fault.Kind = "fathomry." + ProviderID + ".connect"
	ErrRPC       fault.Kind = "fathomry." + ProviderID + ".rpc"
	ErrLimit     fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrCleanup   fault.Kind = "fathomry." + ProviderID + ".cleanup"
	ErrExecution fault.Kind = "fathomry." + ProviderID + ".execution"
	ErrWorker    fault.Kind = "fathomry." + ProviderID + ".worker"
	ErrTask      fault.Kind = "fathomry." + ProviderID + ".task"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "temporal[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("temporal[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("temporal: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("temporal: runtime reconstruction unsupported")
}

func (*OptionsV1) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }
func (*Source) LogValue() slog.Value    { return slog.StringValue("temporal[restricted]") }
func (*Client) LogValue() slog.Value    { return slog.StringValue("temporal[restricted]") }
