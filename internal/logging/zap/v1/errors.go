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

package zap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies the SDK integration, not a source or wire version.
const ProviderID = "logging.zap.v1"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrState       fault.Kind = "fathomry." + ProviderID + ".state"
	ErrWrite       fault.Kind = "fathomry." + ProviderID + ".write"
	ErrSync        fault.Kind = "fathomry." + ProviderID + ".sync"
	ErrFile        fault.Kind = "fathomry." + ProviderID + ".file"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
	ErrRecursion   fault.Kind = "fathomry." + ProviderID + ".recursion"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

func failureOrNil(kind fault.Kind, operation string, causes ...error) error {
	for _, cause := range causes {
		if cause != nil {
			return failure(kind, operation, causes...)
		}
	}
	return nil
}

func nativeFailure(kind fault.Kind, operation string, ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return failure(kind, operation, err, context.Cause(ctx))
	}
	return failure(kind, operation, err)
}

type private struct{}

func (private) String() string   { return "zap[restricted]" }
func (private) GoString() string { return "zap[restricted]" }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("zap: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("zap: runtime reconstruction unsupported")
}

func restricted(state fmt.State)                  { _, _ = io.WriteString(state, "zap[restricted]") }
func (OptionsV1) Format(state fmt.State, _ rune)  { restricted(state) }
func (OutputV1) Format(state fmt.State, _ rune)   { restricted(state) }
func (Source) Format(state fmt.State, _ rune)     { restricted(state) }
func (Logger) Format(state fmt.State, _ rune)     { restricted(state) }
func (Result) Format(state fmt.State, _ rune)     { restricted(state) }
func (SinkResult) Format(state fmt.State, _ rune) { restricted(state) }
func (*OptionsV1) LogValue() slog.Value           { return slog.StringValue("zap[restricted]") }
func (*OutputV1) LogValue() slog.Value            { return slog.StringValue("zap[restricted]") }
func (*Source) LogValue() slog.Value              { return slog.StringValue("zap[restricted]") }
func (*Logger) LogValue() slog.Value              { return slog.StringValue("zap[restricted]") }
func (*Result) LogValue() slog.Value              { return slog.StringValue("zap[restricted]") }
func (*SinkResult) LogValue() slog.Value          { return slog.StringValue("zap[restricted]") }
