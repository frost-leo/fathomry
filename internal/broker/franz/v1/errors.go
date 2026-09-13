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

package franz

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

const ProviderID = "broker.franz.v1"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrAuthority   fault.Kind = "fathomry." + ProviderID + ".authority"
	ErrConnect     fault.Kind = "fathomry." + ProviderID + ".connect"
	ErrIdentity    fault.Kind = "fathomry." + ProviderID + ".identity"
	ErrProduce     fault.Kind = "fathomry." + ProviderID + ".produce"
	ErrRead        fault.Kind = "fathomry." + ProviderID + ".read"
	ErrMissing     fault.Kind = "fathomry." + ProviderID + ".missing"
	ErrUnavailable fault.Kind = "fathomry." + ProviderID + ".unavailable"
	ErrExpired     fault.Kind = "fathomry." + ProviderID + ".expired"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrState       fault.Kind = "fathomry." + ProviderID + ".state"
	ErrTransaction fault.Kind = "fathomry." + ProviderID + ".transaction"
	ErrOffsets     fault.Kind = "fathomry." + ProviderID + ".offsets"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}
func nativeFailure(kind fault.Kind, operation string, ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	return failure(kind, operation, err, ctx.Err(), context.Cause(ctx))
}

// Runtime data is deliberately not a durable record/reference protocol.
type private struct{}

func (private) String() string                 { return "kafka[restricted]" }
func (private) GoString() string               { return "kafka[restricted]" }
func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "kafka[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("kafka[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("kafka: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("kafka: runtime reconstruction unsupported")
}

// Pointer receivers keep typed nil runtime values safe for slog handlers.
func (*OptionsV1) LogValue() slog.Value        { return slog.StringValue("kafka[restricted]") }
func (*Source) LogValue() slog.Value           { return slog.StringValue("kafka[restricted]") }
func (*Client) LogValue() slog.Value           { return slog.StringValue("kafka[restricted]") }
func (*Message) LogValue() slog.Value          { return slog.StringValue("kafka[restricted]") }
func (*Header) LogValue() slog.Value           { return slog.StringValue("kafka[restricted]") }
func (*Position) LogValue() slog.Value         { return slog.StringValue("kafka[restricted]") }
func (*Record) LogValue() slog.Value           { return slog.StringValue("kafka[restricted]") }
func (*Write) LogValue() slog.Value            { return slog.StringValue("kafka[restricted]") }
func (*Read) LogValue() slog.Value             { return slog.StringValue("kafka[restricted]") }
func (*Range) LogValue() slog.Value            { return slog.StringValue("kafka[restricted]") }
func (*Page) LogValue() slog.Value             { return slog.StringValue("kafka[restricted]") }
func (*Result) LogValue() slog.Value           { return slog.StringValue("kafka[restricted]") }
func (*Topic) LogValue() slog.Value            { return slog.StringValue("kafka[restricted]") }
func (*Checkpoint) LogValue() slog.Value       { return slog.StringValue("kafka[restricted]") }
func (*CheckpointResult) LogValue() slog.Value { return slog.StringValue("kafka[restricted]") }
func (*Consumer) LogValue() slog.Value         { return slog.StringValue("kafka[restricted]") }
func (*ConsumerProgress) LogValue() slog.Value { return slog.StringValue("kafka[restricted]") }
