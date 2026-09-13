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

package otel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies this implementation, independently of options and OTLP.
const ProviderID = "telemetry.otel.v1"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrEnvironment fault.Kind = "fathomry." + ProviderID + ".environment"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrState       fault.Kind = "fathomry." + ProviderID + ".state"
	ErrExport      fault.Kind = "fathomry." + ProviderID + ".export"
	ErrPartial     fault.Kind = "fathomry." + ProviderID + ".partial"
	ErrProtocol    fault.Kind = "fathomry." + ProviderID + ".protocol"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
	ErrUndelivered fault.Kind = "fathomry." + ProviderID + ".undelivered"
	ErrRecursion   fault.Kind = "fathomry." + ProviderID + ".recursion"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}
func joined(kind fault.Kind, operation string, causes ...error) error {
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
	if ctx != nil && ctx.Err() != nil && (errors.Is(err, ctx.Err()) || errors.Is(err, context.Cause(ctx))) {
		return failure(kind, operation, err, ctx.Err(), context.Cause(ctx))
	}
	return failure(kind, operation, err)
}

// A malformed TLS first record carries a fallback connection in its native
// error. This client already closes it; retaining it would expose and retain
// the network owner through a diagnostic cause. Other native fields survive.
func detachedTLSFailure(err error) error {
	if header, ok := err.(tls.RecordHeaderError); ok {
		header.Conn = nil
		return header
	}
	return err
}

type private struct{}

func (private) String() string                 { return "otel[restricted]" }
func (private) GoString() string               { return "otel[restricted]" }
func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "otel[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("otel[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("otel: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("otel: runtime reconstruction unsupported")
}
