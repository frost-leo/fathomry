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

package kafka

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"

	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Stable codes identify technical meaning, never retry or effect policy.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0002
	ErrAuthority     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0003
	ErrConnect       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0004
	ErrIdentity      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0005
	ErrProduce       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0006
	ErrRead          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0007
	ErrMissing       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0008
	ErrUnavailable   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0009
	ErrExpired       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000a
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000b
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000c
	ErrTransaction   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000d
	ErrOffsets       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000e
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000f
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0010
)

// Definitions returns detached offline declarations; it never opens a client.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrAuthority), definition(ErrConnect), definition(ErrIdentity), definition(ErrProduce), definition(ErrRead), definition(ErrMissing), definition(ErrUnavailable), definition(ErrExpired), definition(ErrLimit), definition(ErrState), definition(ErrTransaction), definition(ErrOffsets), definition(ErrCleanup), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The Kafka input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested Kafka profile is unsupported."
	case ErrAuthority:
		identifier, message = "authority_refused", "The Kafka endpoint is outside the permitted authority."
	case ErrConnect:
		identifier, message = "connection_failed", "The Kafka connection or readiness check failed."
	case ErrIdentity:
		identifier, message = "identity_mismatch", "The Kafka cluster, topic or assignment identity does not match."
	case ErrProduce:
		identifier, message = "production_failed", "Kafka production failed; inspect per-record effect evidence."
	case ErrRead:
		identifier, message = "read_failed", "The Kafka read failed; partial observations may remain."
	case ErrMissing:
		identifier, message = "record_missing", "The exact Kafka offset has no visible record."
	case ErrUnavailable:
		identifier, message = "record_unavailable", "Kafka data or assignment is not currently available."
	case ErrExpired:
		identifier, message = "offset_expired", "The requested Kafka offset is outside the observed log range."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A Kafka capability bound was exceeded."
	case ErrState:
		identifier, message = "invalid_state", "The Kafka owner or assignment cannot perform this operation."
	case ErrTransaction:
		identifier, message = "transaction_failed", "The Kafka transaction failed; its final outcome is separate."
	case ErrOffsets:
		identifier, message = "checkpoint_failed", "The Kafka checkpoint operation failed; inspect commit evidence."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Kafka cleanup reported a failure; completion is separate."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Kafka runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.broker_kafka." + identifier),
		Module: "fathomry", Component: "broker_kafka", Revision: 1, Message: message}
}
func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}

// InspectError deliberately exposes a borrowed native broker cause. Its text may
// contain sensitive data; do not log it blindly. Absence proves no effect state.
func InspectError(err error) (*kerr.Error, bool) {
	var value *kerr.Error
	ok := errors.As(err, &value)
	return value, ok && value != nil
}
func translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(failure.Occurrence); ok {
		return err
	}
	codes := make([]failure.Code, 0, 16)
	remaining := 128
	var visit func(error)
	visit = func(err error) {
		if err == nil || remaining == 0 {
			return
		}
		remaining--
		var kind fault.Kind
		switch value := err.(type) {
		case *fault.Error:
			kind = value.Diagnostic().Kind
		case fault.Kind:
			kind = value
		}
		var code failure.Code
		switch kind {
		case native.ErrInput:
			code = ErrInput
		case native.ErrUnsupported:
			code = ErrUnsupported
		case native.ErrAuthority:
			code = ErrAuthority
		case native.ErrConnect:
			code = ErrConnect
		case native.ErrIdentity:
			code = ErrIdentity
		case native.ErrProduce:
			code = ErrProduce
		case native.ErrRead:
			code = ErrRead
		case native.ErrMissing:
			code = ErrMissing
		case native.ErrUnavailable:
			code = ErrUnavailable
		case native.ErrExpired:
			code = ErrExpired
		case native.ErrLimit:
			code = ErrLimit
		case native.ErrState:
			code = ErrState
		case native.ErrTransaction:
			code = ErrTransaction
		case native.ErrOffsets:
			code = ErrOffsets
		case native.ErrCleanup:
			code = ErrCleanup
		case source.ErrCapacity, invocation.ErrEvidence:
			code = ErrLimit
		case source.ErrConfiguration:
			code = ErrInput
		case source.ErrIncomplete, source.ErrCleanup, invocation.ErrCleanup:
			code = ErrCleanup
		}
		if code != 0 && !slices.Contains(codes, code) {
			codes = append(codes, code)
		}
		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			for _, cause := range value.Unwrap() {
				if remaining == 0 {
					break
				}
				visit(cause)
			}
		case interface{ Unwrap() error }:
			visit(value.Unwrap())
		}
	}
	visit(err)
	code := ErrState
	if len(codes) > 0 {
		code = codes[0]
	}
	causes := []error{err}
	if len(codes) > 1 {
		for _, nested := range codes[1:] {
			causes = append(causes, nested)
		}
	}
	return fail(code, operation, causes...)
}

type private struct{}

func (private) String() string                     { return "kafka[restricted]" }
func (private) GoString() string                   { return "kafka[restricted]" }
func (private) Format(state fmt.State, _ rune)     { _, _ = state.Write([]byte("kafka[restricted]")) }
func (private) LogValue() slog.Value               { return slog.StringValue("kafka[restricted]") }
func (private) MarshalJSON() ([]byte, error)       { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error        { return fail(ErrSerialization, "unmarshal") }
func (Settings) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (Settings) LogValue() slog.Value              { return private{}.LogValue() }
func (*Owner) LogValue() slog.Value                { return private{}.LogValue() }
func (*Handle) LogValue() slog.Value               { return private{}.LogValue() }
func (*Client) LogValue() slog.Value               { return private{}.LogValue() }
func (*Result) LogValue() slog.Value               { return private{}.LogValue() }
func (*TransactionIDs) LogValue() slog.Value       { return private{}.LogValue() }
func (*Header) LogValue() slog.Value               { return private{}.LogValue() }
func (*Message) LogValue() slog.Value              { return private{}.LogValue() }
func (*Position) LogValue() slog.Value             { return private{}.LogValue() }
func (*Range) LogValue() slog.Value                { return private{}.LogValue() }
func (*Record) LogValue() slog.Value               { return private{}.LogValue() }
func (*Page) LogValue() slog.Value                 { return private{}.LogValue() }
func (*Write) LogValue() slog.Value                { return private{}.LogValue() }
func (*Read) LogValue() slog.Value                 { return private{}.LogValue() }
func (*Topic) LogValue() slog.Value                { return private{}.LogValue() }
func (*Checkpoint) LogValue() slog.Value           { return private{}.LogValue() }
func (*CheckpointResult) LogValue() slog.Value     { return private{}.LogValue() }
func (*ConsumerProgress) LogValue() slog.Value     { return private{}.LogValue() }
func (*Assignment) LogValue() slog.Value           { return private{}.LogValue() }
func (*GroupSnapshot) LogValue() slog.Value        { return private{}.LogValue() }
func (*GroupBatch) LogValue() slog.Value           { return private{}.LogValue() }
func (*Consumer) LogValue() slog.Value             { return private{}.LogValue() }
func (*Group) LogValue() slog.Value                { return private{}.LogValue() }

//go:embed resources/*.json
var resources embed.FS

// Resources supplies offline English and zh-CN explanations.
func Resources() fs.FS { return resources }
