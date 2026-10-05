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

package minio

import (
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
	source "github.com/frost-leo/fathomry/internal/resource"
)

// Stable codes classify failures, never retryability, rollback or effect state.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0002
	ErrAuthority     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0003
	ErrConnect       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0004
	ErrRead          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0005
	ErrWrite         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0006
	ErrList          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0007
	ErrRemove        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0008
	ErrMissing       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0009
	ErrDenied        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000a
	ErrExpired       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000b
	ErrCondition     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000c
	ErrIntegrity     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000d
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000e
	ErrProtocol      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000f
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0010
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0011
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0012
)

// Definitions returns detached offline declarations for explicit composition.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrAuthority), definition(ErrConnect), definition(ErrRead), definition(ErrWrite), definition(ErrList), definition(ErrRemove), definition(ErrMissing), definition(ErrDenied), definition(ErrExpired), definition(ErrCondition), definition(ErrIntegrity), definition(ErrLimit), definition(ErrProtocol), definition(ErrCleanup), definition(ErrState), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The object-storage input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested object-storage capability is unsupported."
	case ErrAuthority:
		identifier, message = "authority_refused", "The selected source does not grant this object-storage operation."
	case ErrConnect:
		identifier, message = "connection_failed", "Object-storage construction or readiness failed."
	case ErrRead:
		identifier, message = "read_failed", "The object read failed; inspect partial data and cleanup evidence."
	case ErrWrite:
		identifier, message = "write_failed", "The object write failed; its remote effects may be unknown."
	case ErrList:
		identifier, message = "enumeration_failed", "Object enumeration failed or is incomplete."
	case ErrRemove:
		identifier, message = "removal_failed", "Object removal failed; inspect each target's evidence."
	case ErrMissing:
		identifier, message = "object_missing", "The service reported a missing object, version, bucket or upload."
	case ErrDenied:
		identifier, message = "access_denied", "The service denied object-storage access."
	case ErrExpired:
		identifier, message = "credentials_expired", "The service reported expired credentials."
	case ErrCondition:
		identifier, message = "condition_failed", "An object-storage condition was not satisfied."
	case ErrIntegrity:
		identifier, message = "integrity_failed", "Object integrity verification failed."
	case ErrLimit:
		identifier, message = "limit_exceeded", "An object-storage capability bound was exceeded."
	case ErrProtocol:
		identifier, message = "protocol_invalid", "The object-storage response did not satisfy the selected protocol."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Object-storage cleanup failed; local release and remote effects are separate."
	case ErrState:
		identifier, message = "invalid_state", "The object-storage handle cannot perform this operation in its current state."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Object-storage runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.objectstore_minio." + identifier), Module: "fathomry", Component: "objectstore_minio", Revision: 1, Message: message}
}
func fail(code failure.Code, operation string, causes ...error) error {
	def := definition(code)
	if code.Facility() == failure.FacilityOperation {
		for _, candidate := range adapters.Definitions() {
			if candidate.Code == code {
				def = candidate
				break
			}
		}
	}
	value, err := failure.New(def, failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}
func translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(failure.Occurrence); ok {
		return err
	}
	codes := []failure.Code{}
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
		case native.ErrRead:
			code = ErrRead
		case native.ErrWrite:
			code = ErrWrite
		case native.ErrList:
			code = ErrList
		case native.ErrRemove:
			code = ErrRemove
		case native.ErrMissing:
			code = ErrMissing
		case native.ErrDenied:
			code = ErrDenied
		case native.ErrExpired:
			code = ErrExpired
		case native.ErrCondition:
			code = ErrCondition
		case native.ErrIntegrity:
			code = ErrIntegrity
		case native.ErrLimit:
			code = ErrLimit
		case native.ErrProtocol:
			code = ErrProtocol
		case native.ErrCleanup:
			code = ErrCleanup
		case native.ErrState:
			code = ErrState
		case invocation.ErrEvidence:
			code = adapters.ErrEvidence
		case source.ErrCapacity, invocation.ErrAttempts:
			code = adapters.ErrLimit
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
	code := adapters.ErrOperation
	if operation == "validate" || operation == "open" {
		code = ErrInput
	}
	if operation == "close" || operation == "cleanup" {
		code = ErrCleanup
	}
	causes := []error{err}
	if len(codes) > 0 {
		code = codes[0]
		for _, nested := range codes[1:] {
			causes = append(causes, nested)
		}
	}
	return fail(code, operation, causes...)
}

type private struct{}

func (private) String() string                     { return "minio[restricted]" }
func (private) GoString() string                   { return "minio[restricted]" }
func (private) Format(state fmt.State, _ rune)     { _, _ = state.Write([]byte("minio[restricted]")) }
func (private) LogValue() slog.Value               { return slog.StringValue("minio[restricted]") }
func (private) MarshalJSON() ([]byte, error)       { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error        { return fail(ErrSerialization, "unmarshal") }
func (Settings) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (Settings) String() string                    { return private{}.String() }
func (Settings) GoString() string                  { return private{}.String() }
func (Settings) LogValue() slog.Value              { return private{}.LogValue() }

//go:embed resources/*.json
var resources embed.FS

// Resources supplies immutable English and zh-CN messages without service I/O.
func Resources() fs.FS                     { return resources }
func (*Owner) LogValue() slog.Value        { return private{}.LogValue() }
func (*Handle) LogValue() slog.Value       { return private{}.LogValue() }
func (*Client) LogValue() slog.Value       { return private{}.LogValue() }
func (*Result) LogValue() slog.Value       { return private{}.LogValue() }
func (*Address) LogValue() slog.Value      { return private{}.LogValue() }
func (*Object) LogValue() slog.Value       { return private{}.LogValue() }
func (*Transfer) LogValue() slog.Value     { return private{}.LogValue() }
func (*Removal) LogValue() slog.Value      { return private{}.LogValue() }
func (*Upload) LogValue() slog.Value       { return private{}.LogValue() }
func (*Part) LogValue() slog.Value         { return private{}.LogValue() }
func (*Delegation) LogValue() slog.Value   { return private{}.LogValue() }
func (*ReadRequest) LogValue() slog.Value  { return private{}.LogValue() }
func (*WriteRequest) LogValue() slog.Value { return private{}.LogValue() }
func (*CopyRequest) LogValue() slog.Value  { return private{}.LogValue() }
func (*ListRequest) LogValue() slog.Value  { return private{}.LogValue() }
func (*UploadQuery) LogValue() slog.Value  { return private{}.LogValue() }
func (*SignRequest) LogValue() slog.Value  { return private{}.LogValue() }
func (*Multipart) LogValue() slog.Value    { return private{}.LogValue() }
func (*Cursor) LogValue() slog.Value       { return private{}.LogValue() }
