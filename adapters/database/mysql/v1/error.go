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

package mysql

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/database/mysql/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/go-sql-driver/mysql"
)

// Stable identities describe technical failures, never retryability or effect state.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMySQL)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMySQL)<<16 | 0x0002
	ErrConnect       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMySQL)<<16 | 0x0003
	ErrQuery         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMySQL)<<16 | 0x0004
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMySQL)<<16 | 0x0005
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMySQL)<<16 | 0x0006
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMySQL)<<16 | 0x0007
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMySQL)<<16 | 0x0008
	ErrProtocol      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMySQL)<<16 | 0x0009
)

// Definitions returns detached, offline error declarations.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrConnect), definition(ErrQuery), definition(ErrLimit), definition(ErrState), definition(ErrCleanup), definition(ErrSerialization), definition(ErrProtocol)}
}

func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The database input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested database capability is unsupported."
	case ErrConnect:
		identifier, message = "connection_failed", "The database connection attempt failed."
	case ErrQuery:
		identifier, message = "operation_failed", "The database operation failed; inspect its effect evidence."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A database capability bound was exceeded."
	case ErrState:
		identifier, message = "invalid_state", "The database handle cannot perform this operation in its current state."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Database cleanup reported a failure; completion is separate."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Database runtime value serialization is unsupported."
	case ErrProtocol:
		identifier, message = "protocol_failed", "The MySQL protocol response violated the supported profile."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.database_mysql." + identifier), Module: "fathomry", Component: "database_mysql", Revision: 1, Message: message}
}

func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}

// InspectError finds the first native server error without erasing its identity.
// The returned error is borrowed and sensitive; do not mutate it or log it blindly.
// Absence proves neither no dispatch nor no remote effects.
// Translated errors keep their nearest native classification as the outer public
// occurrence, expose recognized nested identities through errors.Is, and retain
// the original native graph as the first cause. No identity is retry/effect policy.
func InspectError(err error) (*sdk.MySQLError, bool) {
	var value *sdk.MySQLError
	ok := errors.As(err, &value) && value != nil
	return value, ok
}

func translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(failure.Occurrence); ok {
		return err
	}
	code := ErrQuery
	if operation == "open" || operation == "validate" {
		code = ErrInput
	}
	if operation == "cleanup" || operation == "close" {
		code = ErrCleanup
	}
	classifications := nativeCodes(err)
	causes := []error{err}
	if len(classifications) != 0 {
		code = classifications[0]
		for _, nested := range classifications[1:] {
			causes = append(causes, nested)
		}
	}
	return fail(code, operation, causes...)
}

// Preserve outer native classification rather than selecting a deeper cause by
// category priority. Neutral invocation/resource wrappers retain their children.
func nativeCodes(err error) []failure.Code {
	codes := make([]failure.Code, 0, 9)
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
		if code := codeForKind(kind); code != 0 && !slices.Contains(codes, code) {
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
	return codes
}

func codeForKind(kind fault.Kind) failure.Code {
	switch kind {
	case native.ErrInput:
		return ErrInput
	case native.ErrUnsupported:
		return ErrUnsupported
	case native.ErrConnect:
		return ErrConnect
	case native.ErrQuery:
		return ErrQuery
	case native.ErrLimit, source.ErrCapacity:
		return ErrLimit
	case native.ErrState:
		return ErrState
	case native.ErrCleanup, invocation.ErrCleanup, source.ErrCleanup, source.ErrIncomplete:
		return ErrCleanup
	case native.ErrProtocol:
		return ErrProtocol
	}
	return 0
}

type private struct{}

func (private) String() string { return "mysql[restricted]" }

func (private) GoString() string { return "mysql[restricted]" }

func (private) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("mysql[restricted]")) }

func (private) LogValue() slog.Value { return slog.StringValue("mysql[restricted]") }

func (private) MarshalJSON() ([]byte, error) { return nil, fail(ErrSerialization, "marshal") }

func (*private) UnmarshalJSON([]byte) error { return fail(ErrSerialization, "unmarshal") }

func (Settings) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }

// LogValue redacts Settings values and non-nil pointers. Its value receiver is
// required for JSON-handler safety; normalize optional nil pointers to untyped
// nil before logging rather than invoking their generated value-method wrapper.
func (Settings) LogValue() slog.Value { return private{}.LogValue() }

func (*Owner) LogValue() slog.Value { return private{}.LogValue() }

func (*Handle) LogValue() slog.Value { return private{}.LogValue() }

func (*Client) LogValue() slog.Value { return private{}.LogValue() }

func (*Result) LogValue() slog.Value { return private{}.LogValue() }

func (*Row) LogValue() slog.Value { return private{}.LogValue() }

func (*Column) LogValue() slog.Value { return private{}.LogValue() }

func (*Transaction) LogValue() slog.Value { return private{}.LogValue() }

func (*Statement) LogValue() slog.Value { return private{}.LogValue() }

// outcomeError preserves independent phase facts while selecting primary failure
// for direct presentation. A sole occurrence keeps its identity and typed details.
func outcomeError(value adapters.Snapshot[Result]) error {
	return combineResultErrors(value.Info().Operation, value.Primary(), value.Cleanup())
}

func combineResultErrors(operation string, primary, secondary error) error {
	if primary == nil {
		return secondary
	}
	if secondary == nil {
		return primary
	}
	if core, ok := failure.Inspect(primary); ok {
		diagnostic := core.Diagnostic()
		location := diagnostic.Location
		location.Operation = operation
		if detailed, ok := primary.(*failure.Detailed[adapters.Details]); ok {
			if details, present := detailed.Details(); present {
				combined, err := failure.NewDetailed(diagnostic.Definition, location, details, func(value adapters.Details) adapters.Details { return value }, primary, secondary)
				if err == nil {
					return combined
				}
			}
		} else if diagnostic.Definition.Details.ID == "" {
			combined, err := failure.New(diagnostic.Definition, location, primary, secondary)
			if err == nil {
				return combined
			}
		}
	}
	return fail(ErrQuery, operation, primary, secondary)
}

//go:embed resources/*.json
var resources embed.FS

// Resources returns component-owned English and Chinese message bundles.
func Resources() fs.FS { return resources }
