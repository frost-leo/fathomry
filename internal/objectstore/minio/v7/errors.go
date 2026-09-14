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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
	native "github.com/minio/minio-go/v7"
)

const ProviderID = "objectstore.minio.v7"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrAuthority   fault.Kind = "fathomry." + ProviderID + ".authority"
	ErrConnect     fault.Kind = "fathomry." + ProviderID + ".connect"
	ErrRead        fault.Kind = "fathomry." + ProviderID + ".read"
	ErrWrite       fault.Kind = "fathomry." + ProviderID + ".write"
	ErrList        fault.Kind = "fathomry." + ProviderID + ".list"
	ErrRemove      fault.Kind = "fathomry." + ProviderID + ".remove"
	ErrMissing     fault.Kind = "fathomry." + ProviderID + ".missing"
	ErrDenied      fault.Kind = "fathomry." + ProviderID + ".denied"
	ErrExpired     fault.Kind = "fathomry." + ProviderID + ".expired"
	ErrCondition   fault.Kind = "fathomry." + ProviderID + ".condition"
	ErrIntegrity   fault.Kind = "fathomry." + ProviderID + ".integrity"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrProtocol    fault.Kind = "fathomry." + ProviderID + ".protocol"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}
func nativeFailure(kind fault.Kind, operation string, ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	causes := []error{err, ctx.Err(), context.Cause(ctx)}
	var response native.ErrorResponse
	var pointer *native.ErrorResponse
	if errors.As(err, &response) || errors.As(err, &pointer) && pointer != nil {
		if pointer != nil {
			response = *pointer
		}
		switch response.Code {
		case "NoSuchKey", "NoSuchVersion", "NoSuchBucket", "NoSuchUpload":
			causes = append(causes, ErrMissing)
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
			causes = append(causes, ErrDenied)
		case "ExpiredToken":
			causes = append(causes, ErrExpired)
		case "PreconditionFailed", "ConditionalRequestConflict":
			causes = append(causes, ErrCondition)
		case "BadDigest", "ChecksumMismatch":
			causes = append(causes, ErrIntegrity)
		}
	}
	return failure(kind, operation, causes...)
}

type private struct{}

func (private) String() string                 { return "minio[restricted]" }
func (private) GoString() string               { return "minio[restricted]" }
func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "minio[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("minio[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("minio: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("minio: runtime reconstruction unsupported")
}

func (*OptionsV1) LogValue() slog.Value    { return slog.StringValue("minio[restricted]") }
func (*Source) LogValue() slog.Value       { return slog.StringValue("minio[restricted]") }
func (*Client) LogValue() slog.Value       { return slog.StringValue("minio[restricted]") }
func (*Address) LogValue() slog.Value      { return slog.StringValue("minio[restricted]") }
func (*Object) LogValue() slog.Value       { return slog.StringValue("minio[restricted]") }
func (*Result) LogValue() slog.Value       { return slog.StringValue("minio[restricted]") }
func (*Transfer) LogValue() slog.Value     { return slog.StringValue("minio[restricted]") }
func (*Removal) LogValue() slog.Value      { return slog.StringValue("minio[restricted]") }
func (*Upload) LogValue() slog.Value       { return slog.StringValue("minio[restricted]") }
func (*ReadRequest) LogValue() slog.Value  { return slog.StringValue("minio[restricted]") }
func (*WriteRequest) LogValue() slog.Value { return slog.StringValue("minio[restricted]") }
func (*CopyRequest) LogValue() slog.Value  { return slog.StringValue("minio[restricted]") }
func (*ListRequest) LogValue() slog.Value  { return slog.StringValue("minio[restricted]") }
func (*UploadQuery) LogValue() slog.Value  { return slog.StringValue("minio[restricted]") }
