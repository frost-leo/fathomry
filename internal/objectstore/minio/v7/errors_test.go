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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	native "github.com/minio/minio-go/v7"
)

func TestErrorsAndRuntimePrivacy(t *testing.T) {
	cause := errors.New("private-canary")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	err := nativeFailure(ErrWrite, "put", ctx, native.ErrorResponse{Code: "AccessDenied", Message: "private-canary", BucketName: "private-canary", Key: "private-canary"})
	for _, want := range []error{ErrWrite, ErrDenied, context.Canceled, cause} {
		if !errors.Is(err, want) {
			t.Fatal("cause lost")
		}
	}
	conformance.Cause[native.ErrorResponse](t, err, func(value native.ErrorResponse) bool { return value.Code == "AccessDenied" })
	expired := nativeFailure(ErrRead, "get", context.Background(), native.ErrorResponse{Code: "ExpiredToken"})
	if !errors.Is(expired, ErrExpired) || errors.Is(expired, ErrMissing) || errors.Is(expired, ErrDenied) {
		t.Fatal("expired credentials conflated with missing or denied data")
	}
	conformance.Runtime(t, OptionsV1{AccessKey: "private-canary", SecretKey: "private-canary"}, new(OptionsV1), "private-canary")
	values := []any{err, Address{Key: "private-canary"}, WriteRequest{Metadata: map[string]string{"private-canary": "private-canary"}},
		Object{metadata: map[string]string{"k": "private-canary"}}, Transfer{UploadID: "private-canary"}, Removal{Err: cause},
		Result{data: &resultData{content: []byte("private-canary")}}}
	for _, value := range values {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%d"} {
			if strings.Contains(fmt.Sprintf(format, value), "private-canary") {
				t.Fatal("runtime presentation leaked")
			}
		}
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	logger.Info("runtime", "client", (*Client)(nil), "options", (*OptionsV1)(nil), "result", (*Result)(nil))
	if strings.Contains(output.String(), "panic") || !strings.Contains(output.String(), "minio[restricted]") {
		t.Fatal("typed nil logging unsafe")
	}
}
