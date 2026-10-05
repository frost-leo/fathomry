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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
	source "github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/minio/minio-go/v7"
)

func TestPublicFailureIdentityAndMessages(t *testing.T) {
	remote := &sdk.ErrorResponse{Code: "AccessDenied", Message: "private-error-canary"}
	original := native.ErrWrite.New(fault.Context{}, native.ErrDenied.New(fault.Context{}, remote))
	translated := translate(invocation.ErrFailed.New(fault.Context{}, original), "put")
	occurrence, ok := failure.Inspect(translated)
	if !ok || occurrence.Diagnostic().Definition.Code != ErrWrite || !errors.Is(translated, ErrDenied) || !errors.Is(translated, original) {
		t.Fatal("classification lost")
	}
	var observed *sdk.ErrorResponse
	if !errors.As(translated, &observed) || observed != remote {
		t.Fatal("original cause lost")
	}
	if strings.Contains(fmt.Sprintf("%+v", translated), "canary") {
		t.Fatal("error disclosure")
	}
	capacity := translate(source.ErrCapacity, "admission")
	if !errors.Is(capacity, adapters.ErrLimit) {
		t.Fatal("shared limit hidden")
	}
	if _, err := failure.Prepare(append(adapters.Definitions(), Definitions()...)...); err != nil {
		t.Fatal(err)
	}
	catalog, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "objectstore_minio", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()})
	if err != nil || catalog == nil {
		t.Fatal("message catalog", err)
	}
	allocations := failure.Allocations()
	found := false
	for _, allocation := range allocations {
		if allocation.Facility == failure.FacilityMinIO {
			found = allocation.Module == "fathomry" && allocation.Component == "objectstore_minio"
		}
	}
	if !found || ErrInput.Domain() != failure.DomainObjectStorage {
		t.Fatal("facility collision")
	}
}

func TestPublicRuntimeDiagnostics(t *testing.T) {
	sensitive := []any{Settings{SecretKey: "private-canary"}, Address{Key: "private-canary"}, WriteRequest{Key: "private-canary", Metadata: map[string]string{"k": "private-canary"}},
		Result{data: &resultData{content: []byte("private-canary")}}, Delegation{url: "private-canary"}, Object{Address: Address{Key: "private-canary"}}, Removal{Err: errors.New("private-canary")}}
	for _, value := range sensitive {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
			if strings.Contains(fmt.Sprintf(format, value), "private-canary") {
				t.Fatal("format disclosure")
			}
		}
		if strings.Contains(slog.AnyValue(value).Resolve().String(), "private-canary") {
			t.Fatal("log disclosure")
		}
	}
	for _, value := range []any{new(Owner), new(Handle), new(Client), new(Result), new(Delegation), new(Multipart), new(Cursor), new(Object), new(Part)} {
		if _, err := json.Marshal(value); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime serialized")
		}
		if err := json.Unmarshal([]byte("{}"), value); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime reconstructed")
		}
	}
	for _, value := range []any{(*Owner)(nil), (*Client)(nil), (*Result)(nil), (*Delegation)(nil), (*Multipart)(nil), (*Cursor)(nil)} {
		if slog.AnyValue(value).Resolve().String() != "minio[restricted]" {
			t.Fatal("nil log panic")
		}
	}
}
