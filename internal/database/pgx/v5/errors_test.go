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

package pgx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestNativeErrorCausesWithoutInventedCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("cancel-cause-canary")
	cancel(cause)
	native := &pgconn.PgError{Code: "23505", Message: "private-native-canary", Detail: "private-detail-canary"}
	observed := nativeFailure(ErrQuery, "query", ctx, native)
	if !errors.Is(observed, native) || errors.Is(observed, cause) || errors.Is(observed, context.Canceled) {
		t.Fatal("coincident cancellation replaced or relabeled a native server error")
	}
	observed = nativeFailure(ErrQuery, "query", ctx, context.Canceled)
	if !errors.Is(observed, cause) || !errors.Is(observed, context.Canceled) {
		t.Fatal("actual native cancellation lost its original cause")
	}
	if nativeFailure(ErrQuery, "query", ctx, nil) != nil {
		t.Fatal("completed native work acquired a later cancellation error")
	}
	conformance.Private(t, observed, "cancel-cause-canary", "private-native-canary", "private-detail-canary")
}
func TestNilAndValueDiagnosticsRemainRestricted(t *testing.T) {
	for _, value := range []any{(*OptionsV1)(nil), (*Source)(nil), (*Database)(nil), (*Transaction)(nil), (*Result)(nil), (*Row)(nil), (*Column)(nil)} {
		// Ordinary fmt has a defined nil-pointer fallback; direct invocation of
		// a value-receiver Formatter on nil is not this runtime contract.
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%d", "%f", "%x", "%O"} {
			if fmt.Sprintf(format, value) != "<nil>" {
				t.Fatal("nil fmt projection changed")
			}
		}
		for _, jsonOutput := range []bool{false, true} {
			var output bytes.Buffer
			var handler slog.Handler = slog.NewTextHandler(&output, nil)
			if jsonOutput {
				handler = slog.NewJSONHandler(&output, nil)
			}
			slog.New(handler).Info("fixture", "value", value)
			if !strings.Contains(output.String(), "pgx[restricted]") || strings.Contains(output.String(), "panic") {
				t.Fatal("nil slog projection changed")
			}
		}
	}
	for _, test := range []struct{ value, target any }{
		{Result{data: &resultData{command: "secret-canary", rows: []Row{{cells: []cell{{text: "secret-canary"}}}}}}, new(Result)},
		{Column{Name: "secret-canary"}, new(Column)},
		{Row{cells: []cell{{text: "secret-canary"}}}, new(Row)},
		{Source{}, new(Source)}, {Database{}, new(Database)}, {Transaction{}, new(Transaction)},
		{OptionsV1{Password: "secret-canary"}, new(OptionsV1)},
	} {
		conformance.Runtime(t, test.value, test.target, "secret-canary")
		pointer := reflect.New(reflect.TypeOf(test.value))
		pointer.Elem().Set(reflect.ValueOf(test.value))
		conformance.Runtime(t, pointer.Interface(), reflect.New(pointer.Elem().Type()).Interface(), "secret-canary")
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%f", "%c", "%O"} {
			for _, value := range []any{test.value, pointer.Interface(), []any{test.value}, struct{ Value any }{test.value}} {
				if strings.Contains(fmt.Sprintf(format, value), "secret-canary") {
					t.Fatal("value formatting disclosed private data")
				}
			}
		}
	}
}
