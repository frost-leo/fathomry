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

package logging_test

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/internal/conformance"
)

func limits() logging.Limits {
	return logging.Limits{MaxFields: 64, MaxNodes: 256, MaxDepth: 8, MaxBytes: 64 << 10}
}
func occurrence(t *testing.T, index int) *failure.Error {
	t.Helper()
	value, err := failure.New(logging.Definitions()[index], failure.Location{Operation: "test"}, errors.New("native-private-canary"))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestClosedValuesKindsCopiesAndBounds(t *testing.T) {
	bytes := []byte{0, 255}
	items := []logging.Value{logging.Uint64(math.MaxUint64), logging.Float32(float32(1.25))}
	fields := []logging.Field{{Key: "binary", Value: logging.Binary(bytes)}, {Key: "array", Value: logging.Array(items...)}, {Key: "null", Value: logging.Null()},
		{Key: "empty", Value: logging.Group()}, {Key: "time", Value: logging.Time(time.Unix(-1, 123).In(time.FixedZone("caller", 3600)))},
		{Key: "duration", Value: logging.Duration(-time.Second)}, {Key: "text", Value: logging.ByteString([]byte("text"))}}
	frozen, err := logging.FreezeFields(fields, limits())
	if err != nil {
		t.Fatal(err)
	}
	bytes[0] = 99
	items[0] = logging.Int64(0)
	fields[0].Key = "changed"
	if frozen[0].Key != "binary" || frozen[0].Value.BytesCopy()[0] != 0 || frozen[1].Value.ElementsCopy()[0].Uint64() != math.MaxUint64 ||
		frozen[1].Value.ElementsCopy()[1].Kind() != logging.Float32Kind || frozen[1].Value.ElementsCopy()[1].Float32() != 1.25 ||
		frozen[2].Value.Kind() != logging.NullKind || frozen[3].Value.Kind() != logging.GroupKind ||
		len(frozen[3].Value.FieldsCopy()) != 0 || !frozen[4].Value.Time().Equal(time.Unix(-1, 123)) ||
		frozen[5].Value.Duration() != -time.Second || frozen[6].Value.Kind() != logging.ByteStringKind {
		t.Fatal("closed data was coerced or aliased")
	}
	copy := frozen[0].Value.BytesCopy()
	copy[0] = 42
	array := frozen[1].Value.ElementsCopy()
	array[0] = logging.Null()
	if frozen[0].Value.BytesCopy()[0] != 0 || frozen[1].Value.ElementsCopy()[0].Uint64() != math.MaxUint64 {
		t.Fatal("accessors expose mutable storage")
	}
	if _, err := logging.MeasureFields(frozen, limits()); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []logging.Value{logging.Float64(math.NaN()), logging.Float64(math.Inf(1)), logging.Float32(float32(math.Inf(1))),
		logging.ByteString([]byte{0xff}), logging.String(string([]byte{0xff})), logging.Group(logging.Field{Key: "x", Value: logging.Null()}, logging.Field{Key: "x", Value: logging.Null()})} {
		if _, err := logging.Freeze(bad, limits()); err == nil {
			t.Fatal("invalid closed value accepted")
		}
	}
	cycle := make([]logging.Value, 1)
	cycle[0] = logging.Array(cycle...)
	if _, err := logging.Freeze(cycle[0], limits()); !errors.Is(err, logging.ErrLimit) {
		t.Fatal("cyclic value did not reject under bounds", err)
	}
	small := limits()
	small.MaxBytes = 32
	if _, err := logging.Freeze(logging.String("large"), small); !errors.Is(err, logging.ErrLimit) {
		t.Fatal("byte budget ignored", err)
	}
	for _, value := range []any{logging.Null(), frozen[0], logging.Attribution{}, logging.Output{}} {
		conformance.Runtime(t, value, reflect.New(reflect.TypeOf(value)).Interface(), "native-private-canary")
	}
}

type hostileError struct{ calls *int }

func (value *hostileError) Error() string        { *value.calls++; panic("formatter called") }
func (value *hostileError) LogValue() slog.Value { *value.calls++; panic("LogValue called") }
func (value *hostileError) Unwrap() error        { *value.calls++; panic("private original traversed") }

type unknownError struct{ calls *int }

func (value *unknownError) Error() string { *value.calls++; panic("Error called") }

type emptyWrapper struct{}

func (emptyWrapper) Error() string   { panic("Error called") }
func (emptyWrapper) Unwrap() []error { return nil }

type cycleWrapper struct{}

func (value *cycleWrapper) Error() string { panic("Error called") }
func (value *cycleWrapper) Unwrap() error { return value }

func TestSafeErrorNeverFormatsOrTraversesPrivateCauses(t *testing.T) {
	first, second := occurrence(t, 0), occurrence(t, 2)
	calls := 0
	private := &hostileError{calls: &calls}
	forwarded := errorbridge.Forward(private, errors.Join(first, second))
	value, err := logging.SafeError(forwarded)
	if err != nil || calls != 0 {
		t.Fatal("safe projection visited private original", err, calls)
	}
	list := value.FieldsCopy()
	if len(list) != 1 || list[0].Key != "errors" || len(list[0].Value.ElementsCopy()) != 2 {
		t.Fatal("aggregate acquired an arbitrary primary")
	}
	wrapped := fmt.Errorf("private-wrapper-canary: %w", first)
	single, err := logging.SafeError(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range single.FieldsCopy() {
		if strings.Contains(field.Value.StringValue(), "canary") {
			t.Fatal("native/wrapper text escaped")
		}
	}
	for _, bad := range []error{nil, &unknownError{calls: &calls}, emptyWrapper{}, &cycleWrapper{}, errors.Join(first, &unknownError{calls: &calls})} {
		if _, err := logging.SafeError(bad); err == nil {
			t.Fatal("unknown/empty graph silently projected")
		}
	}
	if calls != 0 {
		t.Fatal("untrusted formatter invoked", calls)
	}
}

func TestSafeErrorLocalesAndDetachedDefinitions(t *testing.T) {
	catalog, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "logging", BaseLocale: "en", Resources: logging.Resources(), Directory: "resources", Definitions: logging.Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err = presenter.WithLocale("zh-CN")
	if err != nil {
		t.Fatal(err)
	}
	translated := presenter.Present(occurrence(t, 0))
	value, err := logging.SafeError(translated)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, field := range value.FieldsCopy() {
		if field.Value.Kind() == logging.StringKind {
			seen[field.Key] = field.Value.StringValue()
		}
	}
	if seen["locale"] != "zh-CN" || seen["message"] == logging.Definitions()[0].Message || seen["requested_locale"] != "zh-CN" {
		t.Fatal("captured localization lost")
	}
	definitions := logging.Definitions()
	definitions[0].Message = "changed"
	if logging.Definitions()[0].Message == "changed" {
		t.Fatal("shared definitions were mutable")
	}
}

func FuzzClosedValueBounds(f *testing.F) {
	f.Add("key", []byte("value"), uint8(3))
	f.Fuzz(func(t *testing.T, key string, data []byte, depth uint8) {
		value := logging.Binary(data)
		for range int(depth % 16) {
			value = logging.Group(logging.Field{Key: key, Value: value})
		}
		frozen, err := logging.Freeze(value, limits())
		if err == nil {
			if _, err := logging.Freeze(frozen, limits()); err != nil {
				t.Fatal("frozen value changed validity")
			}
		}
	})
}
