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

package duckdb

import (
	"database/sql/driver"
	"math"
	"math/big"
	"reflect"
	"slices"
	"time"
	"unicode/utf8"

	bindings "github.com/duckdb/duckdb-go-bindings"
	sdk "github.com/duckdb/duckdb-go/v2"
)

func boundArguments(statement *sdk.Stmt, values []any) ([]driver.NamedValue, error) {
	converted := make([]driver.NamedValue, len(values))
	for index, value := range values {
		switch scalar := value.(type) {
		case uint:
			value = uint64(scalar)
		case sdk.UUID:
			value = scalar.String()
		case sdk.Decimal:
			value = scalar.String()
		case time.Time:
			kind, err := statement.ParamType(index + 1)
			if err != nil {
				return nil, failure(ErrNative, "parameter-type", err)
			}
			if kind == sdk.TYPE_INVALID {
				kind = sdk.TYPE_TIMESTAMP_TZ
			}
			info, err := sdk.NewTypeInfo(kind)
			if err != nil {
				return nil, failure(ErrUnsupported, "parameter-type", err)
			}
			value, err = appendScalar(scalar, info)
			if err != nil {
				return nil, err
			}
		}
		converted[index] = driver.NamedValue{Ordinal: index + 1, Value: value}
	}
	return converted, nil
}

func finiteTimestampNS(value time.Time) bool {
	nanos := value.UnixNano()
	return time.Unix(0, nanos).Equal(value) && bindings.IsFiniteTimestampNS(bindings.NewTimestampNS(nanos))
}

func resultScalarSize(value any, typeName string) (int64, error) {
	if instant, ok := value.(time.Time); ok && typeName == "TIMESTAMP_NS" && !finiteTimestampNS(instant) {
		return 0, failure(ErrUnsupported, "timestamp-sentinel")
	}
	return scalarSize(value)
}

func scalarSize(value any) (int64, error) {
	switch value := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, sdk.UUID, sdk.Interval:
		return 48, nil
	case time.Time:
		if value.UTC().Year() >= 1 && value.UTC().Year() <= 9999 {
			return 64, nil
		}
		return 0, failure(ErrUnsupported, "time-range")
	case string:
		if !utf8.ValidString(value) {
			return 0, failure(ErrInput, "utf8")
		}
		return int64(len(value)) + 48, nil
	case []byte:
		return int64(len(value)) + 48, nil
	case *big.Int:
		if value != nil && value.BitLen() <= 128 {
			return 96, nil
		}
		return 0, failure(ErrInput, "integer-range")
	case sdk.Decimal:
		if value.Value != nil && value.Width >= 1 && value.Width <= 38 && value.Scale <= value.Width &&
			value.Value.BitLen() <= 127 {
			return 128, nil
		}
		return 0, failure(ErrInput, "decimal")
	default:
		return 0, failure(ErrUnsupported, "value-type")
	}
}

func copyScalar(value any) any {
	switch value := value.(type) {
	case []byte:
		return slices.Clone(value)
	case *big.Int:
		return new(big.Int).Set(value)
	case sdk.Decimal:
		value.Value = new(big.Int).Set(value.Value)
		return value
	default:
		return value
	}
}

func signed(value any, bits int) (int64, bool) {
	native := reflect.ValueOf(value)
	var integer int64
	switch native.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		integer = native.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if native.Uint() > math.MaxInt64 {
			return 0, false
		}
		integer = int64(native.Uint())
	default:
		return 0, false
	}
	return integer, bits == 64 || integer >= -(int64(1)<<(bits-1)) && integer < int64(1)<<(bits-1)
}

func unsigned(value any, bits int) (uint64, bool) {
	native := reflect.ValueOf(value)
	var integer uint64
	switch native.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if native.Int() < 0 {
			return 0, false
		}
		integer = uint64(native.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		integer = native.Uint()
	default:
		return 0, false
	}
	return integer, bits == 64 || integer < uint64(1)<<bits
}

// The native Appender's numeric setters use Go casts (including narrowing).
// Validate target type, range and precision before those setters see the value.
func appendScalar(value any, info sdk.TypeInfo) (any, error) {
	if value == nil {
		return nil, nil
	}
	kind := info.InternalType()
	switch kind {
	case sdk.TYPE_TINYINT, sdk.TYPE_SMALLINT, sdk.TYPE_INTEGER, sdk.TYPE_BIGINT:
		bits := map[sdk.Type]int{sdk.TYPE_TINYINT: 8, sdk.TYPE_SMALLINT: 16, sdk.TYPE_INTEGER: 32, sdk.TYPE_BIGINT: 64}[kind]
		if number, ok := signed(value, bits); ok {
			return number, nil
		}
	case sdk.TYPE_UTINYINT, sdk.TYPE_USMALLINT, sdk.TYPE_UINTEGER, sdk.TYPE_UBIGINT:
		bits := map[sdk.Type]int{sdk.TYPE_UTINYINT: 8, sdk.TYPE_USMALLINT: 16, sdk.TYPE_UINTEGER: 32, sdk.TYPE_UBIGINT: 64}[kind]
		if number, ok := unsigned(value, bits); ok {
			return number, nil
		}
	case sdk.TYPE_BOOLEAN:
		if value, ok := value.(bool); ok {
			return value, nil
		}
	case sdk.TYPE_FLOAT:
		if value, ok := value.(float32); ok {
			return value, nil
		}
	case sdk.TYPE_DOUBLE:
		switch value := value.(type) {
		case float32:
			return float64(value), nil
		case float64:
			return value, nil
		}
	case sdk.TYPE_VARCHAR, sdk.TYPE_ENUM:
		if value, ok := value.(string); ok {
			return value, nil
		}
	case sdk.TYPE_BLOB:
		if value, ok := value.([]byte); ok {
			return value, nil
		}
	case sdk.TYPE_UUID:
		if value, ok := value.(sdk.UUID); ok {
			return value, nil
		}
		if value, ok := value.([]byte); ok && len(value) == 16 {
			return value, nil
		}
	case sdk.TYPE_HUGEINT, sdk.TYPE_UHUGEINT:
		if value, ok := value.(*big.Int); ok && value != nil {
			valid := value.Sign() >= 0 && value.BitLen() <= 128
			if kind == sdk.TYPE_HUGEINT {
				lower := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
				upper := new(big.Int).Neg(lower)
				valid = value.Cmp(lower) >= 0 && value.Cmp(upper) < 0
			}
			if valid {
				return value, nil
			}
		}
	case sdk.TYPE_DECIMAL:
		if value, ok := value.(sdk.Decimal); ok && value.Value != nil {
			details := info.Details().(*sdk.DecimalDetails)
			bound := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(details.Width)), nil)
			if value.Width == details.Width && value.Scale == details.Scale && new(big.Int).Abs(value.Value).Cmp(bound) < 0 {
				return value, nil
			}
		}
	case sdk.TYPE_INTERVAL:
		if value, ok := value.(sdk.Interval); ok {
			return value, nil
		}
	case sdk.TYPE_DATE, sdk.TYPE_TIME, sdk.TYPE_TIMESTAMP, sdk.TYPE_TIMESTAMP_S, sdk.TYPE_TIMESTAMP_MS, sdk.TYPE_TIMESTAMP_NS, sdk.TYPE_TIMESTAMP_TZ:
		if value, ok := value.(time.Time); ok && value.UTC().Year() >= 1 && value.UTC().Year() <= 9999 {
			value = value.UTC()
			precision := 1000
			switch kind {
			case sdk.TYPE_DATE:
				if value.Hour() != 0 || value.Minute() != 0 || value.Second() != 0 || value.Nanosecond() != 0 {
					break
				}
				return value, nil
			case sdk.TYPE_TIME:
				if value.Year() != 1 || value.Month() != time.January || value.Day() != 1 {
					break
				}
				if value.Nanosecond()%1000 == 0 {
					return value, nil
				}
			case sdk.TYPE_TIMESTAMP_NS:
				if value.Year() >= 1678 && finiteTimestampNS(value) {
					return value, nil
				}
			default:
				if kind == sdk.TYPE_TIMESTAMP_S {
					precision = 1e9
				}
				if kind == sdk.TYPE_TIMESTAMP_MS {
					precision = 1e6
				}
				if value.Nanosecond()%precision == 0 {
					return value, nil
				}
			}
		}
	}
	return nil, failure(ErrUnsupported, "append-value")
}
