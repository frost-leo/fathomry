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

package trino

import (
	"math"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	native "github.com/frost-leo/fathomry/internal/sqlengine/trino/v0"
	sdk "github.com/trinodb/trino-go-client/trino"
)

// Statement borrows one SQL string and positional values until return. Read
// freezes admitted values before asynchronous use. Arbitrary Valuer, callbacks,
// maps, pointers, floats and native option/credential handles are refused.
type Statement struct {
	private
	SQL  string
	Args []any
}

// BatchInsert describes one physical VALUES statement. Identifiers are quoted
// by the native builder; an empty or ragged batch is rejected, never split.
type BatchInsert struct {
	private
	Table   string
	Columns []string
	Rows    [][]any
}

// Numeric preserves exact decimal/exponent text without a floating conversion.
type Numeric string

var numberPattern = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func inwardStatement(input Statement, config native.OptionsV1) (native.Statement, error) {
	if len(input.SQL) == 0 || len(input.SQL) > config.MaxSQLBytes || len(input.Args) > config.MaxParameters {
		return native.Statement{}, fail(ErrInput, "statement")
	}
	remaining := int64(config.MaxSQLBytes) - int64(len(input.SQL))
	if len(input.Args) > 0 {
		remaining -= int64(len(input.SQL)) + 32 + int64(len(input.Args))*2
	}
	if remaining < 0 || int64(len(input.Args)) > remaining {
		return native.Statement{}, fail(ErrLimit, "arguments")
	}
	var args []any
	if input.Args != nil {
		args = make([]any, len(input.Args))
	}
	for index, value := range input.Args {
		converted, err := inwardValue(value, 0, &remaining)
		if err != nil {
			return native.Statement{}, err
		}
		args[index] = converted
	}
	return native.Statement{SQL: strings.Clone(input.SQL), Args: args}, nil
}

func inwardBatch(input BatchInsert, config native.OptionsV1) (native.BatchInsert, error) {
	if len(input.Table) > 128 || len(input.Columns) < 1 || len(input.Columns) > config.MaxColumns || len(input.Rows) < 1 ||
		len(input.Rows) > config.MaxRows || len(input.Rows) > config.MaxParameters/len(input.Columns) {
		return native.BatchInsert{}, fail(ErrInput, "batch")
	}
	for _, column := range input.Columns {
		if len(column) > 128 {
			return native.BatchInsert{}, fail(ErrInput, "column")
		}
	}
	// This is a preallocation lower bound, not a replacement for the native SQL
	// builder's exact identifier/escaping/serialized statement budget.
	remaining := int64(config.MaxSQLBytes)
	minimum := int64(len(input.Rows)) * int64(2*len(input.Columns)+2)
	if minimum > remaining {
		return native.BatchInsert{}, fail(ErrLimit, "batch")
	}
	for _, row := range input.Rows {
		if len(row) != len(input.Columns) {
			return native.BatchInsert{}, fail(ErrInput, "batch-arity")
		}
	}
	rows := make([][]any, len(input.Rows))
	for index, row := range input.Rows {
		rows[index] = make([]any, len(row))
		for column, value := range row {
			converted, err := inwardValue(value, 0, &remaining)
			if err != nil {
				return native.BatchInsert{}, err
			}
			rows[index][column] = converted
		}
	}
	return native.BatchInsert{Table: strings.Clone(input.Table), Columns: slices.Clone(input.Columns), Rows: rows}, nil
}

func inwardValue(input any, depth int, remaining *int64) (any, error) {
	if depth > 8 || *remaining < 0 {
		return nil, fail(ErrLimit, "arguments")
	}
	size := int64(0)
	var output any
	switch value := input.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint16, uint32:
		size = 32
		output = value
	case uint:
		if uint64(value) > math.MaxInt64 {
			return nil, fail(ErrInput, "integer")
		}
		size = 32
		output = value
	case uint64:
		if value > math.MaxInt64 {
			return nil, fail(ErrInput, "integer")
		}
		size = 32
		output = value
	case string:
		if !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return nil, fail(ErrInput, "string")
		}
		size = 2*int64(len(value)) + 2
		output = value
	case []byte:
		size = 2*int64(len(value)) + 3
		if size <= *remaining {
			output = slices.Clone(value)
		}
	case Numeric:
		if len(value) > 128 || !numberPattern.MatchString(string(value)) {
			return nil, fail(ErrInput, "numeric")
		}
		size = int64(len(value))
		output = sdk.Numeric(value)
	case time.Time:
		_, offset := value.Zone()
		if value.Year() < 1 || value.Year() > 9999 || offset%60 != 0 || offset < -14*3600 || offset > 14*3600 {
			return nil, fail(ErrInput, "timestamp")
		}
		size = 80
		output = value
	default:
		slice := reflect.ValueOf(input)
		if !slice.IsValid() || slice.Kind() != reflect.Slice || slice.IsNil() || int64(slice.Len()) > *remaining/2 {
			return nil, fail(ErrUnsupported, "argument")
		}
		*remaining -= 8 + int64(slice.Len())*2
		if *remaining < 0 {
			return nil, fail(ErrLimit, "arguments")
		}
		values := make([]any, slice.Len())
		for index := range values {
			value, err := inwardValue(slice.Index(index).Interface(), depth+1, remaining)
			if err != nil {
				return nil, err
			}
			values[index] = value
		}
		return values, nil
	}
	if size > *remaining {
		return nil, fail(ErrLimit, "arguments")
	}
	*remaining -= size
	return output, nil
}
