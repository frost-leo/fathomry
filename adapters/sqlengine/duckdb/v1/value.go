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
	"math/big"
	"slices"
	"time"

	sdk "github.com/duckdb/duckdb-go/v2"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

// Mode selects a DuckDB operation, not a universal SQL abstraction.
type Mode uint8

const (
	Execute Mode = iota + 1
	Query
	ExecuteMany
	Append
)

// Request is borrowed until the synchronous call returns. Execute/Query use
// SQL/Args; ExecuteMany uses SQL/Rows; Append uses Schema/Table/Columns/Rows.
// No input may be mutated concurrently. Unsupported fields are refused.
type Request struct {
	private
	Mode          Mode
	SQL           string
	Args          []any
	Rows          [][]any
	Schema, Table string
	Columns       []string
}

// Decimal is an exact unscaled integer with decimal width and scale.
// Value is caller-owned; concurrent mutation during input use is forbidden.
type Decimal struct {
	Width, Scale uint8
	Value        *big.Int
}

// UUID preserves the sixteen native UUID bytes.
type UUID [16]byte

// Interval preserves native calendar months, days and microseconds separately.
type Interval struct {
	Days, Months int32
	Micros       int64
}

func inward(inputs []Request, config native.OptionsV1) ([]native.Request, error) {
	if len(inputs) < 1 || len(inputs) > MaxSteps {
		return nil, fail(ErrLimit, "steps")
	}
	remaining := config.InputBytes
	rowsLeft := config.MaxBatchRows
	output := make([]native.Request, len(inputs))
	row := func(input []any) ([]any, error) {
		if len(input) > MaxColumns {
			return nil, fail(ErrLimit, "columns")
		}
		remaining -= 32
		if remaining < 0 {
			return nil, fail(ErrLimit, "input-bytes")
		}
		if int64(len(input))*48 > remaining {
			return nil, fail(ErrLimit, "input-bytes")
		}
		if input == nil {
			return nil, nil
		}
		values := make([]any, len(input))
		for index, value := range input {
			var size int64 = 48
			switch value := value.(type) {
			case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
				values[index] = value
			case time.Time:
				values[index] = value
				size = 64
			case string:
				size += int64(len(value))
				values[index] = value
			case []byte:
				size += int64(len(value))
				if size <= remaining {
					values[index] = slices.Clone(value)
				}
			case *big.Int:
				if value == nil || value.BitLen() > 128 {
					return nil, fail(ErrInput, "integer")
				}
				size = 96
				values[index] = new(big.Int).Set(value)
			case Decimal:
				if !validDecimal(value.Width, value.Scale, value.Value) {
					return nil, fail(ErrInput, "decimal")
				}
				size = 128
				values[index] = sdk.Decimal{Width: value.Width, Scale: value.Scale, Value: new(big.Int).Set(value.Value)}
			case UUID:
				values[index] = sdk.UUID(value)
			case Interval:
				values[index] = sdk.Interval{Days: value.Days, Months: value.Months, Micros: value.Micros}
			default:
				return nil, fail(ErrUnsupported, "input-type")
			}
			if size > remaining {
				return nil, fail(ErrLimit, "input-bytes")
			}
			remaining -= size
		}
		return values, nil
	}
	for index, input := range inputs {
		if len(input.SQL) > MaxSQLBytes || len(input.Schema) > 256 || len(input.Table) > 256 || len(input.Columns) > MaxColumns {
			return nil, fail(ErrLimit, "request")
		}
		remaining -= int64(len(input.SQL)+len(input.Schema)+len(input.Table)) + 256
		for _, column := range input.Columns {
			if len(column) > 256 {
				return nil, fail(ErrLimit, "column")
			}
			remaining -= int64(len(column)) + 32
		}
		if remaining < 0 || len(input.Rows) > rowsLeft {
			return nil, fail(ErrLimit, "input")
		}
		rowsLeft -= len(input.Rows)
		var args []any
		var err error
		switch input.Mode {
		case Execute, Query:
			if len(input.Rows) != 0 {
				return nil, fail(ErrInput, "arguments")
			}
			args, err = row(input.Args)
			if err != nil {
				return nil, err
			}
		case ExecuteMany, Append:
			if len(input.Args) != 0 {
				return nil, fail(ErrInput, "arguments")
			}
		default:
			return nil, fail(ErrInput, "mode")
		}
		var rows [][]any
		if int64(len(input.Rows))*32 > remaining {
			return nil, fail(ErrLimit, "input-bytes")
		}
		if input.Rows != nil {
			rows = make([][]any, len(input.Rows))
		}
		for rowIndex, inputRow := range input.Rows {
			rows[rowIndex], err = row(inputRow)
			if err != nil {
				return nil, err
			}
		}
		output[index] = native.Request{Mode: native.Mode(input.Mode), SQL: input.SQL, Args: args, Rows: rows,
			Schema: input.Schema, Table: input.Table, Columns: slices.Clone(input.Columns)}
	}
	return output, nil
}

func outward(value any, columnType string) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch value := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string, time.Time:
		return value, nil
	case []byte:
		if columnType == "UUID" {
			if len(value) != 16 {
				return nil, fail(ErrUnsupported, "uuid-result")
			}
			return UUID(value), nil
		}
		return slices.Clone(value), nil
	case *big.Int:
		if value == nil {
			return nil, fail(ErrUnsupported, "integer-result")
		}
		return new(big.Int).Set(value), nil
	case sdk.Decimal:
		if !validDecimal(value.Width, value.Scale, value.Value) {
			return nil, fail(ErrUnsupported, "decimal-result")
		}
		return Decimal{Width: value.Width, Scale: value.Scale, Value: new(big.Int).Set(value.Value)}, nil
	case sdk.UUID:
		return UUID(value), nil
	case sdk.Interval:
		return Interval{Days: value.Days, Months: value.Months, Micros: value.Micros}, nil
	default:
		return nil, fail(ErrUnsupported, "result-type")
	}
}

func copyScalar(value any) any {
	switch value := value.(type) {
	case []byte:
		return slices.Clone(value)
	case *big.Int:
		return new(big.Int).Set(value)
	case Decimal:
		value.Value = new(big.Int).Set(value.Value)
		return value
	default:
		return value
	}
}

func validDecimal(width, scale uint8, value *big.Int) bool {
	if value == nil || width < 1 || width > 38 || scale > width || value.BitLen() > 127 {
		return false
	}
	bound := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(width)), nil)
	return new(big.Int).Abs(value).Cmp(bound) < 0
}
