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
	"context"
	"database/sql/driver"
	"io"
	"strings"

	bindings "github.com/duckdb/duckdb-go-bindings"
	sdk "github.com/duckdb/duckdb-go/v2"
)

func prepare(ctx context.Context, conn *sdk.Conn, query string) (*sdk.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, failure(ErrNative, "prepare", err, context.Cause(ctx))
	}
	// This native API checks the parser's statement count without executing
	// preceding statements. PrepareContext has a different, impure contract.
	native, err := conn.Prepare(query)
	if err != nil {
		return nil, failure(ErrNative, "prepare", err)
	}
	return native.(*sdk.Stmt), nil
}

func permitted(statement *sdk.Stmt, request Request, transaction bool) error {
	kind, err := statement.StatementType()
	if err != nil {
		return failure(ErrNative, "statement-type", err)
	}
	switch kind {
	case sdk.STATEMENT_TYPE_SELECT:
		if request.Mode == Query {
			return nil
		}
	case sdk.STATEMENT_TYPE_INSERT, sdk.STATEMENT_TYPE_UPDATE, sdk.STATEMENT_TYPE_DELETE,
		sdk.StmtType(bindings.StatementTypeMergeInto):
		return nil
	case sdk.STATEMENT_TYPE_CREATE, sdk.STATEMENT_TYPE_ALTER, sdk.STATEMENT_TYPE_DROP,
		sdk.STATEMENT_TYPE_ANALYZE, sdk.STATEMENT_TYPE_VACUUM:
		if request.Mode == Execute {
			return nil
		}
	case sdk.STATEMENT_TYPE_CALL:
		if !transaction && request.Mode == Execute &&
			strings.EqualFold(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(request.SQL), ";")), "CHECKPOINT") {
			return nil
		}
	}
	return failure(ErrUnsupported, "statement-type")
}

func runSQL(ctx context.Context, conn *sdk.Conn, request Request, transaction bool, step *Step, remaining *int64, rowsLeft *int) (primary, cleanup error) {
	statement, err := prepare(ctx, conn, request.SQL)
	if err != nil {
		return err, nil
	}
	defer func() { cleanup = joined(ErrCleanup, "statement-close", cleanup, statement.Close()) }()
	step.Prepared = true
	if err := permitted(statement, request, transaction); err != nil {
		return err, nil
	}
	if request.Mode == Query {
		return queryRows(ctx, statement, request.Args, step, remaining, rowsLeft)
	}
	batches := request.Rows
	if request.Mode == Execute {
		batches = [][]any{request.Args}
	}
	for _, row := range batches {
		if err := ctx.Err(); err != nil {
			return failure(ErrNative, "execute", err, context.Cause(ctx)), nil
		}
		if len(row) != statement.NumInput() {
			return failure(ErrInput, "arguments"), nil
		}
		args, err := boundArguments(statement, row)
		if err != nil {
			return err, nil
		}
		step.Submitted = true
		result, err := statement.ExecContext(ctx, args)
		if err != nil {
			return failure(ErrNative, "execute", err), nil
		}
		step.Executions++
		changed, err := result.RowsAffected()
		if err != nil {
			return failure(ErrNative, "rows-changed", err), nil
		}
		step.RowsChanged += changed
		step.RowsChangedKnown = true
	}
	return nil, nil
}

func queryRows(ctx context.Context, statement *sdk.Stmt, args []any, step *Step, remaining *int64, rowsLeft *int) (primary, cleanup error) {
	if len(args) != statement.NumInput() {
		return failure(ErrInput, "arguments"), nil
	}
	if err := ctx.Err(); err != nil {
		return failure(ErrNative, "query", err, context.Cause(ctx)), nil
	}
	bound, err := boundArguments(statement, args)
	if err != nil {
		return err, nil
	}
	step.Submitted = true
	rows, err := statement.QueryContext(ctx, bound)
	if err != nil {
		return failure(ErrNative, "query", err), nil
	}
	defer func() { cleanup = joined(ErrCleanup, "rows-close", rows.Close()) }()
	step.Executions = 1
	names := rows.Columns()
	if len(names) > MaxColumns {
		return failure(ErrLimit, "columns"), nil
	}
	metadata := rows.(driver.RowsColumnTypeDatabaseTypeName)
	for index, name := range names {
		typeName := metadata.ColumnTypeDatabaseTypeName(index)
		if !queryType(typeName) {
			return failure(ErrUnsupported, "result-type"), nil
		}
		size := int64(len(name)+len(typeName)) + 64
		if size > *remaining {
			step.Limited = true
			return failure(ErrLimit, "result-bytes"), nil
		}
		*remaining -= size
		step.Columns = append(step.Columns, Column{Name: name, Type: typeName})
	}
	values := make([]driver.Value, len(names))
	for {
		if err := ctx.Err(); err != nil {
			return failure(ErrNative, "consume", err, context.Cause(ctx)), nil
		}
		err := rows.Next(values)
		if err == io.EOF {
			step.Complete = true
			return nil, nil
		}
		if err != nil {
			return failure(ErrNative, "consume", err), nil
		}
		if *rowsLeft == 0 {
			step.Limited = true
			return failure(ErrLimit, "result-rows"), nil
		}
		size := int64(32)
		for column, value := range values {
			cost, err := resultScalarSize(value, step.Columns[column].Type)
			if err != nil {
				return err, nil
			}
			size += cost
		}
		if size > *remaining {
			step.Limited = true
			return failure(ErrLimit, "result-bytes"), nil
		}
		*remaining -= size
		*rowsLeft--
		row := make([]any, len(values))
		for column, value := range values {
			row[column] = copyScalar(value)
		}
		step.Rows = append(step.Rows, row)
	}
}

// Restrict before rows.Next: the stable driver's composite decoder can panic,
// and JSON numeric decoding is not an exact-value transport.
func queryType(name string) bool {
	switch name {
	case "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
		"FLOAT", "DOUBLE", "VARCHAR", "BLOB", "UUID", "DATE", "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS",
		"TIMESTAMP_NS", "TIMESTAMPTZ", "INTERVAL", "HUGEINT", "UHUGEINT", "ENUM", "SQLNULL":
		return true
	}
	if strings.HasPrefix(name, "DECIMAL(") && strings.HasSuffix(name, ")") {
		// The name comes from native scalar metadata, never a SQL/input parser.
		return !strings.ContainsAny(name[len("DECIMAL("):len(name)-1], "()[] ")
	}
	return false
}
