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
	"strings"

	sdk "github.com/duckdb/duckdb-go/v2"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func identifier(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }

func tableTypes(ctx context.Context, conn *sdk.Conn, request Request) (types []sdk.TypeInfo, primary, cleanup error) {
	table := identifier(request.Table)
	if request.Schema != "" {
		table = identifier(request.Schema) + "." + table
	}
	columns := "*"
	if len(request.Columns) != 0 {
		names := make([]string, len(request.Columns))
		for index, name := range request.Columns {
			names[index] = identifier(name)
		}
		columns = strings.Join(names, ",")
	}
	statement, err := prepare(ctx, conn, "SELECT "+columns+" FROM "+table+" LIMIT 0")
	if err != nil {
		return nil, err, nil
	}
	defer func() { cleanup = joined(ErrCleanup, "schema-statement-close", cleanup, statement.Close()) }()
	rows, err := statement.QueryContext(ctx, nil)
	if err != nil {
		return nil, failure(ErrNative, "schema", err), nil
	}
	count := len(rows.Columns())
	metadata := rows.(driver.RowsColumnTypeDatabaseTypeName)
	if count == 0 || count > MaxColumns {
		return nil, failure(ErrLimit, "columns"), joined(ErrCleanup, "schema-rows-close", rows.Close())
	}
	for column := range count {
		typeName := metadata.ColumnTypeDatabaseTypeName(column)
		// TIME schema/appends are safe; its 24:00 result decoder is not exact.
		if typeName != "TIME" && !queryType(typeName) {
			return nil, failure(ErrUnsupported, "append-type"), joined(ErrCleanup, "schema-rows-close", rows.Close())
		}
	}
	cleanup = joined(ErrCleanup, "schema-rows-close", rows.Close())
	if cleanup != nil {
		return nil, nil, cleanup
	}
	for column := range count {
		info, err := statement.ColumnTypeInfo(column)
		if err != nil {
			return nil, failure(ErrNative, "schema", err), nil
		}
		types = append(types, info)
	}
	return types, nil, nil
}

func appendRows(ctx, cleanupCtx context.Context, config settings, conn *sdk.Conn, request Request, step *Step) (primary, cleanup error) {
	types, primary, cleanup := tableTypes(ctx, conn, request)
	if primary != nil || cleanup != nil {
		return primary, cleanup
	}
	for _, row := range request.Rows {
		if err := ctx.Err(); err != nil {
			return failure(ErrNative, "append-validate", err, context.Cause(ctx)), nil
		}
		if len(row) != len(types) {
			return failure(ErrInput, "append-columns"), nil
		}
		for column, value := range row {
			if _, err := appendScalar(value, types[column]); err != nil {
				return err, nil
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return failure(ErrNative, "append-create", err, context.Cause(ctx)), nil
	}
	var appender *sdk.Appender
	var err error
	if len(request.Columns) == 0 {
		appender, err = sdk.NewAppenderFromConn(conn, request.Schema, request.Table)
	} else {
		appender, err = sdk.NewAppenderWithColumns(conn, "", request.Schema, request.Table, request.Columns)
	}
	if err != nil {
		return failure(ErrNative, "append-create", err), nil
	}
	values := make([]driver.Value, len(types))
	for _, row := range request.Rows {
		if err := ctx.Err(); err != nil {
			primary = failure(ErrNative, "append", err, context.Cause(ctx))
			break
		}
		for column, value := range row {
			values[column], err = appendScalar(value, types[column])
			if err != nil {
				primary = err
				break
			}
		}
		if primary != nil {
			break
		}
		step.Submitted = true
		if err := appender.AppendRow(values...); err != nil {
			primary = failure(ErrNative, "append", err)
			break
		}
		step.AcceptedRows++
	}
	if primary == nil && ctx.Err() != nil {
		primary = failure(ErrNative, "append", ctx.Err(), context.Cause(ctx))
	}
	if primary == nil {
		step.Submitted = true
		step.FlushAttempted = true
		if err := appender.FlushWithCancel(ctx); err != nil {
			primary = failure(ErrNative, "flush", err)
		} else {
			step.Flushed = true
			step.FlushedRows = step.AcceptedRows
		}
	}
	if primary == nil && ctx.Err() != nil {
		primary = failure(ErrNative, "flush", ctx.Err(), context.Cause(ctx))
	}
	clean, cancel, err := (invocation.Budget{Limit: config.CleanupTimeout}).Context(cleanupCtx, invocation.Cleanup)
	if err != nil {
		cleanup = failure(ErrCleanup, "appender-budget", err)
		clean = cleanupCtx
	} else {
		defer cancel()
	}
	if primary != nil || cleanup != nil {
		cleanup = joined(ErrCleanup, "appender-clear", cleanup, appender.Clear())
	}
	// Close always flushes and destroys. Clear is not rollback, and a canceled
	// CloseWithCancel is not discard; the surrounding transaction owns effects.
	err = appender.CloseWithCancel(clean)
	step.AppenderClosed = true
	step.AppenderCloseSucceeded = err == nil
	cleanup = joined(ErrCleanup, "appender-close", cleanup, err)
	return primary, cleanup
}
