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

package create

import (
	"context"
	"strconv"
	"strings"

	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
)

func (creationState *creationState) createPostgres(ctx context.Context, result *DatabaseChange) error {
	if err := creationState.admit(ctx); err != nil {
		return err
	}
	statement, err := creationState.pgAdmin.Prepare(ctx, creationState.sql.database)
	creationState.statement = statement
	if err != nil {
		return fail(databasecmd.ErrDatabase, err)
	}
	if statement == nil || !statement.Ready().Complete() || !strings.HasPrefix(statement.Ready().ServerVersion(), "18.") {
		return fail(databasecmd.ErrProfile)
	}
	if err := creationState.admit(ctx); err != nil {
		return err
	}
	result.Stage, result.Effect = "database", DatabaseUnknown
	created, err := statement.Exec(ctx)
	if created.Complete() && created.CommandTag() == "CREATE DATABASE" {
		result.Effect = DatabaseCreated
	} else if native, present := postgres.InspectError(err); present && databaseExists(native.Code, native.SchemaName, native.TableName, native.ConstraintName) {
		result.Stage, result.Effect = "existing", DatabaseUnchanged
		return fail(databasecmd.ErrDatabaseExists, err)
	}
	if err != nil || result.Effect != DatabaseCreated {
		return fail(databasecmd.ErrDatabase, err)
	}
	if err := creationState.finish(ctx); err != nil {
		return err
	}
	result.Stage = "target"
	if err := creationState.admit(ctx); err != nil {
		return err
	}
	creationState.transaction, err = creationState.pgTarget.Begin(ctx, postgres.TxOptions{Isolation: postgres.ReadCommitted, Access: postgres.ReadWrite})
	if err != nil {
		return fail(databasecmd.ErrDatabase, err)
	}
	if creationState.transaction == nil {
		return fail(databasecmd.ErrDatabase)
	}
	profile, err := creationState.pgRows(ctx, `SELECT current_database(),current_user,current_setting('server_encoding'),current_setting('server_version_num'),pg_is_in_recovery()::text,pg_get_userbyid(datdba),datlocprovider::text,datlocale
FROM pg_catalog.pg_database WHERE datname=current_database()`)
	if err != nil {
		return err
	}
	if len(profile) != 1 || len(profile[0]) != 8 {
		return fail(databasecmd.ErrProfile)
	}
	values := profile[0]
	version, parseError := strconv.Atoi(values[3])
	if parseError != nil || version < 180000 || version >= 190000 || values[0] != creationState.options.Database || values[1] != creationState.options.Owner || values[2] != "UTF8" || values[4] != "false" || values[5] != creationState.options.Owner || values[6] != "b" || values[7] != "C.UTF-8" {
		return fail(databasecmd.ErrProfile)
	}
	result.Stage = "schema"
	existing, err := creationState.pgRows(ctx, "SELECT pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname=$1", creationState.options.Schema)
	if err != nil {
		return err
	}
	if creationState.options.Schema == "public" {
		if len(existing) != 1 || len(existing[0]) != 1 || existing[0][0] != "pg_database_owner" && existing[0][0] != creationState.options.Owner {
			return fail(databasecmd.ErrProfile)
		}
		result.SchemaEffect = DatabaseUnchanged
	} else {
		if len(existing) != 0 {
			return fail(databasecmd.ErrProfile)
		}
		if err := creationState.admit(ctx); err != nil {
			return err
		}
		result.SchemaEffect = DatabaseUnknown
		value, err := creationState.transaction.Exec(ctx, creationState.sql.schema)
		if err != nil || !value.Complete() || value.CommandTag() != "CREATE SCHEMA" {
			return fail(databasecmd.ErrDatabase, err)
		}
		owners, err := creationState.pgRows(ctx, "SELECT pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname=$1", creationState.options.Schema)
		if err != nil {
			return err
		}
		if len(owners) != 1 || len(owners[0]) != 1 || owners[0][0] != creationState.options.Owner {
			return fail(databasecmd.ErrProfile)
		}
	}
	if err := creationState.admit(ctx); err != nil {
		return err
	}
	creationState.commitSent = true
	value, err := creationState.transaction.Commit(ctx)
	creationState.outcome = value.TransactionOutcome()
	if value.TransactionOutcome() == postgres.CommitAcknowledged {
		if creationState.options.Schema != "public" {
			result.SchemaEffect = DatabaseCreated
		}
		result.Stage, result.Ready = "ready", true
	} else if value.TransactionOutcome() == postgres.RollbackAcknowledged || value.TransactionOutcome() == postgres.CommitRolledBack {
		if result.SchemaEffect == DatabaseUnknown {
			result.SchemaEffect = DatabaseUnchanged
		}
	}
	if err != nil || !result.Ready {
		return fail(databasecmd.ErrDatabase, err)
	}
	return nil
}

func databaseExists(code, schema, table, constraint string) bool {
	// Concurrent CREATE DATABASE can reach the catalog's unique index instead
	// of the earlier duplicate-name check. No other uniqueness error is adoption.
	return code == "42P04" || code == "23505" && schema == "pg_catalog" && table == "pg_database" && constraint == "pg_database_datname_index"
}
func (creationState *creationState) pgRows(ctx context.Context, sql string, args ...any) ([][]string, error) {
	if err := creationState.admit(ctx); err != nil {
		return nil, err
	}
	value, err := creationState.transaction.Query(ctx, sql, args...)
	if err != nil || !value.Complete() {
		return nil, fail(databasecmd.ErrDatabase, err)
	}
	rows := value.RowsCopy()
	if len(rows) > 16 {
		return nil, fail(databasecmd.ErrLimit)
	}
	result := make([][]string, 0, len(rows))
	total := 0
	for _, row := range rows {
		cells := row.ValuesCopy()
		record := make([]string, len(cells))
		for index, cell := range cells {
			total += len(cell)
			if total > 64<<10 {
				return nil, fail(databasecmd.ErrLimit)
			}
			record[index] = string(cell)
		}
		result = append(result, record)
	}
	return result, nil
}
