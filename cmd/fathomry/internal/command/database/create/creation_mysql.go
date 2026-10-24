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
	"strings"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
)

func (creationState *creationState) createMySQL(ctx context.Context, result *DatabaseChange) error {
	if err := creationState.admit(ctx); err != nil {
		return err
	}
	session, err := creationState.myAdmin.Retain(ctx, mysql.SessionOptions{Timeout: creationState.options.Timeout})
	creationState.session = session
	if err != nil {
		return fail(databasecmd.ErrDatabase, err)
	}
	if session == nil {
		return fail(databasecmd.ErrDatabase)
	}
	if err := creationState.admit(ctx); err != nil {
		return err
	}
	profile, err := session.Query(ctx, "SELECT VERSION(),@@innodb_page_size,@@lower_case_table_names,@@autocommit,@@read_only,@@super_read_only,DATABASE()")
	if err != nil || !profile.Complete() {
		return fail(databasecmd.ErrDatabase, err)
	}
	rows, err := myRows(profile)
	if err != nil {
		return err
	}
	if len(rows) != 1 || len(rows[0]) != 7 || !strings.HasPrefix(rows[0][0], "8.4.") || rows[0][1] != "16384" || rows[0][2] != "0" || rows[0][3] != "1" || rows[0][4] != "0" || rows[0][5] != "0" || rows[0][6] != "" {
		return fail(databasecmd.ErrProfile)
	}
	if err := creationState.admit(ctx); err != nil {
		return err
	}
	result.Stage, result.Effect = "database", DatabaseUnknown
	created, err := session.Exec(ctx, creationState.sql.database)
	if created.Complete() {
		result.Effect = DatabaseCreated
	} else if native, present := mysql.InspectError(err); present && native.Number == 1007 {
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
	verified, err := creationState.myTarget.Query(ctx, `SELECT DATABASE(),VERSION(),@@innodb_page_size,@@lower_case_table_names,@@read_only,@@super_read_only,DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME
FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=DATABASE()`)
	if err != nil || !verified.Complete() {
		return fail(databasecmd.ErrDatabase, err)
	}
	rows, err = myRows(verified)
	if err != nil {
		return err
	}
	if len(rows) != 1 || len(rows[0]) != 8 || rows[0][0] != creationState.options.Database || !strings.HasPrefix(rows[0][1], "8.4.") || rows[0][2] != "16384" || rows[0][3] != "0" || rows[0][4] != "0" || rows[0][5] != "0" || rows[0][6] != "utf8mb4" || rows[0][7] != "utf8mb4_0900_bin" {
		return fail(databasecmd.ErrProfile)
	}
	result.Stage, result.Ready = "ready", true
	return nil
}
func myRows(value mysql.Result) ([][]string, error) {
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
