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

package schema

import (
	"strings"

	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	mysqlassets "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	pgassets "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
)

func (state *runnerState) objects(session *connection) ([]object, error) {
	var result []object
	var err error
	if state.dialect == "postgresql" {
		result, err = state.postgresObjects(session)
	} else {
		result, err = state.mysqlObjects(session)
	}
	if err != nil {
		return nil, err
	}
	size := 0
	if len(result) > maxObjects {
		return nil, fail(databasecmd.ErrLimit)
	}
	for _, value := range result {
		size += len(value.Kind) + len(value.Owner) + len(value.Name)
		for _, text := range value.Attributes {
			size += len(text)
		}
		if size > maxStructureBytes {
			return nil, fail(databasecmd.ErrLimit)
		}
	}
	sortObjects(result)
	return result, nil
}
func (state *runnerState) addObjects(session *connection, result *[]object, kind, sql string, args ...any) error {
	rows, err := session.query(sql, args...)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if len(row) < 2 || row[0] == nil || row[1] == nil {
			return fail(databasecmd.ErrDatabase)
		}
		values := texts(row)
		if state.dialect == "postgresql" && (kind == "constraint" || kind == "index" || kind == "trigger" || kind == "function") {
			last := len(values) - 1
			values[last] = pgassets.NormalizeInspection(values[last], state.target)
		}
		if err := appendObject(result, object{Kind: kind, Owner: values[0], Name: values[1], Attributes: values[2:]}); err != nil {
			return err
		}
	}
	return nil
}

func appendObject(result *[]object, value object) error {
	if len(*result) == maxObjects {
		return fail(databasecmd.ErrLimit)
	}
	size := func(value object) int {
		bytes := len(value.Kind) + len(value.Owner) + len(value.Name)
		for _, attribute := range value.Attributes {
			bytes += len(attribute)
		}
		return bytes
	}
	remaining := maxStructureBytes - size(value)
	for _, present := range *result {
		remaining -= size(present)
	}
	if remaining < 0 {
		return fail(databasecmd.ErrLimit)
	}
	*result = append(*result, value)
	return nil
}

func (state *runnerState) postgresObjects(session *connection) ([]object, error) {
	queries, err := pgassets.InspectionQueries()
	if err != nil {
		return nil, fail(databasecmd.ErrAssets, err)
	}
	result := []object{}
	for _, query := range queries {
		if err := state.addObjects(session, &result, query.Kind, query.SQL, state.target); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (state *runnerState) mysqlObjects(session *connection) ([]object, error) {
	queries, err := mysqlassets.InspectionQueries()
	if err != nil {
		return nil, fail(databasecmd.ErrAssets, err)
	}
	result := []object{}
	for _, query := range queries {
		if err := state.addObjects(session, &result, query.Kind, query.SQL, state.target); err != nil {
			return nil, err
		}
	}
	for _, entry := range append([]object(nil), result...) {
		if entry.Kind != "relation" || len(entry.Attributes) != 1 || entry.Attributes[0] != "BASE TABLE" {
			continue
		}
		ddl, err := session.query("SHOW CREATE TABLE " + state.quoted + ".`" + strings.ReplaceAll(entry.Name, "`", "``") + "`")
		if err != nil {
			return nil, err
		}
		if len(ddl) != 1 || len(ddl[0]) != 2 {
			return nil, fail(databasecmd.ErrDatabase)
		}
		if err := appendObject(&result, object{Kind: "table", Owner: entry.Name, Name: entry.Name, Attributes: []string{mysqlassets.NormalizeInspection(string(ddl[0][1]))}}); err != nil {
			return nil, err
		}
	}
	return result, nil
}
