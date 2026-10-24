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

package mysql

import (
	"encoding/json"
	"strconv"
	"strings"
)

// InspectionQuery describes one bounded read of owned schema objects. SQL binds
// the target name as its sole parameter; it does not acquire locks or prove readiness.
type InspectionQuery struct{ Kind, SQL string }

// InspectionQueries returns detached read-only SQL tied to the embedded structure
// profile. The caller owns target binding, supported session settings, locks,
// row/byte budgets, manifest verification and the resulting compatibility decision.
func InspectionQueries() ([]InspectionQuery, error) {
	raw, err := assets.ReadFile("0001/structure.json")
	if err != nil {
		return nil, err
	}
	var expected struct{ Objects []struct{ Kind, Name string } }
	if err := json.Unmarshal(raw, &expected); err != nil {
		return nil, err
	}
	tables := listLiteral([]string{"fathomry_schema_history", "i18n_locale", "i18n_resource", "i18n_text", "workflow_category", "workflow_definition", "workflow_definition_tag", "workflow_version", "workflow_mode", "workflow_mode_version"})
	triggers, constraints := []string{}, []string{}
	for _, entry := range expected.Objects {
		if entry.Kind == "trigger" {
			triggers = append(triggers, entry.Name)
		}
		if entry.Kind == "constraint" {
			constraints = append(constraints, entry.Name)
		}
	}
	return []InspectionQuery{
		{"relation", "SELECT TABLE_NAME,TABLE_NAME,TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN (" + tables + ") ORDER BY TABLE_NAME LIMIT 513"},
		{"trigger", `SELECT EVENT_OBJECT_TABLE,TRIGGER_NAME,ACTION_TIMING,EVENT_MANIPULATION,ACTION_ORIENTATION,CAST(ACTION_ORDER AS CHAR),ACTION_STATEMENT,SQL_MODE,CHARACTER_SET_CLIENT,COLLATION_CONNECTION
FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=? AND (EVENT_OBJECT_TABLE IN (` + tables + `) OR TRIGGER_NAME IN (` + listLiteral(triggers) + `)) ORDER BY EVENT_OBJECT_TABLE,TRIGGER_NAME LIMIT 513`},
		{"constraint", `SELECT TABLE_NAME,CONSTRAINT_NAME,CONSTRAINT_TYPE,ENFORCED
FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=? AND (TABLE_NAME IN (` + tables + `) OR CONSTRAINT_TYPE IN ('FOREIGN KEY','CHECK') AND CONSTRAINT_NAME IN (` + listLiteral(constraints) + `)) ORDER BY TABLE_NAME,CONSTRAINT_NAME LIMIT 513`},
	}, nil
}

func listLiteral(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = "'" + strings.ReplaceAll(value, "'", "''") + "'"
	}
	if len(quoted) == 0 {
		return "NULL"
	}
	return strings.Join(quoted, ",")
}

// NormalizeInspection removes only the mutable SHOW CREATE auto-increment counter.
// The caller must use NO_BACKSLASH_ESCAPES or reject inputs containing backslashes.
// Quoted values, including comments containing counter-like text, are not rewritten.
func NormalizeInspection(sql string) string {
	// SHOW CREATE emits mutable counters as unquoted table-option tokens.
	var result strings.Builder
	quote := byte(0)
	for index := 0; index < len(sql); {
		if quote != 0 {
			result.WriteByte(sql[index])
			if sql[index] == quote {
				if index+1 < len(sql) && sql[index+1] == quote {
					index++
					result.WriteByte(sql[index])
				} else {
					quote = 0
				}
			}
			index++
			continue
		}
		if sql[index] == '\'' || sql[index] == '`' {
			quote = sql[index]
			result.WriteByte(sql[index])
			index++
			continue
		}
		const prefix = " AUTO_INCREMENT="
		if strings.HasPrefix(sql[index:], prefix) {
			end := index + len(prefix)
			for end < len(sql) && sql[end] >= '0' && sql[end] <= '9' {
				end++
			}
			if _, err := strconv.ParseUint(sql[index+len(prefix):end], 10, 64); err == nil && (end == len(sql) || sql[end] == ' ') {
				index = end
				continue
			}
		}
		result.WriteByte(sql[index])
		index++
	}
	return result.String()
}
