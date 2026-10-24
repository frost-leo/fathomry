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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"

	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
)

func (state *runnerState) prepare(session *connection) (bool, error) {
	var physical string
	if state.dialect == "postgresql" {
		rows, err := session.query("SELECT current_setting('server_version_num'), current_database(), current_setting('server_encoding'), pg_is_in_recovery()::text, current_setting('session_replication_role')")
		if err != nil {
			return false, err
		}
		if len(rows) != 1 || len(rows[0]) != 5 {
			return false, fail(databasecmd.ErrProfile)
		}
		values := texts(rows[0])
		version, err := strconv.Atoi(values[0])
		if err != nil || version < 180000 || version >= 190000 || values[2] != "UTF8" || values[3] != "false" || values[4] != "origin" {
			return false, fail(databasecmd.ErrProfile)
		}
		physical = values[1]
		if _, err = session.exec("SET LOCAL search_path TO pg_catalog"); err != nil {
			return false, err
		}
		if _, err = session.exec("SET LOCAL TIME ZONE 'UTC'"); err != nil {
			return false, err
		}
		if _, err = session.exec("SET LOCAL standard_conforming_strings TO on"); err != nil {
			return false, err
		}
		exists, err := session.scalar("SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname=$1", state.target)
		if err != nil {
			return false, err
		}
		if exists != "1" {
			return false, fail(databasecmd.ErrProfile)
		}
	} else {
		rows, err := session.query("SELECT VERSION(), DATABASE(), @@innodb_page_size, @@lower_case_table_names, @@autocommit, @@session.foreign_key_checks, @@session.unique_checks, @@read_only, @@super_read_only")
		if err != nil {
			return false, err
		}
		if len(rows) != 1 || len(rows[0]) != 9 {
			return false, fail(databasecmd.ErrProfile)
		}
		value := texts(rows[0])
		if !strings.HasPrefix(value[0], "8.4.") || value[1] != state.target || value[2] != "16384" || value[3] != "0" ||
			value[4] != "1" || value[5] != "1" || value[6] != "1" || value[7] != "0" || value[8] != "0" {
			return false, fail(databasecmd.ErrProfile)
		}
		physical = value[1]
		if _, err = session.exec("SET SESSION time_zone='+00:00', sql_mode='STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION,NO_BACKSLASH_ESCAPES', sql_quote_show_create=1, show_create_table_verbosity=ON"); err != nil {
			return false, err
		}
		if _, err = session.exec("SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin"); err != nil {
			return false, err
		}
	}
	if err := state.privileges(session, false); err != nil {
		return false, err
	}
	name := sha256.Sum256([]byte(family + "\x00" + physical + "\x00" + state.target))
	if state.dialect == "postgresql" {
		value, err := session.scalar("SELECT pg_catalog.pg_try_advisory_xact_lock($1)", int64(binary.BigEndian.Uint64(name[:8])))
		if err != nil {
			return false, err
		}
		if value != "t" && value != "f" {
			return false, fail(databasecmd.ErrDatabase)
		}
		return value == "t", nil
	}
	value, err := session.scalar("SELECT GET_LOCK(?,0)", hex.EncodeToString(name[:]))
	if err != nil {
		return false, err
	}
	if value != "1" && value != "0" {
		return false, fail(databasecmd.ErrDatabase)
	}
	return value == "1", nil
}
func (state *runnerState) privileges(session *connection, write bool) error {
	if state.dialect == "postgresql" {
		permission := "USAGE"
		value, err := session.scalar("SELECT pg_catalog.has_schema_privilege(current_user,$1,$2)", state.target, permission)
		if err != nil {
			return err
		}
		if value != "t" {
			return fail(databasecmd.ErrAccess)
		}
		if write {
			for _, query := range []string{
				"SELECT pg_catalog.has_schema_privilege(current_user,$1,'CREATE')",
				"SELECT pg_catalog.has_language_privilege(current_user,'plpgsql','USAGE')",
			} {
				var values []any
				if strings.Contains(query, "$1") {
					values = []any{state.target}
				}
				value, err = session.scalar(query, values...)
				if err != nil {
					return err
				}
				if value != "t" {
					return fail(databasecmd.ErrAccess)
				}
			}
		}
		return nil
	}
	rows, err := session.query("SHOW GRANTS FOR CURRENT_USER")
	if err != nil {
		return err
	}
	required := []string{"SELECT", "TRIGGER"}
	if write {
		required = append(required, "CREATE", "ALTER", "INDEX", "REFERENCES", "INSERT", "UPDATE")
	}
	granted, super := directGrants(rows, state.quoted)
	for _, permission := range required {
		if !granted[permission] {
			return fail(databasecmd.ErrAccess)
		}
	}
	if write {
		rows, err = session.query("SELECT @@log_bin, @@log_bin_trust_function_creators")
		if err != nil {
			return err
		}
		if len(rows) != 1 || len(rows[0]) != 2 {
			return fail(databasecmd.ErrProfile)
		}
		if string(rows[0][0]) == "1" && string(rows[0][1]) != "1" && !super {
			return fail(databasecmd.ErrAccess)
		}
	}
	return nil
}
func directGrants(rows [][][]byte, target string) (map[string]bool, bool) {
	result := map[string]bool{}
	super := false
	for _, row := range rows {
		if len(row) != 1 {
			continue
		}
		grant := string(row[0])
		if strings.HasPrefix(grant, "REVOKE ") {
			return map[string]bool{}, false
		}
		if !strings.HasPrefix(grant, "GRANT ") {
			continue
		}
		list, scope, found := strings.Cut(strings.TrimPrefix(grant, "GRANT "), " ON ")
		if !found {
			continue
		}
		global := strings.HasPrefix(scope, "*.* TO ")
		if !global && !strings.HasPrefix(scope, target+".* TO ") {
			continue
		}
		permissions := strings.Split(list, ", ")
		all := slices.Contains(permissions, "ALL PRIVILEGES")
		if all {
			permissions = append(permissions, "SELECT", "TRIGGER", "CREATE", "ALTER", "INDEX", "REFERENCES", "INSERT", "UPDATE")
		}
		for _, permission := range permissions {
			result[permission] = true
		}
		super = super || global && (all || slices.Contains(permissions, "SUPER"))
	}
	return result, super
}
func texts(cells [][]byte) []string {
	result := make([]string, len(cells))
	for index, cell := range cells {
		result[index] = string(cell)
	}
	return result
}
