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
	"bytes"
	"encoding/hex"
	"strconv"
	"strings"

	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
)

func (state *runnerState) inspect(session *connection) (Status, error) {
	objects, err := state.objects(session)
	if err != nil {
		return state.status(Indeterminate, "inspection_failed"), err
	}
	again := func() (bool, error) {
		after, err := state.objects(session)
		if err != nil {
			return false, err
		}
		return sameObjects(objects, after), nil
	}
	history := ownedObjects(objects, historyTable)
	if len(history) == 0 {
		stable, err := again()
		if err != nil {
			return state.status(Indeterminate, "inspection_failed"), err
		}
		if !stable {
			return state.status(Changing, "objects_changed"), nil
		}
		if len(objects) == 0 {
			return state.status(NotInstalled, "empty"), nil
		}
		return state.status(Unmanaged, "objects_without_history"), nil
	}
	if !sameObjects(history, ownedObjects(state.contract.expected, historyTable)) {
		return state.status(Unmanaged, "history_structure"), nil
	}
	steps := state.contract.manifest.Migrations[0].Steps
	rows, err := session.query("SELECT family,migration_revision,step_ordinal,manifest_sha256,step_id,step_sha256,state,CASE WHEN acknowledged_at IS NULL THEN 0 ELSE 1 END FROM " + state.quoted + "." + historyTable + " ORDER BY migration_revision,step_ordinal LIMIT " + strconv.Itoa(len(steps)+2))
	if err != nil {
		return state.status(Indeterminate, "history_unavailable"), err
	}
	if len(rows) == 0 {
		return state.status(Incomplete, "empty_history"), nil
	}
	complete := true
	for index, row := range rows {
		if len(row) != 8 {
			return state.status(Incomplete, "history_shape"), nil
		}
		if string(row[0]) != family || string(row[1]) != "1" {
			return state.status(Incompatible, "history_revision"), nil
		}
		ordinal, err := strconv.Atoi(string(row[2]))
		if err != nil || ordinal < 0 || ordinal > len(steps) {
			return state.status(Incompatible, "history_step"), nil
		}
		if ordinal != index {
			return state.status(Incomplete, "history_gap"), nil
		}
		expected := state.contract.manifest.Bootstrap
		expected.ID = state.contract.manifest.Migrations[0].Name
		wantState := "complete"
		if ordinal > 0 {
			expected = steps[ordinal-1]
			wantState = "applied"
		}
		hash, err := state.binary(row[3])
		if err != nil || !bytes.Equal(hash, state.contract.digest) {
			return state.status(Drifted, "manifest_checksum"), nil
		}
		hash, err = state.binary(row[5])
		if err != nil || hex.EncodeToString(hash) != expected.SHA256 || string(row[4]) != expected.ID {
			return state.status(Drifted, "step_checksum"), nil
		}
		switch string(row[6]) {
		case "intent":
			if string(row[7]) != "0" {
				return state.status(Incomplete, "history_acknowledgement"), nil
			}
			complete = false
		case wantState:
			if string(row[7]) != "1" {
				return state.status(Incomplete, "history_acknowledgement"), nil
			}
		default:
			return state.status(Incomplete, "history_state"), nil
		}
	}
	if len(rows) != len(steps)+1 || !complete {
		return state.status(Incomplete, "unfinished_history"), nil
	}
	if !sameObjects(objects, state.contract.expected) {
		return state.status(Drifted, "owned_structure"), nil
	}
	if err = state.english(session); err != nil {
		return state.status(Drifted, "english_seed"), err
	}
	stable, err := again()
	if err != nil {
		return state.status(Indeterminate, "inspection_failed"), err
	}
	if !stable {
		return state.status(Changing, "objects_changed"), nil
	}
	return state.status(Current, "verified"), nil
}
func (state *runnerState) binary(value []byte) ([]byte, error) {
	if state.dialect == "postgresql" {
		if !bytes.HasPrefix(value, []byte("\\x")) {
			return nil, fail(databasecmd.ErrSchema)
		}
		result, err := hex.DecodeString(string(value[2:]))
		if err != nil {
			return nil, fail(databasecmd.ErrSchema)
		}
		return result, nil
	}
	return value, nil
}
func (state *runnerState) parameters(count int) string {
	values := make([]string, count)
	for index := range values {
		values[index] = "?"
		if state.dialect == "postgresql" {
			values[index] = "$" + strconv.Itoa(index+1)
		}
	}
	return strings.Join(values, ",")
}
func (state *runnerState) english(session *connection) error {
	value, err := session.scalar("SELECT COUNT(*) FROM "+state.quoted+".i18n_locale WHERE code="+state.parameters(1)+" AND enabled", "en")
	if err != nil {
		return err
	}
	if value != "1" {
		return fail(databasecmd.ErrSchema)
	}
	return nil
}
func (state *runnerState) intent(session *connection, ordinal int, entry asset) error {
	hash, err := hex.DecodeString(entry.SHA256)
	if err != nil {
		return fail(databasecmd.ErrAssets)
	}
	count, err := session.exec("INSERT INTO "+state.quoted+"."+historyTable+" (family,migration_revision,step_ordinal,manifest_sha256,step_id,step_sha256,state) VALUES ("+state.parameters(7)+")",
		family, int64(1), int64(ordinal), state.contract.digest, entry.ID, hash, "intent")
	if err != nil {
		return err
	}
	if count != 1 {
		return fail(databasecmd.ErrSchema)
	}
	return nil
}
func (state *runnerState) acknowledge(session *connection, ordinal int) error {
	mark, clock := "applied", "CURRENT_TIMESTAMP(6)"
	if ordinal == 0 {
		mark = "complete"
	}
	if state.dialect == "postgresql" {
		clock = "clock_timestamp()"
	}
	parameters := strings.Split(state.parameters(2), ",")
	count, err := session.exec("UPDATE "+state.quoted+"."+historyTable+" SET state="+parameters[0]+",acknowledged_at="+clock+
		" WHERE migration_revision=1 AND step_ordinal="+parameters[1]+" AND state='intent'", mark, int64(ordinal))
	if err != nil {
		return err
	}
	if count != 1 {
		return fail(databasecmd.ErrSchema)
	}
	return nil
}
func (state *runnerState) install(session *connection, result *Change) error {
	if _, err := session.exec(strings.ReplaceAll(state.contract.bootstrap, "{{target}}", state.quoted)); err != nil {
		return err
	}
	result.LastAcknowledgedStep = state.contract.manifest.Bootstrap.ID
	header := state.contract.manifest.Bootstrap
	header.ID = state.contract.manifest.Migrations[0].Name
	if err := state.intent(session, 0, header); err != nil {
		return err
	}
	for index, step := range state.contract.manifest.Migrations[0].Steps {
		if err := state.intent(session, index+1, step); err != nil {
			return err
		}
		if _, err := session.exec(strings.ReplaceAll(state.contract.sql[index], "{{target}}", state.quoted)); err != nil {
			return err
		}
		result.LastAcknowledgedStep = step.ID
		if err := state.acknowledge(session, index+1); err != nil {
			return err
		}
	}
	objects, err := state.objects(session)
	if err != nil {
		return err
	}
	if !sameObjects(objects, state.contract.expected) {
		return fail(databasecmd.ErrSchema)
	}
	if err = state.english(session); err != nil {
		return err
	}
	return state.acknowledge(session, 0)
}
