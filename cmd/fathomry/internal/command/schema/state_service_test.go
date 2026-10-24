//go:build schema_service

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
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
)

func requireState(t *testing.T, runner *Runner, want State) {
	t.Helper()
	status, err := runner.Status(context.Background())
	if err != nil || status.State != want {
		t.Fatal("unexpected schema state", status, err)
	}
	change, err := runner.Apply(context.Background())
	if !errors.Is(err, databasecmd.ErrSchema) || change.Effect != NotStarted || change.Before.State != want {
		t.Fatal("unproven state admitted automatic installation", change, err)
	}
}
func TestSchemaHistoryAndStructuralRefusal(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			cases := []struct {
				name      string
				want      State
				installed bool
				change    func(*testing.T, *service, *Runner)
			}{
				{"unmanaged", Unmanaged, false, func(t *testing.T, value *service, _ *Runner) {
					value.exec(t, "CREATE TABLE "+value.quoted()+".workflow_definition (id BIGINT)")
				}},
				{"empty_history", Incomplete, false, func(t *testing.T, value *service, runner *Runner) {
					value.exec(t, strings.ReplaceAll(runner.state.contract.bootstrap, "{{target}}", value.quoted()))
				}},
				{"future", Incompatible, true, func(t *testing.T, value *service, _ *Runner) {
					value.exec(t, "UPDATE "+value.quoted()+"."+historyTable+" SET migration_revision=2")
				}},
				{"missing_header", Incomplete, true, func(t *testing.T, value *service, _ *Runner) {
					value.exec(t, "DELETE FROM "+value.quoted()+"."+historyTable+" WHERE step_ordinal=0")
				}},
				{"missing_step", Incomplete, true, func(t *testing.T, value *service, _ *Runner) {
					value.exec(t, "DELETE FROM "+value.quoted()+"."+historyTable+" WHERE step_ordinal=2")
				}},
				{"unfinished", Incomplete, true, func(t *testing.T, value *service, _ *Runner) {
					value.exec(t, "UPDATE "+value.quoted()+"."+historyTable+" SET state='intent',acknowledged_at=NULL WHERE step_ordinal=0")
				}},
				{"manifest_checksum", Drifted, true, func(t *testing.T, value *service, runner *Runner) {
					value.exec(t, "UPDATE "+value.quoted()+"."+historyTable+" SET manifest_sha256="+runner.state.parameters(1), bytes.Repeat([]byte{1}, 32))
				}},
				{"step_checksum", Drifted, true, func(t *testing.T, value *service, runner *Runner) {
					value.exec(t, "UPDATE "+value.quoted()+"."+historyTable+" SET step_sha256="+runner.state.parameters(1)+" WHERE step_ordinal=2", bytes.Repeat([]byte{1}, 32))
				}},
				{"step_identity", Drifted, true, func(t *testing.T, value *service, _ *Runner) {
					value.exec(t, "UPDATE "+value.quoted()+"."+historyTable+" SET step_id='different' WHERE step_ordinal=2")
				}},
				{"column", Drifted, true, func(t *testing.T, value *service, _ *Runner) {
					value.exec(t, "ALTER TABLE "+value.quoted()+".workflow_definition ADD COLUMN unexpected INTEGER")
				}},
				{"check", Drifted, true, func(t *testing.T, value *service, _ *Runner) {
					action := "DROP CONSTRAINT"
					if value.dialect == "mysql" {
						action = "DROP CHECK"
					}
					value.exec(t, "ALTER TABLE "+value.quoted()+".workflow_version "+action+" ck_workflow_version_document")
				}},
				{"owner_fk", Drifted, true, func(t *testing.T, value *service, runner *Runner) {
					for _, entry := range runner.state.contract.expected {
						if entry.Kind == "constraint" && entry.Owner == "workflow_definition" && (entry.Attributes[0] == "f" || entry.Attributes[0] == "FOREIGN KEY") {
							action := "DROP CONSTRAINT"
							if value.dialect == "mysql" {
								action = "DROP FOREIGN KEY"
							}
							value.exec(t, "ALTER TABLE "+value.quoted()+".workflow_definition "+action+" "+entry.Name)
							return
						}
					}
					t.Fatal("owned FK fixture absent")
				}},
				{"guard", Drifted, true, func(t *testing.T, value *service, runner *Runner) {
					for _, entry := range runner.state.contract.expected {
						if entry.Kind == "trigger" && entry.Owner == "workflow_version" {
							sql := "DROP TRIGGER " + entry.Name + " ON " + value.quoted() + ".workflow_version"
							if value.dialect == "mysql" {
								sql = "DROP TRIGGER " + value.quoted() + "." + entry.Name
							}
							value.exec(t, sql)
							return
						}
					}
					t.Fatal("guard fixture absent")
				}},
			}
			for _, test := range cases {
				t.Run(test.name, func(t *testing.T) {
					value := fixture(t, dialect)
					runner := value.runner(t)
					if test.installed {
						if result, err := runner.Apply(context.Background()); err != nil || result.Effect != Applied {
							t.Fatal("fixture installation failed", err)
						}
					}
					test.change(t, value, runner)
					requireState(t, runner, test.want)
				})
			}
		})
	}
}
func TestSchemaNoOpJournalAndComments(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			value := fixture(t, dialect)
			runner := value.runner(t)
			plan, err := runner.Plan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			plan.Steps[0].ID = "changed"
			second, err := runner.Plan(context.Background())
			if err != nil || second.Steps[0].ID == "changed" {
				t.Fatal("plan aliases the manifest", err)
			}
			if _, err := runner.Apply(context.Background()); err != nil {
				t.Fatal(err)
			}
			query := "SELECT * FROM " + value.quoted() + "." + historyTable + " ORDER BY step_ordinal"
			before, _ := json.Marshal(value.rows(t, query))
			change, err := runner.Apply(context.Background())
			if err != nil || change.Effect != Unchanged {
				t.Fatal(err)
			}
			after, _ := json.Marshal(value.rows(t, query))
			if !bytes.Equal(before, after) {
				t.Fatal("repeated application rewrote history/timestamps")
			}
			var comments [][][]byte
			if dialect == "postgresql" {
				comments = value.rows(t, `SELECT obj_description(c.oid,'pg_class') FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2
UNION ALL SELECT col_description(c.oid,a.attnum) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid WHERE n.nspname=$1 AND c.relname=$2 AND a.attnum>0 AND NOT a.attisdropped`, value.target, historyTable)
			} else {
				comments = value.rows(t, `SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
UNION ALL SELECT COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, value.target, historyTable, value.target, historyTable)
			}
			if len(comments) != 10 {
				t.Fatal("journal comment inventory incomplete")
			}
			for _, row := range comments {
				if len(row) != 1 || !bytes.HasPrefix(row[0], []byte("en: ")) || !bytes.Contains(row[0], []byte("; zh-CN: ")) {
					t.Fatal("journal locale comments missing")
				}
			}
		})
	}
}
func TestSchemaPinnedTargetQuoting(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		for _, name := range []string{"old", "new", "quote\"'`target", "控制库"} {
			t.Run(dialect+"/"+name, func(t *testing.T) {
				value := fixture(t, dialect, name)
				runner := value.runner(t)
				if result, err := runner.Apply(context.Background()); err != nil || result.After.State != Current {
					t.Fatal("quoted target did not preserve structure", result, err)
				}
				if result, err := runner.Status(context.Background()); err != nil || result.State != Current {
					t.Fatal("quoted target observation changed", result, err)
				}
			})
		}
	}
}
func TestSchemaCompetingSourceAliases(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			value := fixture(t, dialect)
			first := value.runner(t)
			second := *value
			if dialect == "postgresql" {
				settings := value.pgSettings
				settings.Name = "independent-alias"
				second.pg = postgresClient(t, settings)
			} else {
				settings := value.mySettings
				settings.Name = "independent-alias"
				second.my = mysqlClient(t, settings)
			}
			contender := second.runner(t)
			session, err := first.state.begin(context.Background(), true)
			if err != nil {
				t.Fatal(err)
			}
			locked, err := first.state.prepare(session)
			if err != nil || !locked {
				t.Fatal("owner lock unavailable", err)
			}
			status, err := contender.Status(context.Background())
			if err != nil || status.State != Changing {
				t.Fatal("competing status not changing", status, err)
			}
			plan, err := contender.Plan(context.Background())
			if err != nil || plan.Applicable || plan.Status.State != Changing {
				t.Fatal("competing plan admitted", plan.Status, err)
			}
			change, err := contender.Apply(context.Background())
			if !errors.Is(err, databasecmd.ErrBusy) || change.Effect != NotStarted {
				t.Fatal("competing installer wrote", change, err)
			}
			if _, _, err := first.state.finish(); err != nil {
				t.Fatal(err)
			}
			change, err = contender.Apply(context.Background())
			if err != nil || change.Effect != Applied {
				t.Fatal("later contender failed", change, err)
			}
		})
	}
}
func TestSchemaWrongTargetAndSessionProfile(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			value := fixture(t, dialect)
			runner := value.runner(t)
			runner.state.target += "missing"
			runner.state.quoted, _ = quoteTarget(dialect, runner.state.target)
			if status, err := runner.Status(context.Background()); !errors.Is(err, databasecmd.ErrProfile) || status.State != Incompatible {
				t.Fatal("wrong physical target admitted", status, err)
			}
			if change, err := runner.Apply(context.Background()); !errors.Is(err, databasecmd.ErrProfile) || change.Effect != NotStarted {
				t.Fatal("wrong target mutated", change, err)
			}
		})
	}
	t.Run("mysql_foreign_key_checks", func(t *testing.T) {
		value := fixture(t, "mysql")
		runner := value.runner(t)
		value.exec(t, "SET SESSION foreign_key_checks=0")
		if result, err := runner.Apply(context.Background()); !errors.Is(err, databasecmd.ErrProfile) || result.Effect != NotStarted {
			t.Fatal("disabled FK session admitted", result, err)
		}
		if len(value.rows(t, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=?", value.target)) != 0 {
			t.Fatal("wrong profile created history")
		}
	})
}
func TestSchemaPostgresGuardAndFKEnforcement(t *testing.T) {
	for _, change := range []string{"function", "internal_trigger", "inheritance", "rule"} {
		t.Run(change, func(t *testing.T) {
			value := fixture(t, "postgresql")
			runner := value.runner(t)
			if _, err := runner.Apply(context.Background()); err != nil {
				t.Fatal(err)
			}
			if change == "function" {
				value.exec(t, "CREATE OR REPLACE FUNCTION "+value.quoted()+".reject_catalog_change() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $body$ BEGIN RETURN NEW; END; $body$")
			} else if change == "inheritance" {
				value.exec(t, "CREATE TABLE "+value.quoted()+".unexpected_child () INHERITS ("+value.quoted()+".workflow_definition)")
			} else if change == "rule" {
				value.exec(t, "CREATE RULE discard_insert AS ON INSERT TO "+value.quoted()+".workflow_definition DO INSTEAD NOTHING")
			} else {
				rows := value.rows(t, "SELECT t.tgname FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname='workflow_definition' AND t.tgisinternal LIMIT 1", value.target)
				if len(rows) != 1 {
					t.Fatal("internal FK enforcement trigger absent")
				}
				value.exec(t, "ALTER TABLE "+value.quoted()+".workflow_definition DISABLE TRIGGER \""+strings.ReplaceAll(string(rows[0][0]), "\"", "\"\"")+"\"")
			}
			requireState(t, runner, Drifted)
		})
	}
}
func TestSchemaCanceledOperationAndBorrowedSource(t *testing.T) {
	value := fixture(t, "postgresql")
	runner := value.runner(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("runner-lifetime-canary")
	cancel(cause)
	if result, err := runner.Apply(ctx); result.Effect != NotStarted || !errors.Is(err, cause) {
		t.Fatal("canceled attempt lost original cause", err)
	}
	if err := runner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result, err := value.pg.Ping(context.Background()); err != nil || !result.Complete() {
		t.Fatal("runner closed borrowed source", err)
	}
	if _, err := runner.Status(context.Background()); !errors.Is(err, databasecmd.ErrClosed) {
		t.Fatal("closed runner accepted work")
	}
	_, err := PostgreSQL(value.pg, value.target, databasecmd.Options{CleanupTimeout: time.Nanosecond})
	if !errors.Is(err, databasecmd.ErrInput) {
		t.Fatal(err)
	}
}

func TestSchemaMySQLMetadataVisibilityRequiresProvenRights(t *testing.T) {
	value := fixture(t, "mysql")
	operator := value.runner(t)
	if _, err := operator.Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	var random [24]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	user := "f155_" + hex.EncodeToString(random[:8])
	password := hex.EncodeToString(random[:])
	account := literal(user) + "@'%'"
	value.exec(t, "CREATE USER "+account+" IDENTIFIED BY "+literal(password))
	t.Cleanup(func() { value.exec(t, "DROP USER "+account) })
	value.exec(t, "GRANT SELECT ON "+value.quoted()+".* TO "+account)
	settings := value.mySettings
	settings.User, settings.Password, settings.Name = user, password, "metadata-reader"
	reader := *value
	reader.my = mysqlClient(t, settings)
	invisible := reader.rows(t, "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=?", value.target)
	if len(invisible) != 1 || string(invisible[0][0]) != "0" {
		t.Fatal("restricted visibility control failed")
	}
	restricted := reader.runner(t)
	status, err := restricted.Status(context.Background())
	if !errors.Is(err, databasecmd.ErrAccess) || status.State != Indeterminate {
		t.Fatal("invisible guards were misclassified", status, err)
	}
	value.exec(t, "GRANT TRIGGER ON "+value.quoted()+".* TO "+account)
	reader.my = mysqlClient(t, settings)
	visible := reader.runner(t)
	status, err = visible.Status(context.Background())
	if err != nil || status.State != Current {
		t.Fatal("explicit metadata rights did not qualify", status, err)
	}
	change, err := visible.Apply(context.Background())
	if err != nil || change.Effect != Unchanged {
		t.Fatal("read-only current no-op required DDL rights", change, err)
	}
	empty := fixture(t, "mysql")
	value.exec(t, "GRANT SELECT, TRIGGER ON "+empty.quoted()+".* TO "+account)
	settings.Database = empty.target
	readonly := *empty
	readonly.my = mysqlClient(t, settings)
	unprivileged := readonly.runner(t)
	plan, err := unprivileged.Plan(context.Background())
	if !errors.Is(err, databasecmd.ErrAccess) || plan.Applicable || plan.Status.State != NotInstalled {
		t.Fatal("unproven DDL rights admitted a plan", plan.Status, err)
	}
	change, err = unprivileged.Apply(context.Background())
	if !errors.Is(err, databasecmd.ErrAccess) || change.Effect != NotStarted {
		t.Fatal("missing DDL rights reached bootstrap", change, err)
	}
	if len(empty.rows(t, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=?", empty.target)) != 0 {
		t.Fatal("privilege refusal created history")
	}
}

func TestSchemaPostgresAcknowledgedRollbackLeavesNoHistory(t *testing.T) {
	value := fixture(t, "postgresql")
	name := "reject_" + strings.TrimPrefix(value.target, "fathomry_runner_")
	needle := "CREATE TABLE " + value.quoted() + ".workflow_version ("
	value.exec(t, "CREATE FUNCTION "+value.quoted()+".test_ddl_refusal() RETURNS event_trigger LANGUAGE plpgsql AS $body$ BEGIN IF pg_catalog.strpos(pg_catalog.current_query(), "+literal(needle)+") > 0 THEN RAISE EXCEPTION 'controlled fixture refusal'; END IF; END; $body$")
	value.exec(t, "CREATE EVENT TRIGGER "+name+" ON ddl_command_start EXECUTE FUNCTION "+value.quoted()+".test_ddl_refusal()")
	t.Cleanup(func() {
		value.exec(t, "DROP EVENT TRIGGER "+name)
		value.exec(t, "DROP FUNCTION "+value.quoted()+".test_ddl_refusal()")
	})
	runner := value.runner(t)
	change, err := runner.Apply(context.Background())
	if err == nil || change.Effect != RolledBack || !change.CleanupComplete || change.LastAcknowledgedStep == "" {
		t.Fatal("acknowledged transactional rollback was not preserved", change, err)
	}
	status, err := runner.Status(context.Background())
	if err != nil || status.State != NotInstalled {
		t.Fatal("rolled-back schema left partial history", status, err)
	}
}
