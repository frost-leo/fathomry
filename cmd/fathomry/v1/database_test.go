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

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	"github.com/frost-leo/fathomry/failure/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
)

func resolverSettings(dialect string) DatabaseConfiguration {
	if dialect == "postgresql" {
		return DatabaseConfiguration{Dialect: dialect, Schema: "control", PostgreSQL: &postgres.Settings{Name: "resolver-test", Address: "127.0.0.1", Port: 1, Database: "catalog_test", User: "test", Password: "private-canary", Plaintext: true, ParserHome: os.Getenv("HOME"), MaxConnections: 2}}
	}
	return DatabaseConfiguration{Dialect: "mysql", MySQL: &mysql.Settings{Name: "resolver-test", Address: "127.0.0.1", Port: 1, Database: "catalog_test", User: "test", Password: "private-canary", Plaintext: true, MaxConnections: 2}}
}
func TestDatabaseResolverRetainsBoundedHandoff(t *testing.T) {
	ctx := context.Background()
	original := resolverSettings("mysql")
	selected, err := resolveDatabase(ctx, func(context.Context) (DatabaseConfiguration, error) { return original, nil })
	if err != nil {
		t.Fatal(err)
	}
	original.MySQL.Password = "changed"
	if selected.MySQL.Password == "changed" {
		t.Fatal("resolver settings alias")
	}
	cause := errors.New("private-resolver-canary")
	original.Records = []configuration.Record{{}}
	failed, err := resolveDatabase(ctx, func(context.Context) (DatabaseConfiguration, error) { return original, cause })
	if !errors.Is(err, databasecmd.ErrConfiguration) || !errors.Is(err, cause) || len(failed.Records) != 1 || &failed.Records[0] == &original.Records[0] {
		t.Fatal("failed resolver lost or aliased bounded evidence", err)
	}
	failed, err = resolveDatabase(ctx, func(context.Context) (DatabaseConfiguration, error) { return original, nil })
	if !errors.Is(err, databasecmd.ErrConfiguration) || len(failed.Records) != 1 {
		t.Fatal("invalid evidence lost", err)
	}
	original.Records = make([]configuration.Record, 129)
	failed, err = resolveDatabase(ctx, func(context.Context) (DatabaseConfiguration, error) { return original, cause })
	if !errors.Is(err, databasecmd.ErrLimit) || !errors.Is(err, cause) || len(failed.Records) != 0 {
		t.Fatal("excessive handoff retained", err)
	}
	canceled, cancel := context.WithCancelCause(ctx)
	cause = errors.New("canceled-during-resolve")
	_, err = resolveDatabase(canceled, func(context.Context) (DatabaseConfiguration, error) {
		cancel(cause)
		return resolverSettings("mysql"), nil
	})
	if !errors.Is(err, databasecmd.ErrCanceled) || !errors.Is(err, cause) {
		t.Fatal("resolver cancellation lost", err)
	}
	if _, err := resolveDatabase(ctx, nil); !errors.Is(err, databasecmd.ErrConfiguration) {
		t.Fatal("ambient resolver invented")
	}
}

func TestDatabaseConfigurationRecords(t *testing.T) {
	if !errors.Is(checkDatabaseRecords(nil, DatabaseConfiguration{}), databasecmd.ErrInput) {
		t.Fatal("nil configuration context accepted")
	}
	selected := resolverSettings("mysql")
	selected.Records = []configuration.Record{{}}
	if !errors.Is(checkDatabaseRecords(context.Background(), selected), databasecmd.ErrConfiguration) {
		t.Fatal("unreleased configuration evidence accepted")
	}
}

func TestDatabaseAdmissionKeepsCommonOccurrence(t *testing.T) {
	for _, input := range []DatabaseConfiguration{{}, {Dialect: "postgresql"}, {Dialect: "mysql"}} {
		_, err := normalizeDatabase(input)
		core, known := failure.Inspect(err)
		if !known || core.Diagnostic().Definition.Code != databasecmd.ErrInput || core.Diagnostic().Location.Operation != "configuration" {
			t.Fatal("configuration refused without its common occurrence", err)
		}
	}
	for _, dialect := range []string{"postgresql", "mysql"} {
		target, err := normalizeDatabase(resolverSettings(dialect))
		if err != nil {
			t.Fatal(err)
		}
		admin := administration{User: "admin"}
		if dialect == "mysql" {
			admin.MaintenanceDatabase = "unexpected"
		}
		_, err = administrativeDatabase(target, admin)
		core, known := failure.Inspect(err)
		if !known || core.Diagnostic().Definition.Code != databasecmd.ErrInput || core.Diagnostic().Location.Operation != "administration" {
			t.Fatal("administrative refusal lost its common occurrence", err)
		}
	}
}

func TestDatabaseHandoffAdmissionAndDiagnostics(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		original := resolverSettings(dialect)
		selected, err := normalizeDatabase(original)
		if err != nil || selected.Timeout != 45*time.Second || selected.CleanupTimeout != 5*time.Second {
			t.Fatal("runtime defaults not admitted", err)
		}
		if selected.PostgreSQL != nil {
			selected.PostgreSQL.Database = "changed"
			if original.PostgreSQL.Database == "changed" {
				t.Fatal("provider data aliased")
			}
		} else {
			selected.MySQL.Database = "changed"
			if original.MySQL.Database == "changed" {
				t.Fatal("provider data aliased")
			}
		}
		original.Timeout = time.Nanosecond
		if _, err := normalizeDatabase(original); !errors.Is(err, databasecmd.ErrInput) {
			t.Fatal("invalid timing accepted", err)
		}
	}
	value := resolverSettings("mysql")
	for _, input := range []any{value, &value, administration{User: "private-canary", Password: "private-canary"}} {
		if bytes.Contains([]byte(fmt.Sprintf("%+v %#v", input, input)), []byte("private-canary")) {
			t.Fatal("formatted credentials exposed")
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("handoff", "value", input)
		if bytes.Contains(output.Bytes(), []byte("private-canary")) {
			t.Fatal("logged credentials exposed")
		}
	}
	if _, err := json.Marshal(value); !errors.Is(err, databasecmd.ErrSerialization) {
		t.Fatal("handoff serialized as deployment configuration", err)
	}
}

func TestAdministrativeSelectionCannotRedirectTarget(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		target, err := normalizeDatabase(resolverSettings(dialect))
		if err != nil {
			t.Fatal(err)
		}
		admin := administration{User: "admin", Password: "private-admin"}
		if dialect == "postgresql" {
			admin.MaintenanceDatabase = "maintenance"
		}
		selected, err := administrativeDatabase(target, admin)
		if err != nil {
			t.Fatal(err)
		}
		if selected.PostgreSQL != nil {
			if selected.PostgreSQL.Database != "maintenance" || selected.PostgreSQL.Address != target.PostgreSQL.Address || target.PostgreSQL.Database != "catalog_test" {
				t.Fatal("administration redirected or mutated target")
			}
			admin.MaintenanceDatabase = target.PostgreSQL.Database
		} else {
			if selected.MySQL.Database != "" || selected.MySQL.Address != target.MySQL.Address || target.MySQL.Database != "catalog_test" {
				t.Fatal("administration selected or mutated future database")
			}
			admin.MaintenanceDatabase = "mysql"
		}
		if _, err := administrativeDatabase(target, admin); !errors.Is(err, databasecmd.ErrInput) {
			t.Fatal("invalid maintenance target accepted")
		}
	}
}
