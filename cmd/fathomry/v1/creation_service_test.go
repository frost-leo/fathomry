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

package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	createcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create"
	schemaop "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type creationSettings = DatabaseConfiguration
type creationAdministration = administration

func loadServiceSettings(t *testing.T, dialect string) creationSettings {
	t.Helper()
	name := "FATHOMRY_SCHEMA_POSTGRES_CONFIG"
	if dialect == "mysql" {
		name = "FATHOMRY_SCHEMA_MYSQL_CONFIG"
	}
	path := os.Getenv(name)
	if path == "" {
		t.Skip("explicit isolated database fixture not supplied")
	}
	if os.Getenv("FATHOMRY_SCHEMA_ALLOW_DDL") != "1" {
		t.Fatal("isolated DDL authority required")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
		t.Fatal("private fixture unavailable")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("private fixture unavailable")
	}
	value := creationSettings{Dialect: dialect}
	if dialect == "postgresql" {
		value.Schema = "catalog_test"
		value.PostgreSQL = &postgres.Settings{}
		if json.Unmarshal(raw, value.PostgreSQL) != nil || value.PostgreSQL.Address != "127.0.0.1" || value.PostgreSQL.Database != "catalog_test" || !value.PostgreSQL.Plaintext {
			t.Fatal("only isolated fixture admitted")
		}
		value.PostgreSQL.ParserHome = os.Getenv("HOME")
		value.PostgreSQL.MaxConnections = 2
	} else {
		value.MySQL = &mysql.Settings{}
		if json.Unmarshal(raw, value.MySQL) != nil || value.MySQL.Address != "127.0.0.1" || value.MySQL.Database != "catalog_test" || !value.MySQL.Plaintext {
			t.Fatal("only isolated fixture admitted")
		}
		value.MySQL.MaxConnections = 2
	}
	return value
}

func managedTestControl[T controlOperation](t *testing.T, selected databaseSelection, construct func(controlClients) (T, error)) T {
	t.Helper()
	var err error
	selected.target, err = normalizeDatabase(selected.target)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := resource.New(context.Background(), resource.Options{CleanupTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := scope.Close(ctx)
		if !scope.ShutdownComplete() || err != nil && !errors.Is(err, databasecmd.ErrCleanup) {
			t.Error("test resource cleanup did not finish", err)
		}
	})
	ref, err := resource.Bind(scope, resource.Binding[databaseSelection, T]{
		Name: "test-control", Policy: resource.Fixed, Clone: databaseSelection.copy,
		Select: func(view settings.View) (databaseSelection, error) {
			snapshot, err := settings.As[databaseSelection](view)
			if err != nil {
				return databaseSelection{}, err
			}
			return snapshot.ValueCopy()
		},
		Build: func(ctx context.Context, value databaseSelection) (*resource.Instance[T], error) {
			return buildControl(ctx, value, construct)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settings.New(selected, databaseSelection.copy)
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(context.Background(), snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := ref.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(); err != nil {
			t.Error(err)
		}
	})
	value, err := lease.Value()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func creationInspector(t *testing.T, value creationSettings) controlClients {
	t.Helper()
	var result controlClients
	_ = managedTestControl(t, databaseSelection{target: value, workspace: "test.inspection", workBytes: 1 << 20}, func(clients controlClients) (*pendingControl, error) { result = clients; return &pendingControl{}, nil })
	return result
}
func creationTargetRunner(t *testing.T, value creationSettings) *schemaop.Runner {
	t.Helper()
	return managedTestControl(t, databaseSelection{target: value, workspace: "schema.workspace", workBytes: schemaop.WorkBytes}, func(clients controlClients) (*schemaop.Runner, error) {
		options := databasecmd.Options{Timeout: value.Timeout, CleanupTimeout: value.CleanupTimeout}
		if clients.pg != nil {
			return schemaop.PostgreSQL(clients.pg, value.Schema, options)
		}
		return schemaop.MySQL(clients.mysql, value.MySQL.Database, options)
	})
}
func makeCreator(t *testing.T, target creationSettings, admin creationAdministration) (*createcmd.Creator, error) {
	t.Helper()
	selected, err := normalizeDatabase(target)
	if err != nil {
		return nil, err
	}
	administrative, err := administrativeDatabase(selected, admin)
	if err != nil {
		return nil, err
	}
	return managedTestControl(t, databaseSelection{target: selected, admin: administrative, create: true, workspace: "database.create.workspace", workBytes: createcmd.WorkBytes}, func(clients controlClients) (*createcmd.Creator, error) {
		options := creationOptions(selected)
		if clients.pg != nil {
			return createcmd.PostgreSQLCreation(clients.adminPG, clients.pg, options)
		}
		return createcmd.MySQLCreation(clients.adminMySQL, clients.mysql, options)
	}), nil
}
func creationFixture(t *testing.T, dialect string) (creationSettings, creationAdministration, controlClients) {
	t.Helper()
	selected := loadServiceSettings(t, dialect)
	adminOwner := creationInspector(t, selected)
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	target := "fathomry_created_" + hex.EncodeToString(random[:])
	admin := creationAdministration{}
	if dialect == "postgresql" {
		admin.User, admin.Password, admin.MaintenanceDatabase = selected.PostgreSQL.User, selected.PostgreSQL.Password, selected.PostgreSQL.Database
		selected.PostgreSQL.Database = target
		selected.Schema = "control"
	} else {
		admin.User, admin.Password = selected.MySQL.User, selected.MySQL.Password
		selected.MySQL.Database = target
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if dialect == "postgresql" {
			_, err := adminOwner.pg.Exec(ctx, "DROP DATABASE IF EXISTS \""+target+"\"")
			if err != nil {
				t.Error("fixture database cleanup failed", err)
			}
		} else {
			_, err := adminOwner.mysql.Exec(ctx, "DROP DATABASE IF EXISTS `"+target+"`")
			if err != nil {
				t.Error("fixture database cleanup failed", err)
			}
		}
	})
	return selected, admin, adminOwner
}
func openCreator(t *testing.T, target creationSettings, admin creationAdministration) *createcmd.Creator {
	t.Helper()
	owner, err := makeCreator(t, target, admin)
	if owner != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := owner.Close(ctx); err != nil || !owner.Released() {
				t.Error("creator cleanup failed", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return owner
}
func TestDatabaseCreationService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			selected, admin, _ := creationFixture(t, dialect)
			owner := openCreator(t, selected, admin)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			change, err := owner.Create(ctx)
			if err != nil || change.Effect != createcmd.DatabaseCreated || !change.Ready || change.Stage != "ready" || !change.CleanupComplete {
				t.Fatal("creation failed", change, err)
			}
			if dialect == "postgresql" && change.SchemaEffect != createcmd.DatabaseCreated || dialect == "mysql" && change.SchemaEffect != createcmd.DatabaseNotApplicable {
				t.Fatal("schema effect differs", change)
			}
			repeated, err := owner.Create(ctx)
			if !errors.Is(err, databasecmd.ErrInput) || repeated != change {
				t.Fatal("same owner replayed creation", repeated, err)
			}
			if err := owner.Close(ctx); err != nil {
				t.Fatal(err)
			}
			stored := creationTargetRunner(t, selected)
			status, err := stored.Status(ctx)
			if err != nil || status.State != schemaop.NotInstalled {
				t.Fatal("creation installed schema or left target unusable", status, err)
			}
			existing := openCreator(t, selected, admin)
			refused, err := existing.Create(ctx)
			if !errors.Is(err, databasecmd.ErrDatabaseExists) || refused.Effect != createcmd.DatabaseUnchanged || refused.Ready || refused.Stage != "existing" {
				t.Fatal("existing database was adopted", refused, err)
			}
			installed, err := stored.Apply(ctx)
			if err != nil || installed.Effect != schemaop.Applied {
				t.Fatal("created target cannot install", installed, err)
			}
		})
	}
}
func TestCreationPreservesAcknowledgementAfterTargetAuthFailure(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			selected, admin, inspector := creationFixture(t, dialect)
			if dialect == "postgresql" {
				selected.PostgreSQL.Password = "wrong-runtime-password"
			} else {
				selected.MySQL.Password = "wrong-runtime-password"
			}
			owner := openCreator(t, selected, admin)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			change, err := owner.Create(ctx)
			if err == nil || change.Effect != createcmd.DatabaseCreated || change.Stage != "target" || change.Ready || !change.CleanupComplete {
				t.Fatal("target failure erased created database", change, err)
			}
			var count string
			if dialect == "postgresql" {
				rows, err := inspector.pg.Query(ctx, "SELECT count(*)::text FROM pg_catalog.pg_database WHERE datname=$1", selected.PostgreSQL.Database)
				if err != nil {
					t.Fatal(err)
				}
				count = string(rows.RowsCopy()[0].ValuesCopy()[0])
			} else {
				rows, err := inspector.mysql.Query(ctx, "SELECT CAST(count(*) AS CHAR) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=?", selected.MySQL.Database)
				if err != nil {
					t.Fatal(err)
				}
				count = string(rows.RowsCopy()[0].ValuesCopy()[0])
			}
			if count != "1" {
				t.Fatal("partial creation was silently undone")
			}
		})
	}
}
func TestConcurrentDatabaseCreatorsService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			selected, admin, _ := creationFixture(t, dialect)
			owners := []*createcmd.Creator{openCreator(t, selected, admin), openCreator(t, selected, admin)}
			outcomes := make([]createcmd.DatabaseChange, 2)
			failures := make([]error, 2)
			start := make(chan struct{})
			var group sync.WaitGroup
			for index, owner := range owners {
				group.Go(func() {
					<-start
					ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					outcomes[index], failures[index] = owner.Create(ctx)
				})
			}
			close(start)
			group.Wait()
			created, refused := 0, 0
			for index, outcome := range outcomes {
				if failures[index] == nil && outcome.Effect == createcmd.DatabaseCreated && outcome.Ready {
					created++
				} else if errors.Is(failures[index], databasecmd.ErrDatabaseExists) && outcome.Effect == createcmd.DatabaseUnchanged {
					refused++
				} else {
					if native, found := postgres.InspectError(failures[index]); found {
						t.Logf("SQLSTATE=%s database_name_index=%t database_catalog=%t", native.Code, native.ConstraintName == "pg_database_datname_index", native.SchemaName == "pg_catalog" && native.TableName == "pg_database")
					}
					t.Fatalf("unexpected creator outcome: effect=%s stage=%s cause_types=%T", outcome.Effect, outcome.Stage, failures[index])
				}
			}
			if created != 1 || refused != 1 {
				t.Fatal("creation uniqueness failed")
			}
		})
	}
}
func TestPostgresPublicSchemaCreationService(t *testing.T) {
	selected, admin, _ := creationFixture(t, "postgresql")
	selected.Schema = "public"
	owner := openCreator(t, selected, admin)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	change, err := owner.Create(ctx)
	if err != nil || !change.Ready || change.SchemaEffect != createcmd.DatabaseUnchanged || !strings.EqualFold(string(change.Effect), "created") {
		t.Fatal("built-in public schema was recreated", change, err)
	}
}
