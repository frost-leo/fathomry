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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	mysqlassets "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	pgassets "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
	settingsapi "github.com/frost-leo/fathomry/settings/v1"
)

func literal(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

type service struct {
	dialect, target string
	pgSettings      postgres.Settings
	mySettings      mysql.Settings
	pg              *postgres.Client
	my              *mysql.Client
}

func loadSettings(t *testing.T, name string, target any) {
	t.Helper()
	if os.Getenv("FATHOMRY_SCHEMA_ALLOW_DDL") != "1" {
		t.Fatal("isolated DDL authority required")
	}
	path := os.Getenv(name)
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
		t.Fatal("private fixture unavailable")
	}
	raw, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(raw, target) != nil {
		t.Fatal("invalid private fixture")
	}
}
func postgresClient(t *testing.T, selected postgres.Settings) *postgres.Client {
	t.Helper()
	policy, err := postgres.Recommend(selected)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	runtime, err := framework.New(lifetime, framework.Options{Operations: policy.Runtime})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[postgres.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(context.Context, adapters.Snapshot[postgres.Result]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer cancel()
		ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := runtime.Close(ctx); err != nil || !runtime.Resources().ShutdownComplete() {
			t.Error("fixture resource cleanup incomplete", err)
		}
		if err := receiver.Finish(ctx); err != nil {
			t.Error("receiver incomplete", err)
			_ = receiver.Close(ctx)
		}
		if usage, err := runtime.Operations().Inspect(); err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
			t.Error("work retained", err)
		}
		if usage, err := inbox.Inspect(); err != nil || usage.Outstanding != 0 {
			t.Error("evidence retained", err)
		}
	})
	dependencies := postgres.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	ref, err := resource.Bind(runtime.Resources(), resource.Binding[postgres.Settings, postgres.Handle]{
		Name: "fixture", Policy: resource.Fixed,
		Select: func(view settingsapi.View) (postgres.Settings, error) {
			snapshot, err := settingsapi.As[postgres.Settings](view)
			if err != nil {
				return postgres.Settings{}, err
			}
			return snapshot.ValueCopy()
		},
		Clone: func(value postgres.Settings) postgres.Settings { return value },
		Build: func(ctx context.Context, value postgres.Settings) (*resource.Instance[postgres.Handle], error) {
			owner, err := postgres.Open(ctx, value, dependencies)
			if owner == nil {
				return nil, err
			}
			return &resource.Instance[postgres.Handle]{Value: owner.Handle(), Release: owner.Release}, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settingsapi.New(selected, func(value postgres.Settings) postgres.Settings { return value })
	if err != nil {
		t.Fatal(err)
	}
	update, err := runtime.Resources().Apply(lifetime, snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(lifetime); err != nil {
		t.Fatal(err)
	}
	client, err := postgres.Using(lifetime, ref, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func mysqlClient(t *testing.T, selected mysql.Settings) *mysql.Client {
	t.Helper()
	policy, err := mysql.Recommend(selected)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	runtime, err := framework.New(lifetime, framework.Options{Operations: policy.Runtime})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[mysql.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(context.Context, adapters.Snapshot[mysql.Result]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer cancel()
		ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := runtime.Close(ctx); err != nil || !runtime.Resources().ShutdownComplete() {
			t.Error("fixture resource cleanup incomplete", err)
		}
		if err := receiver.Finish(ctx); err != nil {
			t.Error("receiver incomplete", err)
			_ = receiver.Close(ctx)
		}
		if usage, err := runtime.Operations().Inspect(); err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
			t.Error("work retained", err)
		}
		if usage, err := inbox.Inspect(); err != nil || usage.Outstanding != 0 {
			t.Error("evidence retained", err)
		}
	})
	dependencies := mysql.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	ref, err := resource.Bind(runtime.Resources(), resource.Binding[mysql.Settings, mysql.Handle]{
		Name: "fixture", Policy: resource.Fixed,
		Select: func(view settingsapi.View) (mysql.Settings, error) {
			snapshot, err := settingsapi.As[mysql.Settings](view)
			if err != nil {
				return mysql.Settings{}, err
			}
			return snapshot.ValueCopy()
		},
		Clone: func(value mysql.Settings) mysql.Settings { return value },
		Build: func(ctx context.Context, value mysql.Settings) (*resource.Instance[mysql.Handle], error) {
			owner, err := mysql.Open(ctx, value, dependencies)
			if owner == nil {
				return nil, err
			}
			return &resource.Instance[mysql.Handle]{Value: owner.Handle(), Release: owner.Release}, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settingsapi.New(selected, func(value mysql.Settings) mysql.Settings { return value })
	if err != nil {
		t.Fatal(err)
	}
	update, err := runtime.Resources().Apply(lifetime, snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(lifetime); err != nil {
		t.Fatal(err)
	}
	client, err := mysql.Using(lifetime, ref, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func fixture(t *testing.T, dialect string, target ...string) *service {
	t.Helper()
	value := &service{dialect: dialect}
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	value.target = "fathomry_runner_" + hex.EncodeToString(random[:])
	if len(target) == 1 {
		value.target = target[0]
	}
	if dialect == "postgresql" {
		loadSettings(t, "FATHOMRY_SCHEMA_POSTGRES_CONFIG", &value.pgSettings)
		if value.pgSettings.Address != "127.0.0.1" || value.pgSettings.Database != "catalog_test" || !value.pgSettings.Plaintext {
			t.Fatal("only isolated fixture admitted")
		}
		value.pgSettings.ParserHome = os.Getenv("HOME")
		value.pgSettings.MaxConnections = 2
		value.pgSettings.Timeout = 5 * time.Second
		value.pg = postgresClient(t, value.pgSettings)
		value.exec(t, "CREATE SCHEMA "+value.quoted())
		t.Cleanup(func() { value.exec(t, "DROP SCHEMA "+value.quoted()+" CASCADE") })
	} else {
		loadSettings(t, "FATHOMRY_SCHEMA_MYSQL_CONFIG", &value.mySettings)
		if value.mySettings.Address != "127.0.0.1" || value.mySettings.Database != "catalog_test" || !value.mySettings.Plaintext {
			t.Fatal("only isolated fixture admitted")
		}
		value.mySettings.MaxConnections = 2
		value.mySettings.Timeout = 5 * time.Second
		admin := &service{dialect: dialect, my: mysqlClient(t, value.mySettings)}
		admin.exec(t, "CREATE DATABASE "+value.quoted())
		t.Cleanup(func() { admin.exec(t, "DROP DATABASE "+value.quoted()) })
		value.mySettings.Database = value.target
		value.my = mysqlClient(t, value.mySettings)
	}
	return value
}
func (value *service) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	if value.pg != nil {
		_, err = value.pg.Exec(ctx, sql, args...)
	} else {
		_, err = value.my.Exec(ctx, sql, args...)
	}
	if err != nil {
		t.Fatal("fixture SQL failed", err)
	}
}
func (value *service) rows(t *testing.T, sql string, args ...any) [][][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows := [][][]byte{}
	if value.pg != nil {
		result, err := value.pg.Query(ctx, sql, args...)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range result.RowsCopy() {
			rows = append(rows, row.ValuesCopy())
		}
	} else {
		result, err := value.my.Query(ctx, sql, args...)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range result.RowsCopy() {
			rows = append(rows, row.ValuesCopy())
		}
	}
	return rows
}
func (value *service) runner(t *testing.T) *Runner {
	t.Helper()
	var runner *Runner
	var err error
	if value.pg != nil {
		runner, err = PostgreSQL(value.pg, value.target, databasecmd.Options{})
	} else {
		runner, err = MySQL(value.my, value.target, databasecmd.Options{})
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := runner.Close(ctx); err != nil || !runner.Released() {
			t.Error("runner cleanup incomplete", err)
		}
	})
	return runner
}
func (value *service) quoted() string {
	quoted, _ := quoteTarget(value.dialect, value.target)
	return quoted
}

// TestGenerateStructure is an explicit maintainer generator, not a normal
// service acceptance case. Output is disposable; reviewed bytes enter via patch.
func TestGenerateStructure(t *testing.T) {
	output := os.Getenv("FATHOMRY_SCHEMA_STRUCTURE_OUTPUT")
	if output == "" {
		t.Skip("explicit structure generation only")
	}
	if !strings.HasPrefix(filepath.Clean(output), "/home/frost/tmp/codex-build/") {
		t.Fatal("disposable output directory required")
	}
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			value := fixture(t, dialect)
			var assets fs.FS = pgassets.Assets()
			if dialect == "mysql" {
				assets = mysqlassets.Assets()
			}
			raw, err := fs.ReadFile(assets, "manifest.json")
			if err != nil {
				t.Fatal(err)
			}
			var definition manifest
			if err = json.Unmarshal(raw, &definition); err != nil {
				t.Fatal(err)
			}
			contract := &contract{manifest: definition}
			for index, entry := range append([]asset{definition.Bootstrap}, definition.Migrations[0].Steps...) {
				raw, err := fs.ReadFile(assets, entry.Path)
				if err != nil || digest(raw) != entry.SHA256 {
					t.Fatal("source checksum mismatch", err)
				}
				if index == 0 {
					contract.bootstrap = string(raw)
				} else {
					contract.sql = append(contract.sql, string(raw))
				}
			}
			state := &runnerState{dialect: dialect, target: value.target, quoted: value.quoted(), contract: contract, options: databasecmd.Options{Timeout: time.Minute, CleanupTimeout: 5 * time.Second}, pg: value.pg, mysql: value.my}
			t.Cleanup(func() {
				_, _, err := state.finish()
				if err != nil {
					t.Error(err)
				}
			})
			session, err := state.begin(context.Background(), true)
			if err != nil {
				t.Fatal(err)
			}
			locked, err := state.prepare(session)
			if err != nil || !locked {
				t.Fatal("generation profile unavailable", err)
			}
			for _, sql := range append([]string{contract.bootstrap}, contract.sql...) {
				if _, err = session.exec(strings.ReplaceAll(sql, "{{target}}", state.quoted)); err != nil {
					t.Fatal(err)
				}
			}
			objects, err := state.objects(session)
			if err != nil {
				t.Fatal(err)
			}
			derived := structure{Format: "fathomry.schema-structure/v1", SourcesSHA256: sourcesDigest(definition), Objects: objects, License: definition.License, LicenseNotice: definition.LicenseNotice}
			var encoded bytes.Buffer
			encoder := json.NewEncoder(&encoded)
			encoder.SetEscapeHTML(false)
			encoder.SetIndent("", "  ")
			if err = encoder.Encode(derived); err != nil {
				t.Fatal(err)
			}
			if encoded.Len() > maxStructureBytes {
				t.Fatal("structure exceeds bound")
			}
			if err = os.WriteFile(filepath.Join(output, dialect+"-structure.json"), encoded.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s: %d objects, %d bytes; source digest %s", dialect, len(objects), encoded.Len(), derived.SourcesSHA256)
		})
	}
}
func TestSchemaInitialInstallation(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			value := fixture(t, dialect)
			runner := value.runner(t)
			ctx := context.Background()
			status, err := runner.Status(ctx)
			if err != nil || status.State != NotInstalled {
				t.Fatal("empty status", status, err)
			}
			plan, err := runner.Plan(ctx)
			if err != nil || !plan.Applicable || len(plan.Steps) < 20 {
				t.Fatal("empty plan", plan.Status, err)
			}
			if len(value.rows(t, "SELECT table_name FROM information_schema.tables WHERE table_schema="+literal(value.target))) != 0 {
				t.Fatal("read-only operation created objects")
			}
			change, err := runner.Apply(ctx)
			if err != nil || change.Effect != Applied || change.After.State != Current || !change.CleanupComplete {
				t.Fatal("initial apply", change, err)
			}
			status, err = runner.Status(ctx)
			if err != nil || status.State != Current {
				t.Fatal("installed status", status, err)
			}
			repeat, err := runner.Apply(ctx)
			if err != nil || repeat.Effect != Unchanged || repeat.LastAcknowledgedStep != "" {
				t.Fatal("repeat apply", repeat, err)
			}
			plan, err = runner.Plan(ctx)
			if err != nil || !plan.Applicable || len(plan.Steps) != 0 {
				t.Fatal("current plan", plan.Status, err)
			}
			if !runner.Released() {
				t.Fatal("operation ownership retained")
			}
		})
	}
}
