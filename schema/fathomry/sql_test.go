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

package fathomry_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	mysqlschema "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	postgreschema "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
)

type sqlFixture struct {
	postgresClient *postgres.Client
	mysqlClient    *mysql.Client
	dialect        string
	target         string
	assets         fs.FS
	exec           func(context.Context, string, ...any) error
	query          func(context.Context, string, ...any) ([][][]byte, error)
	code           func(error) string
}

func loadSQLSettings(t *testing.T, variable string, target any) {
	t.Helper()
	if os.Getenv("FATHOMRY_SCHEMA_ALLOW_DDL") != "1" {
		t.Fatal("explicit isolated-schema DDL authorization is required")
	}
	file := os.Getenv(variable)
	info, err := os.Stat(file)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
		t.Fatal("private isolated fixture unavailable or unsafe")
	}
	raw, err := os.ReadFile(file)
	if err != nil || json.Unmarshal(raw, target) != nil {
		t.Fatal("invalid private isolated fixture")
	}
}

func sqlPostgres(t *testing.T, configure ...func(*postgres.Settings)) sqlFixture {
	t.Helper()
	var settings postgres.Settings
	loadSQLSettings(t, "FATHOMRY_SCHEMA_POSTGRES_CONFIG", &settings)
	if settings.Address != "127.0.0.1" || !settings.Plaintext || settings.Database != "catalog_test" {
		t.Fatal("only explicit loopback catalog_test fixtures are admitted")
	}
	settings.ParserHome = os.Getenv("HOME")
	settings.MaxConnections = 1
	settings.Timeout = 5 * time.Second
	for _, apply := range configure {
		apply(&settings)
	}
	policy, err := postgres.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	runtime, err := adapters.New(lifetime, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeSQLRuntime(t, runtime) })
	inbox, err := adapters.NewInbox[postgres.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(_ context.Context, _ adapters.Snapshot[postgres.Result]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	owner, err := postgres.Open(lifetime, settings, postgres.Dependencies{Runtime: runtime, Evidence: inbox})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if owner != nil {
			if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
				t.Error("PostgreSQL owner cleanup incomplete", err)
			}
		}
		if err := receiver.Finish(ctx); err != nil {
			t.Error("PostgreSQL evidence not drained", err)
			_ = receiver.Close(ctx)
		}
		if state, err := inbox.Inspect(); err != nil || state.Outstanding != 0 {
			t.Error("PostgreSQL evidence retained", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	client := owner.Client()
	return sqlFixture{
		dialect: "postgresql", assets: postgreschema.Assets(),
		postgresClient: client,
		exec: func(ctx context.Context, statement string, args ...any) error {
			result, err := client.Exec(ctx, statement, args...)
			if err == nil && !result.Complete() {
				return errors.New("incomplete PostgreSQL statement")
			}
			return err
		},
		query: func(ctx context.Context, statement string, args ...any) ([][][]byte, error) {
			result, err := client.Query(ctx, statement, args...)
			if err != nil || !result.Complete() {
				return nil, errors.Join(err, errors.New("incomplete PostgreSQL read"))
			}
			rows := make([][][]byte, 0, len(result.RowsCopy()))
			for _, row := range result.RowsCopy() {
				rows = append(rows, row.ValuesCopy())
			}
			return rows, nil
		},
		code: func(err error) string {
			if native, found := postgres.InspectError(err); found {
				return native.Code
			}
			return "not-a-server-error"
		},
	}
}

func sqlMySQL(t *testing.T, configure ...func(*mysql.Settings)) sqlFixture {
	t.Helper()
	var settings mysql.Settings
	loadSQLSettings(t, "FATHOMRY_SCHEMA_MYSQL_CONFIG", &settings)
	if settings.Address != "127.0.0.1" || !settings.Plaintext || settings.Database != "catalog_test" {
		t.Fatal("only explicit loopback catalog_test fixtures are admitted")
	}
	settings.MaxConnections = 1
	settings.Timeout = 5 * time.Second
	for _, apply := range configure {
		apply(&settings)
	}
	policy, err := mysql.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	runtime, err := adapters.New(lifetime, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeSQLRuntime(t, runtime) })
	inbox, err := adapters.NewInbox[mysql.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(_ context.Context, _ adapters.Snapshot[mysql.Result]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	owner, err := mysql.Open(lifetime, settings, mysql.Dependencies{Runtime: runtime, Evidence: inbox})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if owner != nil {
			if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
				t.Error("MySQL owner cleanup incomplete", err)
			}
		}
		if err := receiver.Finish(ctx); err != nil {
			t.Error("MySQL evidence not drained", err)
			_ = receiver.Close(ctx)
		}
		if state, err := inbox.Inspect(); err != nil || state.Outstanding != 0 {
			t.Error("MySQL evidence retained", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	client := owner.Client()
	return sqlFixture{
		dialect: "mysql", assets: mysqlschema.Assets(),
		mysqlClient: client,
		exec: func(ctx context.Context, statement string, args ...any) error {
			result, err := client.Exec(ctx, statement, args...)
			if err == nil && !result.Complete() {
				return errors.New("incomplete MySQL statement")
			}
			return err
		},
		query: func(ctx context.Context, statement string, args ...any) ([][][]byte, error) {
			result, err := client.Query(ctx, statement, args...)
			if err != nil || !result.Complete() {
				return nil, errors.Join(err, errors.New("incomplete MySQL read"))
			}
			rows := make([][][]byte, 0, len(result.RowsCopy()))
			for _, row := range result.RowsCopy() {
				rows = append(rows, row.ValuesCopy())
			}
			return rows, nil
		},
		code: func(err error) string {
			if native, found := mysql.InspectError(err); found {
				return strconv.Itoa(int(native.Number))
			}
			return "not-a-server-error"
		},
	}
}

func closeSQLRuntime(t *testing.T, runtime *adapters.Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		t.Error("runtime cleanup incomplete", err)
	}
	if state, err := runtime.Inspect(); err != nil || state.Active != 0 || state.WorkBytes != 0 {
		t.Error("runtime work retained", err)
	}
}

func (fixture sqlFixture) statement(sql string) string {
	return strings.ReplaceAll(sql, "{{target}}", fixture.target)
}

func (fixture sqlFixture) execute(t *testing.T, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := fixture.exec(ctx, fixture.statement(sql), args...); err != nil {
		t.Fatalf("SQL failed: %s (%s)", fixture.code(err), err)
	}
}

func (fixture sqlFixture) rows(t *testing.T, sql string, args ...any) [][][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := fixture.query(ctx, fixture.statement(sql), args...)
	if err != nil {
		t.Fatalf("SQL read failed: %s (%s)", fixture.code(err), err)
	}
	return rows
}

func (fixture sqlFixture) scalar(t *testing.T, sql string, args ...any) string {
	t.Helper()
	rows := fixture.rows(t, sql, args...)
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] == nil {
		t.Fatal("expected one non-NULL scalar")
	}
	return string(rows[0][0])
}

func (fixture sqlFixture) reject(t *testing.T, sql, pgCode, mysqlCode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := fixture.exec(ctx, fixture.statement(sql))
	expected := pgCode
	if fixture.dialect == "mysql" {
		expected = mysqlCode
	}
	if err == nil || fixture.code(err) != expected {
		t.Fatalf("expected server rejection %s, got %s (%s)", expected, fixture.code(err), err)
	}
}

func TestSQLAssets(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			var fixture sqlFixture
			if dialect == "postgresql" {
				fixture = sqlPostgres(t)
				if version := fixture.scalar(t, "SHOW server_version"); !strings.HasPrefix(version, "18.") {
					t.Fatal("PostgreSQL 18 fixture required")
				} else {
					t.Log("PostgreSQL", version)
				}
			} else {
				fixture = sqlMySQL(t)
				if version := fixture.scalar(t, "SELECT VERSION()"); !strings.HasPrefix(version, "8.4.") {
					t.Fatal("MySQL 8.4 fixture required")
				} else {
					t.Log("MySQL", version)
				}
				if fixture.scalar(t, "SELECT @@innodb_page_size") != "16384" {
					t.Fatal("16 KiB InnoDB page profile required")
				}
				mode := fixture.scalar(t, "SELECT @@SESSION.sql_mode")
				if !strings.Contains(mode, "STRICT_TRANS_TABLES") && !strings.Contains(mode, "STRICT_ALL_TABLES") {
					t.Fatal("strict SQL mode required")
				}
			}
			var random [8]byte
			if _, err := rand.Read(random[:]); err != nil {
				t.Fatal(err)
			}
			name := "fathomry_asset_" + hex.EncodeToString(random[:])
			create, drop := "CREATE SCHEMA ", "DROP SCHEMA "
			fixture.target = `"` + name + `"`
			if dialect == "mysql" {
				create, drop = "CREATE DATABASE ", "DROP DATABASE "
				fixture.target = "`" + name + "`"
			}
			fixture.execute(t, create+fixture.target)
			t.Cleanup(func() {
				if dialect == "postgresql" {
					drop += fixture.target + " CASCADE"
				} else {
					drop += fixture.target
				}
				fixture.execute(t, drop)
				if fixture.scalar(t, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='"+name+"'") != "0" {
					t.Error("test-owned schema/database remained after cleanup")
				}
			})
			value := readManifest(t, fixture.assets)
			for _, entry := range value.Migrations[0].Steps {
				sql, err := fs.ReadFile(fixture.assets, entry.Path)
				if err != nil {
					t.Fatal(err)
				}
				t.Log("apply", entry.ID)
				fixture.execute(t, string(sql))
			}
			count := fixture.scalar(t, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='"+name+"' AND table_type='BASE TABLE'")
			if count != fmt.Sprint(len(catalogTables)) {
				t.Fatal("catalog table inventory differs", count)
			}
			exerciseCatalogSQL(t, fixture)
		})
	}
}
