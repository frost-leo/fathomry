//go:build catalog_service

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

package catalog

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
	"github.com/frost-leo/fathomry/resource/v1"
	mysqlassets "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	pgassets "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

var persistenceInstaller string

func TestMain(tests *testing.M) {
	if os.Getenv("FATHOMRY_SCHEMA_ALLOW_DDL") != "1" ||
		os.Getenv("FATHOMRY_SCHEMA_POSTGRES_CONFIG") == "" && os.Getenv("FATHOMRY_SCHEMA_MYSQL_CONFIG") == "" {
		os.Exit(tests.Run())
	}
	job, err := os.MkdirTemp("", "catalog-installer-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "installer scratch unavailable")
		os.Exit(1)
	}
	persistenceInstaller = filepath.Join(job, "installer")
	source := filepath.Join(job, "main.go")
	err = os.WriteFile(source, []byte(persistenceInstallerSource), 0600)
	if err == nil {
		root, problem := filepath.Abs("../../../..")
		if problem != nil {
			err = problem
		} else {
			module := fmt.Sprintf("module example.test/catalog-fixture\n\ngo 1.27.0\n\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", root)
			err = os.WriteFile(filepath.Join(job, "go.mod"), []byte(module), 0600)
		}
	}
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		command := exec.CommandContext(ctx, "go", "build", "-mod=mod", "-o", persistenceInstaller, ".")
		command.Dir = job
		command.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local")
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		err = command.Run()
		cancel()
		if err == nil {
			binary, problem := os.ReadFile(persistenceInstaller)
			if problem != nil {
				err = problem
			} else {
				fmt.Fprintf(os.Stderr, "Independent project artifact SHA256: %x\n", sha256.Sum256(binary))
			}
		}
	}
	status := 1
	if err == nil {
		status = tests.Run()
	} else {
		fmt.Fprintln(os.Stderr, "actual CLI installer could not be built")
	}
	if err := os.RemoveAll(job); err != nil {
		fmt.Fprintln(os.Stderr, "installer scratch cleanup failed")
		status = 1
	}
	os.Exit(status)
}
func installPersistence(t *testing.T, dialect, target string, selected any, install bool) {
	t.Helper()
	if !install {
		return
	}
	config := filepath.Join(t.TempDir(), "settings.json")
	raw, err := json.Marshal(selected)
	if err != nil {
		t.Fatal("fixture settings unavailable")
	}
	if err := os.WriteFile(config, raw, 0600); err != nil {
		t.Fatal("fixture settings unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, persistenceInstaller, dialect, target, config)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("actual schema apply failed: %v; %s", err, output)
	}
	var report struct {
		Data struct {
			Effect          string `json:"effect"`
			CleanupComplete bool   `json:"cleanup_complete"`
		} `json:"data"`
	}
	if err := json.Unmarshal(output, &report); err != nil || report.Data.Effect != "applied" || !report.Data.CleanupComplete {
		t.Fatalf("schema apply did not prove completed installation: %s", output)
	}
}

const persistenceInstallerSource = `package main
import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "io"
 "os"
 "time"
 mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
 postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
 cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
 catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
 wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
 "github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
)
type rejectedOutput struct{}
func (rejectedOutput) Write([]byte)(int,error){return 0,errors.New("controlled output failure")}
func main() {
 resolver:=func(ctx context.Context)(cli.DatabaseConfiguration,error){
  if os.Getenv("FATHOMRY_CATALOG_OFFLINE")=="1" {return cli.DatabaseConfiguration{},os.ErrNotExist}
  raw,err:=os.ReadFile(os.Args[3]); if err!=nil{return cli.DatabaseConfiguration{},err}
  value:=cli.DatabaseConfiguration{Dialect:os.Args[1]}
  if value.Dialect=="postgresql" { value.Schema=os.Args[2];value.PostgreSQL=new(postgres.Settings);err=json.Unmarshal(raw,value.PostgreSQL) } else { value.MySQL=new(mysql.Settings);err=json.Unmarshal(raw,value.MySQL) }
  return value,err
 }
 workflows:=[]cli.Workflow{}
 for _,code:=range []string{"server-health","second-health"}{
  declared:=health.Declarations();declared.Workflows[0].Code=code
  metadata:=&catalog.Metadata{Resources:[]catalog.Resource{{Target:catalog.Target{Kind:catalog.Definition},Field:catalog.Name,Text:catalog.Text{English:"Health"}}}}
  for _,mode:=range declared.Workflows[0].Modes {metadata.Resources=append(metadata.Resources,catalog.Resource{Target:catalog.Target{Kind:catalog.Mode,Version:"1",Mode:mode.Code},Field:catalog.Name,Text:catalog.Text{English:mode.Code}})}
  if code=="second-health"&&os.Getenv("FATHOMRY_CATALOG_BAD_SECOND")=="1"{declared.Workflows[0].Spec.Limits.ActiveChunks=wf.Fixed("0")}
  if code=="second-health"&&os.Getenv("FATHOMRY_CATALOG_MISSING_METADATA")=="1"{metadata=nil}
  if code=="server-health"&&os.Getenv("FATHOMRY_CATALOG_CONFLICT")=="1"{metadata.Resources[0].Text.English="Conflicting"}
  workflows=append(workflows,cli.Workflow{Declaration:declared,Metadata:metadata})
 }
 if os.Getenv("FATHOMRY_CATALOG_NO_WORKFLOWS")=="1"{workflows=nil}
 args:=os.Args[4:];if len(args)==0{args=[]string{"--output=json","schema","apply"}}
 var output io.Writer=os.Stdout
 if os.Getenv("FATHOMRY_CATALOG_OUTPUT_FAILURE")=="1"{output=rejectedOutput{}}
 _,err:=cli.Handle(context.Background(),args,cli.Project{Name:"catalog-fixture",Control:resolver,Workflows:workflows},cli.Options{Input:os.Stdin,Output:output,ErrorOutput:os.Stderr,Language:"en"})
 var retained *cli.ExecutionError
 if errors.As(err,&retained){
  if path:=os.Getenv("FATHOMRY_CATALOG_REPORT_PATH");path!=""{if write:=os.WriteFile(path,retained.ReportCopy(),0600);write!=nil{panic("report write failed")}}
  cleanup,cancel:=context.WithTimeout(context.Background(),5*time.Second);_ = retained.Close(cleanup);cancel()
 }
 if err!=nil {fmt.Fprintln(os.Stderr,err)}
 os.Exit(cli.ExitCode(err))
}
`

type persistenceFixture struct {
	dialect, target, quoted string
	pg                      *postgres.Client
	my                      *mysql.Client
	port                    uint16
}

func loadPersistenceSettings(t *testing.T, dialect string, target any) {
	t.Helper()
	name := "FATHOMRY_SCHEMA_POSTGRES_CONFIG"
	if dialect == "mysql" {
		name = "FATHOMRY_SCHEMA_MYSQL_CONFIG"
	}
	path := os.Getenv(name)
	if path == "" {
		t.Skip("explicit isolated catalog fixture not supplied")
	}
	if os.Getenv("FATHOMRY_SCHEMA_ALLOW_DDL") != "1" {
		t.Fatal("isolated DDL authority required")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
		t.Fatal("private fixture unavailable")
	}
	raw, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(raw, target) != nil {
		t.Fatal("invalid private fixture")
	}
}
func persistencePostgres(t *testing.T, selected postgres.Settings) *postgres.Client {
	t.Helper()
	selected.ParserHome = os.Getenv("HOME")
	selected.MaxConnections = 4
	selected.MaxArgumentBytes = 16 << 20
	selected.MaxMessageBytes = 16 << 20
	selected.MaxResultBytes = 16 << 20
	policy, err := postgres.Recommend(selected)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := framework.New(context.Background(), framework.Options{Operations: policy.Runtime})
	if err != nil {
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil || !runtime.Resources().ShutdownComplete() {
			t.Error("resource cleanup incomplete", err)
		}
		if err := receiver.Finish(ctx); err != nil {
			t.Error("required evidence not drained", err)
		}
		if usage, err := runtime.Operations().Inspect(); err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
			t.Error("retained operation budget", err)
		}
	})
	dependencies := postgres.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	ref, err := resource.Bind(runtime.Resources(), resource.Binding[postgres.Settings, postgres.Handle]{
		Name: "catalog-target", Policy: resource.Fixed, Clone: func(value postgres.Settings) postgres.Settings { return value },
		Select: func(view settings.View) (postgres.Settings, error) {
			snapshot, err := settings.As[postgres.Settings](view)
			if err != nil {
				return postgres.Settings{}, err
			}
			return snapshot.ValueCopy()
		},
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
	snapshot, err := settings.New(selected, func(value postgres.Settings) postgres.Settings { return value })
	if err != nil {
		t.Fatal(err)
	}
	update, err := runtime.Resources().Apply(context.Background(), snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	client, err := postgres.Using(context.Background(), ref, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func persistenceMySQL(t *testing.T, selected mysql.Settings) *mysql.Client {
	t.Helper()
	selected.MaxConnections = 4
	selected.MaxArgumentBytes = 8 << 20
	selected.MaxPacketBytes = 8 << 20
	selected.MaxResultBytes = 16 << 20
	selected.MaxResponseBytes = 32 << 20
	policy, err := mysql.Recommend(selected)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := framework.New(context.Background(), framework.Options{Operations: policy.Runtime})
	if err != nil {
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil || !runtime.Resources().ShutdownComplete() {
			t.Error("resource cleanup incomplete", err)
		}
		if err := receiver.Finish(ctx); err != nil {
			t.Error("required evidence not drained", err)
		}
		if usage, err := runtime.Operations().Inspect(); err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
			t.Error("retained operation budget", err)
		}
	})
	dependencies := mysql.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	ref, err := resource.Bind(runtime.Resources(), resource.Binding[mysql.Settings, mysql.Handle]{
		Name: "catalog-target", Policy: resource.Fixed, Clone: func(value mysql.Settings) mysql.Settings { return value },
		Select: func(view settings.View) (mysql.Settings, error) {
			snapshot, err := settings.As[mysql.Settings](view)
			if err != nil {
				return mysql.Settings{}, err
			}
			return snapshot.ValueCopy()
		},
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
	snapshot, err := settings.New(selected, func(value mysql.Settings) mysql.Settings { return value })
	if err != nil {
		t.Fatal(err)
	}
	update, err := runtime.Resources().Apply(context.Background(), snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	client, err := mysql.Using(context.Background(), ref, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func persistenceService(t *testing.T, dialect string) *persistenceFixture {
	return persistenceTarget(t, dialect, true)
}
func persistenceTarget(t *testing.T, dialect string, install bool) *persistenceFixture {
	t.Helper()
	value := &persistenceFixture{dialect: dialect}
	var entropy [8]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		t.Fatal(err)
	}
	value.target = "catalog_publication_" + hex.EncodeToString(entropy[:])
	value.quoted, _ = publicationTarget(dialect, value.target)
	if dialect == "postgresql" {
		var selected postgres.Settings
		loadPersistenceSettings(t, dialect, &selected)
		if selected.Address != "127.0.0.1" || selected.Database != "catalog_test" || !selected.Plaintext {
			t.Fatal("only isolated fixture admitted")
		}
		value.pg = persistencePostgres(t, selected)
		if !strings.HasPrefix(value.scalar(t, "SHOW server_version"), "18.") || value.scalar(t, "SHOW server_encoding") != "UTF8" {
			t.Fatal("PostgreSQL profile mismatch")
		}
		value.exec(t, "CREATE SCHEMA "+value.quoted)
		t.Cleanup(func() { value.exec(t, "DROP SCHEMA "+value.quoted+" CASCADE") })
		selected.ParserHome = os.Getenv("HOME")
		installPersistence(t, dialect, value.target, selected, install)
	} else {
		var selected mysql.Settings
		loadPersistenceSettings(t, dialect, &selected)
		if selected.Address != "127.0.0.1" || selected.Database != "catalog_test" || !selected.Plaintext {
			t.Fatal("only isolated fixture admitted")
		}
		admin := &persistenceFixture{dialect: dialect, my: persistenceMySQL(t, selected)}
		if !strings.HasPrefix(admin.scalar(t, "SELECT VERSION()"), "8.4.") || admin.scalar(t, "SELECT @@innodb_page_size") != "16384" {
			t.Fatal("MySQL profile mismatch")
		}
		admin.exec(t, "CREATE DATABASE "+value.quoted+" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin")
		t.Cleanup(func() { admin.exec(t, "DROP DATABASE "+value.quoted) })
		selected.Database = value.target
		value.my = persistenceMySQL(t, selected)
		installPersistence(t, dialect, value.target, selected, install)
	}
	return value
}
func (fixture *persistenceFixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var err error
	if fixture.pg != nil {
		_, err = fixture.pg.Exec(ctx, sql, args...)
	} else {
		_, err = fixture.my.Exec(ctx, sql, args...)
	}
	if err != nil {
		t.Fatal("fixture SQL failed", err)
	}
}
func (fixture *persistenceFixture) rows(t *testing.T, sql string, args ...any) [][][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows := [][][]byte{}
	if fixture.pg != nil {
		result, err := fixture.pg.Query(ctx, sql, args...)
		if err != nil || !result.Complete() {
			t.Fatal("fixture query failed", err)
		}
		for _, row := range result.RowsCopy() {
			rows = append(rows, row.ValuesCopy())
		}
	} else {
		result, err := fixture.my.Query(ctx, sql, args...)
		if err != nil || !result.Complete() {
			t.Fatal("fixture query failed", err)
		}
		for _, row := range result.RowsCopy() {
			rows = append(rows, row.ValuesCopy())
		}
	}
	return rows
}
func (fixture *persistenceFixture) scalar(t *testing.T, sql string, args ...any) string {
	t.Helper()
	rows := fixture.rows(t, sql, args...)
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] == nil {
		t.Fatal("missing scalar")
	}
	return string(rows[0][0])
}
func (fixture *persistenceFixture) table(name string) string { return fixture.quoted + "." + name }
func (fixture *persistenceFixture) params(count int) string {
	tx := &publicationTx{pg: new(postgres.Transaction)}
	if fixture.pg == nil {
		tx.pg = nil
	}
	return tx.parameters(count)
}
func (fixture *persistenceFixture) begin(ctx context.Context) (*publicationTx, error) {
	if fixture.pg != nil {
		tx, err := fixture.pg.Begin(ctx, postgres.TxOptions{Isolation: postgres.ReadCommitted, Access: postgres.ReadWrite})
		if err != nil {
			return nil, err
		}
		return postgresPublication(tx, fixture.target)
	}
	tx, err := fixture.my.Begin(ctx, mysql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	return mysqlPublication(tx, fixture.target)
}
func finishPublication(ctx context.Context, tx *publicationTx, commit bool) error {
	result, err := tx.finish(ctx, commit)
	wanted := "rollback-acknowledged"
	if commit {
		wanted = "commit-acknowledged"
	}
	if err != nil || !result.released || result.acknowledgement != wanted {
		return errors.Join(err, fmt.Errorf("publication finalization not acknowledged: %s", result.acknowledgement))
	}
	if result.source.Name != "catalog-target" || result.source.Generation == 0 {
		return errors.New("transaction bypassed resource generation")
	}
	return nil
}
func (fixture *persistenceFixture) publish(prepared *PreparedRegistration) (publicationChanges, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	tx, err := fixture.begin(ctx)
	if err != nil {
		return publicationChanges{}, err
	}
	changes, err := tx.register(ctx, prepared)
	if diagnostic, ok := Diagnose(err); ok {
		err = fmt.Errorf("%s: %w", diagnostic.Reason, err)
	}
	cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	final := finishPublication(cleanup, tx, err == nil)
	return changes, errors.Join(err, final)
}
func (fixture *persistenceFixture) snapshot(t *testing.T) map[string][][][]byte {
	t.Helper()
	result := map[string][][][]byte{}
	for _, table := range []string{"workflow_definition", "workflow_version", "workflow_mode", "workflow_mode_version", "workflow_category", "i18n_resource", "i18n_locale", "i18n_text", "workflow_definition_tag"} {
		order := "id"
		if table == "workflow_definition_tag" {
			order = "workflow_definition_id,tag_key"
		}
		result[table] = fixture.rows(t, "SELECT * FROM "+fixture.table(table)+" ORDER BY "+order)
	}
	return result
}
func preparedPublication(t *testing.T, request Registration) *PreparedRegistration {
	t.Helper()
	prepared, err := Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

func TestPublicationInitialAndNoOpService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			request := registrationInput(t)
			request.Category = &Category{Code: "operations", Name: &Text{English: "Operations"}}
			request.Tags = map[string]string{"team": "initial"}
			prepared := preparedPublication(t, request)
			result, err := fixture.publish(prepared)
			if err != nil || !result.definitionCreated || result.definitionID < 1 || result.versions != 1 || result.modes != 2 || result.revisions != 2 || result.categories != 1 || result.tags != 1 {
				t.Fatal("initial registration failed", result, err)
			}
			for _, table := range []string{"workflow_definition", "workflow_version", "workflow_mode", "workflow_mode_version"} {
				if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table(table)+" WHERE enabled <> FALSE") != "0" {
					t.Fatal("registration enabled execution")
				}
			}
			before := fixture.snapshot(t)
			repeated, err := fixture.publish(prepared)
			if err != nil || repeated.changed() || repeated.definitionID != result.definitionID || !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("equal registration changed persisted state", repeated, err)
			}

			fixture.exec(t, "INSERT INTO "+fixture.table("workflow_category")+" (namespace,code) VALUES ("+fixture.params(2)+")", "example", "operator")
			next := fixture.scalar(t, "SELECT id FROM "+fixture.table("workflow_category")+" WHERE code='operator'")
			fixture.exec(t, "UPDATE "+fixture.table("workflow_definition")+" SET category_id="+next+",enabled=TRUE,row_revision=row_revision+1,updated_at=CURRENT_TIMESTAMP")
			fixture.exec(t, "UPDATE "+fixture.table("workflow_version")+" SET enabled=TRUE,row_revision=row_revision+1,updated_at=CURRENT_TIMESTAMP")
			fixture.exec(t, "UPDATE "+fixture.table("workflow_mode")+" SET enabled=TRUE,row_revision=row_revision+1,updated_at=CURRENT_TIMESTAMP")
			fixture.exec(t, "UPDATE "+fixture.table("workflow_mode_version")+" SET enabled=TRUE,row_revision=row_revision+1,updated_at=CURRENT_TIMESTAMP")
			versionID := fixture.scalar(t, "SELECT id FROM "+fixture.table("workflow_version")+" WHERE version_code='1'")
			modeID := fixture.scalar(t, "SELECT id FROM "+fixture.table("workflow_mode")+" WHERE code='standard'")
			revisionID := fixture.scalar(t, "SELECT id FROM "+fixture.table("workflow_mode_version")+" WHERE workflow_mode_id="+modeID)
			fixture.exec(t, "UPDATE "+fixture.table("workflow_definition")+" SET default_version_id="+versionID+",row_revision=row_revision+1")
			fixture.exec(t, "UPDATE "+fixture.table("workflow_version")+" SET default_mode_id="+modeID+",row_revision=row_revision+1")
			fixture.exec(t, "UPDATE "+fixture.table("workflow_mode")+" SET default_mode_version_id="+revisionID+",row_revision=row_revision+1 WHERE id="+modeID)
			fixture.exec(t, "DELETE FROM "+fixture.table("workflow_category")+" WHERE code='operations'")
			fixture.exec(t, "UPDATE "+fixture.table("workflow_definition_tag")+" SET tag_value='operator'")
			fixture.exec(t, "INSERT INTO "+fixture.table("i18n_locale")+" (code,native_name) VALUES ('zh-CN','中文')")
			resourceID := fixture.scalar(t, "SELECT id FROM "+fixture.table("i18n_resource")+" WHERE scope='workflow.definition'")
			localeID := fixture.scalar(t, "SELECT id FROM "+fixture.table("i18n_locale")+" WHERE code='zh-CN'")
			fixture.exec(t, "INSERT INTO "+fixture.table("i18n_text")+" (resource_id,locale_id,value) VALUES ("+fixture.params(3)+")", resourceID, localeID, "已翻译")
			before = fixture.snapshot(t)
			repeated, err = fixture.publish(prepared)
			if err != nil || repeated.changed() || !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("repeat reset management, tags, translations or old category", err)
			}
		})
	}
}
func TestPublicationConflictRollsBackService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			initial := registrationInput(t)
			prepared := preparedPublication(t, initial)
			if _, err := fixture.publish(prepared); err != nil {
				t.Fatal(err)
			}
			before := fixture.snapshot(t)
			changed := health.Declarations()
			changed.Workflows[0].Modes[0].Values = map[string]declaration.Decimal{"attempt_timeout": "1200"}
			if _, err := fixture.publish(preparedPublication(t, authored(t, changed))); !errors.Is(err, ErrConflict) {
				t.Fatal("changed immutable mode accepted", err)
			}
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("conflict escaped rollback")
			}
			mixed := health.Declarations()
			next := health.Declarations().Workflows[0]
			next.Version = "2"
			mixed.Workflows = append(mixed.Workflows, next)
			request := authored(t, mixed)
			request.Resources[0].Text.English = "Changed"
			if _, err := fixture.publish(preparedPublication(t, request)); !errors.Is(err, ErrConflict) {
				t.Fatal("selected English conflict accepted", err)
			}
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("new version survived metadata conflict")
			}
			request = authored(t, mixed)
			changes, err := fixture.publish(preparedPublication(t, request))
			if err != nil || changes.versions != 1 || changes.revisions != 2 {
				t.Fatal("equal content under another label collapsed", changes, err)
			}
		})
	}
}
func TestPublicationCategoryAndIntegrityRefusalService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			request := registrationInput(t)
			request.Category = &Category{Code: "missing"}
			before := fixture.snapshot(t)
			if _, err := fixture.publish(preparedPublication(t, request)); !errors.Is(err, ErrCategory) {
				t.Fatal("category typo created a category", err)
			}
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("failed initial association left a definition")
			}
			fixture.exec(t, "INSERT INTO "+fixture.table("workflow_category")+" (namespace,code,enabled) VALUES ('example','missing',FALSE)")
			request.Category.Name = &Text{English: "Missing"}
			before = fixture.snapshot(t)
			if _, err := fixture.publish(preparedPublication(t, request)); !errors.Is(err, ErrCategory) {
				t.Fatal("disabled category accepted", err)
			}
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("disabled association left partial state")
			}
			request.Category = nil
			prepared := preparedPublication(t, request)
			initial, err := fixture.publish(prepared)
			if err != nil {
				t.Fatal(err)
			}
			content := prepared.state.contents[0]
			fixture.exec(t, "INSERT INTO "+fixture.table("workflow_version")+" (workflow_definition_id,version_code,spec_format,fingerprint_profile,spec_sha256,spec_document) VALUES ("+fixture.params(6)+")", initial.definitionID, "corrupt", declaration.SpecFormat, declaration.SpecProfile, make([]byte, 32), content.Bytes)
			declared := health.Declarations()
			declared.Workflows[0].Version = "corrupt"
			if _, err := fixture.publish(preparedPublication(t, authored(t, declared))); !errors.Is(err, ErrStored) {
				t.Fatal("corrupt stored digest treated as a content conflict/equality", err)
			}
			fixture.exec(t, "DELETE FROM "+fixture.table("i18n_text")+" WHERE resource_id=(SELECT id FROM "+fixture.table("i18n_resource")+" WHERE scope='workflow.definition')")
			before = fixture.snapshot(t)
			if _, err := fixture.publish(prepared); !errors.Is(err, ErrStored) {
				t.Fatal("missing English silently repaired", err)
			}
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("integrity refusal changed state")
			}
		})
	}
}
func TestPublicationConcurrentCreatorsService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		for _, conflicting := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/conflict=%t", dialect, conflicting), func(t *testing.T) {
				fixture := persistenceService(t, dialect)
				first := registrationInput(t)
				second := registrationInput(t)
				if conflicting {
					second.Resources[0].Text.Context = "different publisher context"
				}
				requests := []*PreparedRegistration{preparedPublication(t, first), preparedPublication(t, second)}
				start := make(chan struct{})
				var workers sync.WaitGroup
				results := make([]publicationChanges, 2)
				failures := make([]error, 2)
				for index := range requests {
					workers.Go(func() { <-start; results[index], failures[index] = fixture.publish(requests[index]) })
				}
				close(start)
				workers.Wait()
				created, unchanged, refused := 0, 0, 0
				for index, err := range failures {
					if err == nil {
						if results[index].definitionCreated {
							created++
						} else {
							unchanged++
						}
					} else if errors.Is(err, ErrConflict) {
						refused++
					} else {
						t.Fatal("unexpected creator failure", err)
					}
				}
				if created != 1 || conflicting && (refused != 1 || unchanged != 0) || !conflicting && (unchanged != 1 || refused != 0) {
					t.Fatal("concurrent registration was not create/compare", created, unchanged, refused)
				}
				if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "1" || fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_mode_version")) != "2" {
					t.Fatal("duplicate entities persisted")
				}
			})
		}
	}
}
func TestPublicationCopiesRetainedContentService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			prepared := preparedPublication(t, registrationInput(t))
			if _, err := fixture.publish(prepared); err != nil {
				t.Fatal(err)
			}
			row := fixture.rows(t, "SELECT spec_document FROM "+fixture.table("workflow_version"))[0][0]
			tx := &publicationTx{}
			if dialect == "postgresql" {
				tx.pg = new(postgres.Transaction)
			}
			raw, err := tx.binary(row, declaration.MaxSpecBytes)
			if err != nil || !bytes.Equal(raw, prepared.state.contents[0].Bytes) {
				t.Fatal("stored canonical bytes differ", err)
			}
		})
	}
}

func TestPublicationSharedCategoryService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		for _, conflicting := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/conflict=%t", dialect, conflicting), func(t *testing.T) {
				fixture := persistenceService(t, dialect)
				prepared := make([]*PreparedRegistration, 2)
				for index := range prepared {
					input := health.Declarations()
					input.Workflows[0].Code = "publisher-" + string(rune('a'+index))
					request := authored(t, input)
					name := "Shared"
					if conflicting && index == 1 {
						name = "Different"
					}
					request.Category = &Category{Code: "shared", Name: &Text{English: name}}
					prepared[index] = preparedPublication(t, request)
				}
				outcomes := make([]publicationChanges, 2)
				problems := make([]error, 2)
				start := make(chan struct{})
				var workers sync.WaitGroup
				for index := range prepared {
					workers.Go(func() { <-start; outcomes[index], problems[index] = fixture.publish(prepared[index]) })
				}
				close(start)
				workers.Wait()
				success, refused := 0, 0
				for _, problem := range problems {
					if problem == nil {
						success++
					} else if errors.Is(problem, ErrConflict) {
						refused++
					} else {
						t.Fatal("unexpected shared publisher failure", problem)
					}
				}
				expected := 2
				if conflicting {
					expected = 1
				}
				if success != expected || refused != 2-expected || fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_category")) != "1" ||
					fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != fmt.Sprint(expected) {
					t.Fatal("shared category publication was not atomic", success, refused)
				}
			})
		}
	}
}

func (fixture *persistenceFixture) waitCategoryLock(t *testing.T) {
	t.Helper()
	query := "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%" + fixture.target + "%.workflow_category%'"
	if fixture.my != nil {
		query = "SELECT count(*) FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l ON w.REQUESTING_ENGINE_LOCK_ID=l.ENGINE_LOCK_ID WHERE l.OBJECT_SCHEMA='" + fixture.target + "' AND l.OBJECT_NAME='workflow_category'"
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fixture.scalar(t, query) != "0" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("category-lock contention was not observed")
}
func TestPublicationCategoryDisableRaceService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		for _, disableFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disable_first=%t", dialect, disableFirst), func(t *testing.T) {
				fixture := persistenceService(t, dialect)
				fixture.exec(t, "INSERT INTO "+fixture.table("workflow_category")+" (namespace,code) VALUES ('example','shared')")
				input := registrationInput(t)
				input.Category = &Category{Code: "shared"}
				prepared := preparedPublication(t, input)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				first, err := fixture.begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				finalized := false
				defer func() {
					if !finalized {
						cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						_ = finishPublication(cleanup, first, false)
					}
				}()
				disable := "UPDATE " + fixture.table("workflow_category") + " SET enabled=FALSE,row_revision=row_revision+1,updated_at=CURRENT_TIMESTAMP WHERE code='shared'"
				if disableFirst {
					if _, err := first.exec(ctx, disable); err != nil {
						t.Fatal(err)
					}
				} else {
					result, err := first.register(ctx, prepared)
					if err != nil || !result.definitionCreated {
						t.Fatal("publication failed before management race", err)
					}
				}
				done := make(chan error, 1)
				go func() {
					if disableFirst {
						_, err := fixture.publish(prepared)
						done <- err
						return
					}
					transaction, err := fixture.begin(ctx)
					if err != nil {
						done <- err
						return
					}
					_, err = transaction.exec(ctx, disable)
					wait, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					done <- errors.Join(err, finishPublication(wait, transaction, err == nil))
				}()
				fixture.waitCategoryLock(t)
				if err := finishPublication(ctx, first, true); err != nil {
					t.Fatal(err)
				}
				finalized = true
				select {
				case err := <-done:
					if disableFirst && !errors.Is(err, ErrCategory) || !disableFirst && err != nil {
						t.Fatal("category disable race violated eligibility", err)
					}
				case <-ctx.Done():
					t.Fatal("category contender did not finish")
				}
				expected := "1"
				if disableFirst {
					expected = "0"
				}
				if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != expected {
					t.Fatal("wrong association after disable ordering")
				}
			})
		}
	}
}

func TestPublicationCancellationAndSingleUseService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			prepared := preparedPublication(t, registrationInput(t))
			ctx, cancel := context.WithCancel(context.Background())
			tx, err := fixture.begin(ctx)
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			result, err := tx.register(ctx, prepared)
			if err != nil || !result.changed() {
				cancel()
				t.Fatal(err)
			}
			if _, err := tx.register(ctx, prepared); !errors.Is(err, ErrHandle) {
				t.Fatal("kernel replayed on one transaction")
			}
			cancel()
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			if tx.pg != nil {
				if err := finishPublication(cleanup, tx, false); err != nil {
					t.Fatal(err)
				}
			} else {
				_, _ = tx.my.Rollback(cleanup)
				final, err := tx.my.Receipt().WaitReleased(cleanup)
				if err != nil || !final.Info().Released {
					t.Fatal("canceled transaction not joined", err)
				}
				outcome, present := final.ValueCopy()
				if !present || outcome.TransactionOutcome() != mysql.RollbackAcknowledged {
					t.Fatal("cancellation lacked rollback evidence")
				}
			}
			if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "0" {
				t.Fatal("canceled registration committed")
			}
		})
	}
}

func maximumCatalogSpecification(t *testing.T) declaration.Declaration {
	t.Helper()
	declared := health.Declarations()
	minimum, maximum := int64(0), int64(1024)
	choices := make([]string, 256)
	for index := range choices {
		choices[index] = fmt.Sprintf("%04d", index) + strings.Repeat("x", 1020)
	}
	properties := map[string]declaration.DataSchema{}
	for index := range 15 {
		properties[fmt.Sprintf("field%02d", index)] = declaration.DataSchema{Type: "string", MinLength: &minimum, MaxLength: &maximum, Enum: choices}
	}
	tail := declaration.DataSchema{Type: "string", MinLength: &minimum, MaxLength: &maximum, Enum: []string{"0000"}}
	properties["field15"] = tail
	declared.Workflows[0].Spec.Contracts["target"] = declaration.DataSchema{Type: "object", Properties: properties}
	baseline := authored(t, declared)
	contents, err := baseline.Bundle.ContentsCopy()
	if err != nil {
		t.Fatal(err)
	}
	remaining := declaration.MaxSpecBytes - len(contents[0].Bytes)
	if remaining < 0 {
		t.Fatal("exact-cap fixture baseline changed")
	}
	for remaining >= 1027 {
		tail.Enum = append(tail.Enum, fmt.Sprintf("%04d", len(tail.Enum))+strings.Repeat("x", 1020))
		remaining -= 1027
	}
	if remaining > 1020 {
		tail.Enum = append(tail.Enum, fmt.Sprintf("%04d", len(tail.Enum)))
		remaining -= 7
	}
	tail.Enum[0] += strings.Repeat("x", remaining)
	properties["field15"] = tail
	return declared
}

func TestPublicationExactDocumentLimitService(t *testing.T) {
	declared := maximumCatalogSpecification(t)
	prepared := preparedPublication(t, authored(t, declared))
	if len(prepared.state.contents[0].Bytes) != declaration.MaxSpecBytes {
		t.Fatal("fixture is not at exact document boundary")
	}
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			value := fixture.catalog(t, Options{})
			initial, err := value.Register(context.Background(), prepared)
			if err != nil || !initial.Changes.DefinitionCreated || initial.Acknowledgement != CommitAcknowledged {
				t.Fatal("full-size registration failed", err)
			}
			repeat, err := value.Register(context.Background(), prepared)
			if err != nil || repeat.Effect != Unchanged || repeat.DefinitionID != initial.DefinitionID {
				t.Fatal("full-size comparison changed data", err)
			}
			view, err := value.Read(context.Background(), ReadRequest{Identity: declaration.Identity{Namespace: "example", Code: "server-health", Version: "1"}, Document: true})
			if err != nil || len(view.Documents) != 1 || !bytes.Equal(view.Documents[0].Canonical, prepared.state.contents[0].Bytes) {
				t.Fatal("full-size document read differs", err)
			}
			comparison, err := value.Compare(context.Background(), prepared)
			if err != nil || !comparison.Equal {
				t.Fatal("full-size comparison did not verify stored bytes", err)
			}
		})
	}
}

func TestPublicationMaximumResourceCountService(t *testing.T) {
	declared := declaration.Declaration{}
	for index := range declaration.MaxWorkflows {
		workflow := health.Declarations().Workflows[0]
		workflow.Version = fmt.Sprint(index)
		workflow.Modes = append(workflow.Modes, declaration.ModeDeclaration{Code: "third", Revision: "1"}, declaration.ModeDeclaration{Code: "fourth", Revision: "1"})
		node := workflow.Spec.Nodes["sample"]
		for activity := range 30 {
			name := fmt.Sprintf("probe%02d", activity)
			node.Activities[name] = node.Activities["cpu"]
			node.Phases[0] = append(node.Phases[0], name)
		}
		workflow.Spec.Nodes["sample"] = node
		declared.Workflows = append(declared.Workflows, workflow)
	}
	request := authored(t, declared)
	request.Category = &Category{Code: "capacity", Name: &Text{English: "Capacity"}}
	request.Tags = map[string]string{}
	for index := range MaxTags {
		request.Tags[fmt.Sprint(index)] = "capacity"
	}
	for _, workflow := range declared.Workflows {
		for activity := range 30 {
			for _, field := range []Field{Name, Description} {
				if len(request.Resources) == MaxResources-1 {
					break
				}
				request.Resources = append(request.Resources, Resource{
					Target: Target{Kind: Activity, Version: workflow.Version, Node: "sample", Activity: fmt.Sprintf("probe%02d", activity)},
					Field:  field, Text: Text{English: "Probe"},
				})
			}
		}
	}
	prepared := preparedPublication(t, request)
	info, err := prepared.Inspect()
	if err != nil || info.Resources != MaxResources || info.Versions != declaration.MaxWorkflows || info.Modes != declaration.MaxModes {
		t.Fatal("fixture misses admitted count boundaries", info, err)
	}
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			value := fixture.catalog(t, Options{})
			initial, err := value.Register(context.Background(), prepared)
			if err != nil || !initial.Changes.DefinitionCreated || initial.Changes.Resources != MaxResources || initial.Changes.Texts != MaxResources || initial.Changes.Tags != MaxTags {
				t.Fatal("admitted maximum resources could not be registered", initial, err)
			}
			repeat, err := value.Register(context.Background(), prepared)
			if err != nil || repeat.Effect != Unchanged || repeat.DefinitionID != initial.DefinitionID {
				t.Fatal("admitted maximum resources could not be compared", err)
			}
			reader := fixture.comparisonCatalog(t)
			equal, err := reader.Compare(context.Background(), prepared)
			if err != nil || !equal.Equal || equal.Resources != MaxResources-1 || equal.Matched != equal.Contents+equal.Resources {
				t.Fatal("full selected metadata anchor did not fit", err)
			}
			fixture.exec(t, "UPDATE "+fixture.table("i18n_text")+" SET value='Different'")
			different, err := reader.Compare(context.Background(), prepared)
			if err != nil || different.Different != MaxResources-1 || len(different.Problems) != MaxComparisonProblems || !different.Truncated {
				t.Fatal("bounded diagnostics lost total differences", err)
			}
			last := prepared.state.resources[len(prepared.state.resources)-1]
			markers := strings.Split(fixture.params(2), ",")
			resourceID := fixture.scalar(t, "SELECT id FROM "+fixture.table("i18n_resource")+" WHERE scope="+markers[0]+" AND code="+markers[1], last.scope, last.key)
			fixture.exec(t, "DELETE FROM "+fixture.table("i18n_text")+" WHERE resource_id="+resourceID)
			failed, err := reader.Compare(context.Background(), prepared)
			if !errors.Is(err, ErrStored) || failed.Complete {
				t.Fatal("truncated diagnostics skipped later integrity failure", err)
			}
		})
	}
}

func TestPublicationSchemaRefusalService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		for _, damage := range []string{"empty", "history_missing", "history_empty", "incomplete", "revision", "checksum", "column", "guard", "english"} {
			t.Run(dialect+"/"+damage, func(t *testing.T) {
				fixture := persistenceTarget(t, dialect, damage != "empty")
				if damage == "empty" {
					if _, err := fixture.publish(preparedPublication(t, registrationInput(t))); !errors.Is(err, ErrSchema) {
						t.Fatal("uninstalled schema admitted", err)
					}
					query := "SELECT count(*) FROM information_schema.tables WHERE table_schema=" + fixture.params(1)
					if fixture.scalar(t, query, fixture.target) != "0" {
						t.Fatal("registration installed tables")
					}
					return
				}
				switch damage {
				case "history_missing":
					fixture.exec(t, "DROP TABLE "+fixture.table(schemaHistory))
				case "history_empty":
					fixture.exec(t, "DELETE FROM "+fixture.table(schemaHistory))
				case "incomplete":
					fixture.exec(t, "UPDATE "+fixture.table(schemaHistory)+" SET state='intent',acknowledged_at=NULL WHERE step_ordinal=0")
				case "revision":
					fixture.exec(t, "UPDATE "+fixture.table(schemaHistory)+" SET migration_revision=2")
				case "checksum":
					fixture.exec(t, "UPDATE "+fixture.table(schemaHistory)+" SET manifest_sha256="+fixture.params(1), make([]byte, 32))
				case "column":
					fixture.exec(t, "ALTER TABLE "+fixture.table("workflow_definition")+" ADD COLUMN unexpected INTEGER")
				case "guard":
					query := "DROP TRIGGER " + fixture.table("immutable_i18n_locale")
					if dialect == "postgresql" {
						query = "DROP TRIGGER immutable_i18n_locale ON " + fixture.table("i18n_locale")
					}
					fixture.exec(t, query)
				case "english":
					query := "DROP TRIGGER " + fixture.table("protect_english_locale")
					var assets fs.FS = mysqlassets.Assets()
					if dialect == "postgresql" {
						query = "DROP TRIGGER protect_english_locale ON " + fixture.table("i18n_locale")
						assets = pgassets.Assets()
					}
					fixture.exec(t, query)
					fixture.exec(t, "DELETE FROM "+fixture.table("i18n_locale")+" WHERE code='en'")
					guard, err := fs.ReadFile(assets, "0001/guards/english_locale.sql")
					if err != nil {
						t.Fatal(err)
					}
					fixture.exec(t, strings.ReplaceAll(string(guard), "{{target}}", fixture.quoted))
				}
				before := fixture.snapshot(t)
				if _, err := fixture.publish(preparedPublication(t, registrationInput(t))); !errors.Is(err, ErrSchema) {
					t.Fatal("unproven schema admitted", err)
				}
				if !reflect.DeepEqual(before, fixture.snapshot(t)) {
					t.Fatal("schema refusal mutated business rows")
				}
			})
		}
	}
}

func TestPublicationSchemaPinsMetadataService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			tx, err := fixture.begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				_ = finishPublication(cleanup, tx, false)
			}()
			if err := tx.schema(ctx); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				sql := "ALTER TABLE " + fixture.table("workflow_definition") + " ADD COLUMN admission_probe INTEGER"
				var err error
				if fixture.pg != nil {
					_, err = fixture.pg.Exec(ctx, sql)
				} else {
					_, err = fixture.my.Exec(ctx, sql)
				}
				done <- err
			}()
			query := "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%" + fixture.target + "%admission_probe%'"
			if dialect == "mysql" {
				query = "SELECT count(*) FROM performance_schema.metadata_locks WHERE OBJECT_SCHEMA='" + fixture.target + "' AND OBJECT_NAME='workflow_definition' AND LOCK_STATUS='PENDING'"
			}
			observed := false
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
				if fixture.scalar(t, query) != "0" {
					observed = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !observed {
				t.Fatal("DDL did not wait for transaction-owned metadata protection")
			}
			if err := finishPublication(ctx, tx, false); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("DDL did not resume after transaction release")
			}
			if _, err := fixture.publish(preparedPublication(t, registrationInput(t))); !errors.Is(err, ErrSchema) {
				t.Fatal("later transaction reused stale admission", err)
			}
		})
	}
}

func TestPublicationRefusesInstallerContentionService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			physical := fixture.target
			if dialect == "postgresql" {
				physical = fixture.scalar(t, "SELECT current_database()")
			}
			lock := sha256.Sum256([]byte(schemaFamily + "\x00" + physical + "\x00" + fixture.target))
			var release func()
			if fixture.pg != nil {
				blocker, err := fixture.begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := blocker.query(ctx, "SELECT pg_catalog.pg_advisory_xact_lock($1)", int64(binary.BigEndian.Uint64(lock[:8]))); err != nil {
					t.Fatal(err)
				}
				release = func() {
					if err := finishPublication(ctx, blocker, false); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				blocker, err := fixture.my.Retain(ctx, mysql.SessionOptions{Timeout: 20 * time.Second})
				if err != nil {
					t.Fatal(err)
				}
				value, err := blocker.Query(ctx, "SELECT GET_LOCK(?,0)", hex.EncodeToString(lock[:]))
				if err != nil || len(value.RowsCopy()) != 1 || string(value.RowsCopy()[0].ValuesCopy()[0]) != "1" {
					t.Fatal("installer lock not acquired", err)
				}
				release = func() {
					if err := blocker.Close(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			released := false
			defer func() {
				if !released {
					release()
				}
			}()
			before := fixture.snapshot(t)
			prepared := preparedPublication(t, registrationInput(t))
			if _, err := fixture.publish(prepared); !errors.Is(err, ErrSchema) {
				t.Fatal("installer contention admitted", err)
			}
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("contended admission wrote business rows")
			}
			release()
			released = true
			if _, err := fixture.publish(prepared); err != nil {
				t.Fatal("released installer lock still blocks registration", err)
			}
			if dialect == "mysql" && fixture.scalar(t, "SELECT IS_FREE_LOCK(?)", hex.EncodeToString(lock[:])) != "1" {
				t.Fatal("registration leaked a session named lock")
			}
		})
	}
}

type catalogReplyProxy struct {
	listener    net.Listener
	upstream    string
	dialect     string
	match       func([]byte) bool
	commit      bool
	dropped     atomic.Bool
	commits     atomic.Int32
	witness     chan struct{}
	mu          sync.Mutex
	connections map[net.Conn]bool
	workers     sync.WaitGroup
	accepted    chan struct{}
}

func newCatalogReplyProxy(t *testing.T, dialect, upstream string, match func([]byte) bool, commit bool) *catalogReplyProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	value := &catalogReplyProxy{listener: listener, upstream: upstream, dialect: dialect, match: match, commit: commit, witness: make(chan struct{}), connections: map[net.Conn]bool{}, accepted: make(chan struct{})}
	go value.accept()
	t.Cleanup(func() {
		_ = value.listener.Close()
		<-value.accepted
		value.mu.Lock()
		for connection := range value.connections {
			_ = connection.Close()
		}
		value.mu.Unlock()
		done := make(chan struct{})
		go func() { value.workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("fault proxy ownership did not end")
		}
	})
	return value
}
func (proxy *catalogReplyProxy) port() uint16 {
	_, port, _ := net.SplitHostPort(proxy.listener.Addr().String())
	value, _ := strconv.ParseUint(port, 10, 16)
	return uint16(value)
}
func (proxy *catalogReplyProxy) accept() {
	defer close(proxy.accepted)
	for {
		client, err := proxy.listener.Accept()
		if err != nil {
			return
		}
		server, err := net.DialTimeout("tcp", proxy.upstream, 5*time.Second)
		if err != nil {
			_ = client.Close()
			continue
		}
		proxy.mu.Lock()
		proxy.connections[client] = true
		proxy.connections[server] = true
		proxy.mu.Unlock()
		proxy.workers.Add(1)
		go func() {
			defer proxy.workers.Done()
			defer func() {
				proxy.mu.Lock()
				delete(proxy.connections, client)
				delete(proxy.connections, server)
				proxy.mu.Unlock()
			}()
			var armed atomic.Bool
			var workers sync.WaitGroup
			workers.Add(2)
			for _, direction := range []bool{true, false} {
				go func(outbound bool) {
					defer workers.Done()
					defer client.Close()
					defer server.Close()
					source, destination := net.Conn(server), net.Conn(client)
					if outbound {
						source, destination = client, server
					}
					startup := outbound && proxy.dialect == "postgresql"
					for {
						packet, payload, tag, err := readCatalogPacket(source, proxy.dialect, startup)
						startup = false
						if err != nil {
							return
						}
						if outbound && proxy.match != nil && proxy.match(payload) {
							armed.Store(true)
							proxy.commits.Add(1)
						}
						drop := false
						if !outbound {
							if proxy.dialect == "postgresql" {
								drop = tag == 'C' && (proxy.commit && bytes.Equal(payload, []byte("COMMIT\x00")) || armed.Load())
							} else {
								drop = len(payload) > 0 && payload[0] == 0 && armed.Load()
							}
						}
						if drop && proxy.dropped.CompareAndSwap(false, true) {
							close(proxy.witness)
							return
						}
						if _, err = io.Copy(destination, bytes.NewReader(packet)); err != nil {
							return
						}
					}
				}(direction)
			}
			workers.Wait()
		}()
	}
}
func readCatalogPacket(connection net.Conn, dialect string, startup bool) (packet, payload []byte, tag byte, err error) {
	if dialect == "mysql" {
		header := make([]byte, 4)
		if _, err = io.ReadFull(connection, header); err != nil {
			return
		}
		size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		if size > 8<<20 {
			err = errors.New("oversized fixture packet")
			return
		}
		payload = make([]byte, size)
		if _, err = io.ReadFull(connection, payload); err != nil {
			return
		}
		packet = append(header, payload...)
		return
	}
	size := 4
	if !startup {
		size = 5
	}
	header := make([]byte, size)
	if _, err = io.ReadFull(connection, header); err != nil {
		return
	}
	length := int(binary.BigEndian.Uint32(header[size-4:])) - 4
	if length < 0 || length > 8<<20 {
		err = errors.New("invalid fixture frame")
		return
	}
	if !startup {
		tag = header[0]
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(connection, payload); err != nil {
		return
	}
	packet = append(header, payload...)
	return
}

func TestPublicationLostCommitAcknowledgementService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			proxied := &persistenceFixture{dialect: dialect, target: fixture.target, quoted: fixture.quoted}
			match := func(payload []byte) bool {
				if dialect == "mysql" {
					return len(payload) > 1 && payload[0] == 3 && strings.EqualFold(string(payload[1:]), "COMMIT")
				}
				return strings.EqualFold(strings.Trim(string(payload), "\x00"), "COMMIT")
			}
			var proxy *catalogReplyProxy
			if dialect == "postgresql" {
				var selected postgres.Settings
				loadPersistenceSettings(t, dialect, &selected)
				proxy = newCatalogReplyProxy(t, dialect, net.JoinHostPort(selected.Address, strconv.Itoa(int(selected.Port))), match, true)
				selected.Port = proxy.port()
				proxied.pg = persistencePostgres(t, selected)
			} else {
				var selected mysql.Settings
				loadPersistenceSettings(t, dialect, &selected)
				proxy = newCatalogReplyProxy(t, dialect, net.JoinHostPort(selected.Address, strconv.Itoa(int(selected.Port))), match, true)
				selected.Port, selected.Database = proxy.port(), fixture.target
				proxied.my = persistenceMySQL(t, selected)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			tx, err := proxied.begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				_, _ = tx.finish(cleanup, false)
			}()
			prepared := preparedPublication(t, registrationInput(t))
			changes, err := tx.register(ctx, prepared)
			if err != nil || !changes.definitionCreated {
				t.Fatal("fault control never reached commit", err)
			}
			final, err := tx.finish(ctx, true)
			if err == nil || !final.released || final.acknowledgement != "finalization-unknown" || final.source.Generation == 0 {
				t.Fatal("lost commit acknowledgement became success/rollback", final.acknowledgement, final.released, err)
			}
			select {
			case <-proxy.witness:
			default:
				t.Fatal("commit response was not withheld")
			}
			repeated, again := tx.finish(ctx, true)
			if again == nil || repeated.acknowledgement != final.acknowledgement || proxy.commits.Load() != 1 {
				t.Fatal("finalization replayed commit or erased uncertainty", proxy.commits.Load(), again)
			}
			if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "1" {
				t.Fatal("control did not commit at server before dropping acknowledgement")
			}
			retry, err := fixture.publish(prepared)
			if err != nil || retry.changed() {
				t.Fatal("explicit equal retry did not compare existing state", err)
			}
		})
	}
}

func TestPublicationCanceledFinalizationRetainsCleanupService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			lifetime, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			tx, err := fixture.begin(lifetime)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, _ = tx.finish(cleanup, false)
			}()
			if _, err := tx.register(lifetime, preparedPublication(t, registrationInput(t))); err != nil {
				t.Fatal(err)
			}
			expired, cancel := context.WithCancel(context.Background())
			cancel()
			first, err := tx.finish(expired, true)
			if err == nil || first.acknowledgement == "commit-acknowledged" {
				t.Fatal("canceled commit fabricated acknowledgement", err)
			}
			finished, err := tx.finish(lifetime, false)
			if !finished.released || finished.acknowledgement != "rollback-acknowledged" || !errors.Is(err, context.Canceled) {
				t.Fatal("original cleanup/outcome/cancellation was lost", finished.acknowledgement, err)
			}
			if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "0" {
				t.Fatal("canceled commit was replayed")
			}
		})
	}
}

func TestPublicationReadOnlyHistoryPrivilegesService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			var entropy [24]byte
			if _, err := rand.Read(entropy[:]); err != nil {
				t.Fatal(err)
			}
			user := "catalog_" + hex.EncodeToString(entropy[:8])
			business := []string{"workflow_category", "workflow_definition", "workflow_definition_tag", "workflow_version", "workflow_mode", "workflow_mode_version", "i18n_resource", "i18n_text"}
			request := registrationInput(t)
			request.Category = &Category{Code: "permissions", Name: &Text{English: "Permissions"}}
			request.Tags = map[string]string{"fixture": "restricted"}
			prepared := preparedPublication(t, request)
			if dialect == "postgresql" {
				role := "\"" + user + "\""
				fixture.exec(t, "CREATE ROLE "+role)
				t.Cleanup(func() { fixture.exec(t, "DROP OWNED BY "+role); fixture.exec(t, "DROP ROLE "+role) })
				fixture.exec(t, "GRANT USAGE ON SCHEMA "+fixture.quoted+" TO "+role)
				fixture.exec(t, "GRANT SELECT ON ALL TABLES IN SCHEMA "+fixture.quoted+" TO "+role)
				fixture.exec(t, "GRANT USAGE ON ALL SEQUENCES IN SCHEMA "+fixture.quoted+" TO "+role)
				for _, table := range business {
					fixture.exec(t, "GRANT INSERT,UPDATE ON "+fixture.table(table)+" TO "+role)
				}
				for range 2 {
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					tx, err := fixture.begin(ctx)
					if err != nil {
						cancel()
						t.Fatal(err)
					}
					if _, err := tx.exec(ctx, "SET LOCAL ROLE "+role); err != nil {
						cancel()
						t.Fatal(err)
					}
					for _, table := range []string{schemaHistory, "i18n_locale"} {
						value, err := tx.schemaScalar(ctx, "SELECT pg_catalog.has_table_privilege(current_user,$1,'UPDATE')", fixture.target+"."+table)
						if err != nil || value != "f" {
							cancel()
							t.Fatal("control retained protected-table writes", err)
						}
					}
					_, err = tx.register(ctx, prepared)
					cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
					final := finishPublication(cleanup, tx, err == nil)
					stop()
					cancel()
					if err != nil || final != nil {
						t.Fatal("publication required history/locale mutation rights", err, final)
					}
				}
			} else {
				password := hex.EncodeToString(entropy[:])
				account := "'" + user + "'@'%'"
				fixture.exec(t, "CREATE USER "+account+" IDENTIFIED BY '"+password+"'")
				t.Cleanup(func() { fixture.exec(t, "DROP USER "+account) })
				fixture.exec(t, "GRANT SELECT ON "+fixture.quoted+".* TO "+account)
				for _, table := range business {
					fixture.exec(t, "GRANT INSERT,UPDATE ON "+fixture.table(table)+" TO "+account)
				}
				var selected mysql.Settings
				loadPersistenceSettings(t, dialect, &selected)
				selected.Database, selected.User, selected.Password = fixture.target, user, password
				writer := &persistenceFixture{dialect: dialect, target: fixture.target, quoted: fixture.quoted, my: persistenceMySQL(t, selected)}
				if _, err := writer.publish(prepared); !errors.Is(err, ErrSchema) {
					t.Fatal("invisible trigger metadata admitted", err)
				}
				if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "0" {
					t.Fatal("invisible metadata allowed mutation")
				}
				fixture.exec(t, "GRANT TRIGGER ON "+fixture.quoted+".* TO "+account)
				writer.my = persistenceMySQL(t, selected)
				for range 2 {
					if _, err := writer.publish(prepared); err != nil {
						t.Fatal("read-only history publication failed", err)
					}
				}
			}
		})
	}
}

func (fixture *persistenceFixture) catalog(t *testing.T, options Options) *Catalog {
	t.Helper()
	var value *Catalog
	var err error
	if fixture.pg != nil {
		value, err = PostgreSQL(fixture.pg, fixture.target, options)
	} else {
		value, err = MySQL(fixture.my, fixture.target, options)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := value.Close(ctx); err != nil || !value.Released() {
			t.Error("catalog ownership did not settle", err)
		}
	})
	return value
}

func TestCatalogReadService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			value := fixture.catalog(t, Options{})
			ctx := context.Background()
			namespace := declaration.Identity{Namespace: "example"}
			empty, err := value.List(ctx, ListRequest{Parent: namespace})
			if err != nil || len(empty.Items) != 0 {
				t.Fatal("empty compatible namespace", err)
			}
			identity := declaration.Identity{Namespace: "example", Code: "server-health"}
			if _, err := value.Read(ctx, ReadRequest{Identity: identity}); !errors.Is(err, ErrMissing) {
				t.Fatal("missing definition was empty success", err)
			}
			request := registrationInput(t)
			request.Category = &Category{Code: "group", Name: &Text{English: "Group"}}
			request.Tags = map[string]string{"team": "one", "zone": "two"}
			request.Resources = append(request.Resources, Resource{Target: Target{Kind: Definition}, Field: Description, Text: Text{English: ""}})
			published, err := value.Register(ctx, preparedPublication(t, request))
			if err != nil {
				t.Fatal(err)
			}
			page, err := value.List(ctx, ListRequest{Parent: namespace, Locale: "zh-cn"})
			if err != nil || len(page.Items) != 1 {
				if diag, ok := Diagnose(err); ok {
					t.Log(diag)
				}
				if native, ok := postgres.InspectError(err); ok {
					t.Log("SQLSTATE", native.Code, "position", native.Position)
				}
				if native, ok := mysql.InspectError(err); ok {
					t.Log("MySQL code", native.Number)
				}
				t.Fatal("list after registration", err)
			}
			item := page.Items[0]
			if item.ID != published.DefinitionID || item.Enabled || item.DefaultID != "" || item.Category == nil || item.Category.Code != "group" || item.Display.Name.Fallback != "unknown_locale" || !item.Display.Description.Present || item.Display.Description.Value != "" {
				t.Fatal("definition metadata/absence differs")
			}
			view, err := value.Read(ctx, ReadRequest{Identity: identity})
			if err != nil || len(view.Tags) != 2 || view.Tags[0].Key != "team" || len(view.Documents) != 0 {
				t.Fatal("definition read differs", err)
			}
			fixture.exec(t, "INSERT INTO "+fixture.table("i18n_locale")+" (code,native_name) VALUES ('zh-CN','简体中文')")
			localeID := fixture.scalar(t, "SELECT id FROM "+fixture.table("i18n_locale")+" WHERE code='zh-CN'")
			for _, field := range []string{"name", "description"} {
				key, _ := resourceKey("example", "server-health", field)
				resourceID := fixture.scalar(t, "SELECT id FROM "+fixture.table("i18n_resource")+" WHERE scope='workflow.definition' AND code="+fixture.params(1), key)
				text := ""
				if field == "name" {
					text = "服务器健康"
				}
				fixture.exec(t, "INSERT INTO "+fixture.table("i18n_text")+" (resource_id,locale_id,value) VALUES ("+fixture.params(3)+")", resourceID, localeID, text)
			}
			translated, err := value.Read(ctx, ReadRequest{Identity: identity, Locale: "zh-cn"})
			if err != nil || translated.Entity.Display.Name.Value != "服务器健康" || translated.Entity.Display.Name.Locale != "zh-CN" || translated.Entity.Display.Name.Fallback != "none" || !translated.Entity.Display.Description.Present || translated.Entity.Display.Description.Value != "" || translated.Entity.Display.Description.Locale != "zh-CN" {
				t.Fatal("exact translation/empty description differs", err)
			}
			extension, err := value.Read(ctx, ReadRequest{Identity: identity, Locale: "zh-CN-u-ca-chinese"})
			if err != nil || extension.Entity.Display.Name.Locale != "en" || extension.Entity.Display.Name.Fallback != "unknown_locale" {
				t.Fatal("Unicode extension silently negotiated", err)
			}
			fixture.exec(t, "UPDATE "+fixture.table("i18n_locale")+" SET enabled=FALSE WHERE code='zh-CN'")
			disabled, err := value.Read(ctx, ReadRequest{Identity: identity, Locale: "zh-CN"})
			if err != nil || disabled.Entity.Display.Name.Fallback != "disabled_locale" {
				t.Fatal("disabled locale selected", err)
			}
			versions, err := value.List(ctx, ListRequest{Parent: identity})
			if err != nil || len(versions.Items) != 1 || versions.Items[0].Identity.Version != "1" || versions.Items[0].DocumentVerified {
				t.Fatal("version metadata query differs", err)
			}
			identity.Version = "1"
			modes, err := value.List(ctx, ListRequest{Parent: identity})
			if err != nil || len(modes.Items) != 2 || modes.Items[0].Identity.Mode != "patient" {
				t.Fatal("mode ordering differs", err)
			}
			identity.Mode = "patient"
			revisions, err := value.List(ctx, ListRequest{Parent: identity})
			if err != nil || len(revisions.Items) != 1 || revisions.Items[0].Identity.Revision != "1" {
				t.Fatal("revision query differs", err)
			}
			identity.Revision = "1"
			content, err := value.Read(ctx, ReadRequest{Identity: identity, Document: true})
			if err != nil || !content.Entity.DocumentVerified || len(content.Documents) != 2 {
				t.Fatal("selected parent/revision not verified", err)
			}
			before := append([]byte(nil), content.Documents[0].Canonical...)
			content.Documents[0].Canonical[0] = '!'
			repeated, err := value.Read(ctx, ReadRequest{Identity: identity, Document: true})
			if err != nil || !bytes.Equal(repeated.Documents[0].Canonical, before) {
				t.Fatal("returned document aliases catalog state", err)
			}
			last, err := value.LastPublication()
			if err != nil || last != published {
				t.Fatal("read cleanup changed publication evidence", err)
			}
		})
	}
}

func TestCatalogReadExactFilterService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			ctx := context.Background()
			for index, category := range []string{"\u200b", "ordinary"} {
				declared := health.Declarations()
				declared.Workflows[0].Code = []string{"invisible-category", "ordinary-category"}[index]
				declared.Workflows[0].Modes = nil
				input := authored(t, declared)
				input.Category = &Category{Code: category, Name: &Text{English: "Category"}}
				if _, err := store.Register(ctx, preparedPublication(t, input)); err != nil {
					t.Fatal("register filter fixture", err)
				}
			}
			for _, test := range []struct {
				name, category, code string
				want                 []string
			}{
				{"absent", "", "", []string{"invisible-category", "ordinary-category"}},
				{"ordinary_category", "ordinary", "", []string{"ordinary-category"}},
				{"ignorable_category", "\u200b", "", []string{"invisible-category"}},
				{"ordinary_code", "", "ordinary-category", []string{"ordinary-category"}},
				{"ignorable_code", "", "\u200b", nil},
			} {
				t.Run(test.name, func(t *testing.T) {
					page, err := store.List(ctx, ListRequest{Parent: declaration.Identity{Namespace: "example"}, Category: test.category, Code: test.code})
					if err != nil || len(page.Items) != len(test.want) {
						t.Fatal("exact filter count differs", len(page.Items), err)
					}
					for index, code := range test.want {
						if page.Items[index].Identity.Code != code {
							t.Fatal("exact filter identity differs", page.Items[index].Identity.Code)
						}
					}
				})
			}
		})
	}
}

func TestCatalogReadPaginationService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			ctx := context.Background()
			for _, code := range []string{"alpha", "beta", "gamma"} {
				declared := health.Declarations()
				declared.Workflows[0].Code = code
				if _, err := store.Register(ctx, preparedPublication(t, authored(t, declared))); err != nil {
					t.Fatal(err)
				}
			}
			request := ListRequest{Parent: declaration.Identity{Namespace: "example"}, Limit: 2}
			first, err := store.List(ctx, request)
			if err != nil || len(first.Items) != 2 || first.Items[0].Identity.Code != "alpha" || first.Items[1].Identity.Code != "beta" || first.Next == "" {
				t.Fatal("first page differs", err)
			}
			request.Cursor = first.Next
			request.Limit = 1
			second, err := store.List(ctx, request)
			if err != nil || len(second.Items) != 1 || second.Items[0].Identity.Code != "gamma" || second.Next != "" {
				t.Fatal("cursor did not retain ordered scope", err)
			}
			request.Parent.Namespace = "other"
			if _, err := store.List(ctx, request); !errors.Is(err, ErrInput) {
				t.Fatal("cursor crossed namespace", err)
			}
			request.Parent.Namespace = "example"
			request.Cursor = ""
			request.Code = "beta"
			filtered, err := store.List(ctx, request)
			if err != nil || len(filtered.Items) != 1 || filtered.Items[0].Identity.Code != "beta" {
				t.Fatal("exact code filter differs", err)
			}
			active := true
			request.Code = ""
			request.Enabled = &active
			filtered, err = store.List(ctx, request)
			if err != nil || len(filtered.Items) != 0 {
				t.Fatal("disabled entities ignored filter", err)
			}
			request = ListRequest{Parent: declaration.Identity{Namespace: "example", Code: "missing"}}
			if _, err := store.List(ctx, request); !errors.Is(err, ErrMissing) {
				t.Fatal("missing parent became empty catalog", err)
			}
		})
	}
}
func TestCatalogReadIntegrityService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			ctx := context.Background()
			prepared := preparedPublication(t, registrationInput(t))
			published, err := store.Register(ctx, prepared)
			if err != nil {
				t.Fatal(err)
			}
			content := prepared.state.contents[0]
			fixture.exec(t, "INSERT INTO "+fixture.table("workflow_version")+" (workflow_definition_id,version_code,spec_format,fingerprint_profile,spec_sha256,spec_document) VALUES ("+fixture.params(6)+")", published.DefinitionID, "corrupt", declaration.SpecFormat, declaration.SpecProfile, make([]byte, 32), content.Bytes)
			identity := declaration.Identity{Namespace: "example", Code: "server-health", Version: "corrupt"}
			metadata, err := store.Read(ctx, ReadRequest{Identity: identity})
			if err != nil || metadata.Entity.DocumentVerified {
				t.Fatal("metadata falsely verified document", err)
			}
			if view, err := store.Read(ctx, ReadRequest{Identity: identity, Document: true}); !errors.Is(err, ErrStored) || !errors.Is(err, declaration.ErrIntegrity) || view.Entity.ID != "" {
				t.Fatal("corrupt body was exposed", err)
			}
			fixture.exec(t, "INSERT INTO "+fixture.table("workflow_version")+" (workflow_definition_id,version_code,spec_format,fingerprint_profile,spec_sha256,spec_document) VALUES ("+fixture.params(6)+")", published.DefinitionID, "future", "future-format", declaration.SpecProfile, make([]byte, 32), content.Bytes)
			identity.Version = "future"
			if _, err := store.Read(ctx, ReadRequest{Identity: identity, Document: true}); !errors.Is(err, ErrStored) || !errors.Is(err, declaration.ErrUnsupported) {
				t.Fatal("unsupported format not distinguished", err)
			}
			fixture.exec(t, "INSERT INTO "+fixture.table("i18n_locale")+" (code,native_name) VALUES ('zh-CN','简体中文')")
			locale := fixture.scalar(t, "SELECT id FROM "+fixture.table("i18n_locale")+" WHERE code='zh-CN'")
			resource := fixture.scalar(t, "SELECT id FROM "+fixture.table("i18n_resource")+" WHERE scope='workflow.definition'")
			fixture.exec(t, "INSERT INTO "+fixture.table("i18n_text")+" (resource_id,locale_id,value) VALUES ("+fixture.params(3)+")", resource, locale, "翻译")
			fixture.exec(t, "DELETE FROM "+fixture.table("i18n_text")+" WHERE resource_id="+resource+" AND locale_id=(SELECT id FROM "+fixture.table("i18n_locale")+" WHERE code='en')")
			identity.Version = ""
			if _, err := store.Read(ctx, ReadRequest{Identity: identity, Locale: "zh-CN"}); !errors.Is(err, ErrStored) {
				t.Fatal("translation hid missing required English", err)
			}
		})
	}
}
func TestCatalogReadAnchorCoherenceService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			ctx := context.Background()
			request := registrationInput(t)
			request.Tags = map[string]string{"team": "old"}
			if _, err := store.Register(ctx, preparedPublication(t, request)); err != nil {
				t.Fatal(err)
			}
			identity := declaration.Identity{Namespace: "example", Code: "server-health"}
			err := store.read(ctx, func(work context.Context, tx *publicationTx) error {
				query, args := tx.readSQL(identity, Definition, "en", "", "", nil, "", 1, true)
				rows, err := tx.readAnchor(work, 65, query, args...)
				if err != nil {
					return err
				}
				writer, err := fixture.begin(work)
				if err != nil {
					return err
				}
				_, err = writer.exec(work, "UPDATE "+fixture.table("workflow_definition")+" SET enabled=TRUE,row_revision=row_revision+1")
				if err == nil {
					_, err = writer.exec(work, "UPDATE "+fixture.table("i18n_text")+" SET value='New name' WHERE resource_id=(SELECT id FROM "+fixture.table("i18n_resource")+" WHERE scope='workflow.definition')")
				}
				if err == nil {
					_, err = writer.exec(work, "UPDATE "+fixture.table("workflow_definition_tag")+" SET tag_value='new'")
				}
				finished := finishPublication(work, writer, err == nil)
				if err != nil || finished != nil {
					return errors.Join(err, finished)
				}
				entity, _, err := tx.readEntity(rows[0], identity, Definition, "en")
				if err != nil {
					return err
				}
				if entity.Enabled || entity.Display.Name.Value != "Server health" || string(rows[0][39]) != "old" {
					t.Fatal("anchor mixed management revisions")
				}
				if _, err := tx.readAnchor(work, 65, query, args...); !errors.Is(err, ErrState) {
					t.Fatal("second mutable read admitted")
				}
				return nil
			})
			if err != nil {
				t.Fatal("snapshot anchor failed", err)
			}
			fresh, err := store.Read(ctx, ReadRequest{Identity: identity})
			if err != nil || !fresh.Entity.Enabled || fresh.Entity.Display.Name.Value != "New name" || fresh.Tags[0].Value != "new" {
				t.Fatal("new request reused stale snapshot", err)
			}
			if err := store.read(ctx, func(work context.Context, tx *publicationTx) error {
				_, err := tx.exec(work, "UPDATE "+fixture.table("workflow_definition")+" SET enabled=FALSE")
				return err
			}); err == nil {
				t.Fatal("read-only transaction permitted mutation")
			}
		})
	}
}
func TestCatalogReadOnlyPermissionsService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			writer := fixture.catalog(t, Options{})
			ctx := context.Background()
			if _, err := writer.Register(ctx, preparedPublication(t, registrationInput(t))); err != nil {
				t.Fatal(err)
			}
			var random [24]byte
			if _, err := rand.Read(random[:]); err != nil {
				t.Fatal(err)
			}
			user, password := "reader_"+hex.EncodeToString(random[:8]), hex.EncodeToString(random[:])
			reader := &persistenceFixture{dialect: dialect, target: fixture.target, quoted: fixture.quoted}
			if dialect == "postgresql" {
				role := "\"" + user + "\""
				fixture.exec(t, "CREATE ROLE "+role+" LOGIN PASSWORD '"+password+"'")
				t.Cleanup(func() { fixture.exec(t, "DROP OWNED BY "+role); fixture.exec(t, "DROP ROLE "+role) })
				fixture.exec(t, "GRANT USAGE ON SCHEMA "+fixture.quoted+" TO "+role)
				fixture.exec(t, "GRANT SELECT ON ALL TABLES IN SCHEMA "+fixture.quoted+" TO "+role)
				var selected postgres.Settings
				loadPersistenceSettings(t, dialect, &selected)
				selected.User, selected.Password = user, password
				reader.pg = persistencePostgres(t, selected)
			} else {
				account := "'" + user + "'@'%'"
				fixture.exec(t, "CREATE USER "+account+" IDENTIFIED BY '"+password+"'")
				t.Cleanup(func() { fixture.exec(t, "DROP USER "+account) })
				fixture.exec(t, "GRANT SELECT,TRIGGER ON "+fixture.quoted+".* TO "+account)
				var selected mysql.Settings
				loadPersistenceSettings(t, dialect, &selected)
				selected.User, selected.Password, selected.Database = user, password, fixture.target
				reader.my = persistenceMySQL(t, selected)
			}
			store := reader.catalog(t, Options{})
			page, err := store.List(ctx, ListRequest{Parent: declaration.Identity{Namespace: "example"}})
			if err != nil || len(page.Items) != 1 {
				t.Fatal("read required publication privileges", err)
			}
			_, err = store.Read(ctx, ReadRequest{Identity: declaration.Identity{Namespace: "example", Code: "server-health", Version: "1"}, Document: true})
			if err != nil {
				t.Fatal("document read required mutation privileges", err)
			}
			comparison, err := store.Compare(ctx, preparedPublication(t, registrationInput(t)))
			if err != nil || !comparison.Equal {
				t.Fatal("comparison required mutation privileges", err)
			}
		})
	}
}

func TestCatalogReadDisappearanceService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			ctx := context.Background()
			declared := health.Declarations()
			declared.Workflows[0].Modes = nil
			if _, err := store.Register(ctx, preparedPublication(t, authored(t, declared))); err != nil {
				t.Fatal(err)
			}
			identity := declaration.Identity{Namespace: "example", Code: "server-health", Version: "1"}
			err := store.read(ctx, func(work context.Context, tx *publicationTx) error {
				query, args := tx.readSQL(identity, Version, "en", "", "", nil, "", 1, true)
				rows, err := tx.readAnchor(work, 2, query, args...)
				if err != nil {
					return err
				}
				_, refs, err := tx.readEntity(rows[0], identity, Version, "en")
				if err != nil {
					return err
				}
				fixture.exec(t, "DELETE FROM "+fixture.table("workflow_version")+" WHERE version_code='1'")
				_, err = tx.readBody(work, refs[0])
				return err
			})
			if !errors.Is(err, ErrStored) {
				t.Fatal("deleted anchored document became mixed success", err)
			}
			if !store.Released() {
				t.Fatal("failed anchored read retained cleanup")
			}
		})
	}
}
func TestProjectCatalogReadsService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			if _, raw, err := fixture.command(t, nil, "--output=json", "workflow", "register"); err != nil {
				t.Fatalf("register failed: %s; %v", raw, err)
			}
			environment := []string{"FATHOMRY_CATALOG_NO_WORKFLOWS=1"}
			_, raw, err := fixture.command(t, environment, "--output=json", "workflow", "list", "--namespace=example", "--limit=1")
			var listed struct {
				Data Page `json:"data"`
			}
			if err != nil || json.Unmarshal(raw, &listed) != nil || len(listed.Data.Items) != 1 || listed.Data.Next == "" {
				t.Fatalf("restart list failed: %s; %v", raw, err)
			}
			_, raw, err = fixture.command(t, environment, "--output=json", "workflow", "list", "--namespace=example", "--limit=1", "--cursor="+listed.Data.Next)
			if err != nil || json.Unmarshal(raw, &listed) != nil || len(listed.Data.Items) != 1 || listed.Data.Items[0].Identity.Code != "server-health" {
				t.Fatalf("restart cursor failed: %s; %v", raw, err)
			}
			_, raw, err = fixture.command(t, environment, "--output=json", "--lang=zh-CN", "workflow", "show", "--namespace=example", "--workflow=server-health", "--version=1", "--mode=patient", "--revision=1", "--document", "--locale=zh-cn-u-ca-chinese")
			var shown struct {
				Data View `json:"data"`
			}
			if err != nil || json.Unmarshal(raw, &shown) != nil || !shown.Data.Entity.DocumentVerified || len(shown.Data.Documents) != 2 || shown.Data.RequestedLocale != "zh-CN-u-ca-chinese" {
				t.Fatalf("document show failed: %s; %v", raw, err)
			}
			contents := []declaration.Content{}
			for _, doc := range shown.Data.Documents {
				contents = append(contents, declaration.Content{Entry: declaration.Entry{Identity: doc.Identity, Profile: doc.Profile, SHA256: doc.SHA256}, Bytes: doc.Canonical})
			}
			if _, err := declaration.ParseContents(context.Background(), contents); err != nil {
				t.Fatal("CLI document encoding changed canonical bytes", err)
			}
			for _, args := range [][]string{{"--lang=zh-CN", "workflow", "list", "--help"}, {"--lang=zh-CN", "workflow", "show", "--help"}} {
				if _, _, err := fixture.command(t, []string{"FATHOMRY_CATALOG_OFFLINE=1"}, args...); err != nil {
					t.Fatal("read help invoked resolver")
				}
			}
			if _, raw, err := fixture.command(t, []string{"FATHOMRY_CATALOG_OFFLINE=1"}, "--output=json", "workflow", "show", "--namespace=example", "--workflow=server-health", "--locale=zh_CN"); err == nil || len(raw) != 0 {
				t.Fatal("invalid read locale escaped validation")
			}
			empty := persistenceTarget(t, dialect, false)
			if _, raw, err := empty.command(t, nil, "--output=json", "workflow", "list", "--namespace=example"); err == nil || len(raw) != 0 {
				t.Fatal("uninstalled schema became an empty catalog")
			}
		})
	}
}

func TestCatalogReadLabelOrderingService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			labels := []string{"A", "a", "a-1", "a.1", "a+1", "01", strings.Repeat("Z", 128)}
			declared := declaration.Declaration{}
			for _, label := range labels {
				workflow := health.Declarations().Workflows[0]
				workflow.Version = label
				workflow.Modes = nil
				declared.Workflows = append(declared.Workflows, workflow)
			}
			if _, err := store.Register(context.Background(), preparedPublication(t, authored(t, declared))); err != nil {
				t.Fatal(err)
			}
			slices.Sort(labels)
			request := ListRequest{Parent: declaration.Identity{Namespace: "example", Code: "server-health"}, Limit: 1}
			for index, want := range labels {
				page, err := store.List(context.Background(), request)
				if err != nil || len(page.Items) != 1 || page.Items[0].Identity.Version != want {
					t.Fatal("machine label ordering changed", index, err)
				}
				request.Cursor = page.Next
				if index < len(labels)-1 && request.Cursor == "" {
					t.Fatal("pagination ended early")
				}
			}
			if request.Cursor != "" {
				t.Fatal("last page has continuation")
			}
		})
	}
}

func (fixture *persistenceFixture) comparisonCatalog(t *testing.T) *Catalog {
	t.Helper()
	selected := &persistenceFixture{dialect: fixture.dialect, target: fixture.target, quoted: fixture.quoted}
	if fixture.pg != nil {
		var settings postgres.Settings
		loadPersistenceSettings(t, "postgresql", &settings)
		settings.MaxRows = MaxComparisonRows
		selected.pg = persistencePostgres(t, settings)
	} else {
		var settings mysql.Settings
		loadPersistenceSettings(t, "mysql", &settings)
		settings.Database, settings.MaxRows = fixture.target, MaxComparisonRows
		selected.my = persistenceMySQL(t, settings)
	}
	return selected.catalog(t, Options{})
}
func TestCatalogComparisonLargeAggregateService(t *testing.T) {
	declared := maximumCatalogSpecification(t)
	template := declared.Workflows[0]
	tail := template.Spec.Contracts["target"].Properties["field15"]
	tail.Enum = tail.Enum[:len(tail.Enum)-8]
	template.Spec.Contracts["target"].Properties["field15"] = tail
	declared.Workflows = nil
	for index := range 4 {
		workflow := template
		workflow.Version = strconv.Itoa(index + 1)
		declared.Workflows = append(declared.Workflows, workflow)
	}
	request := authored(t, declared)
	raw, err := request.Bundle.BytesCopy()
	if err != nil || len(raw) <= 15<<20 || len(raw) > declaration.MaxBundleBytes {
		t.Fatal("fixture does not approach the aggregate cap", len(raw), err)
	}
	prepared := preparedPublication(t, request)
	t.Logf("selected aggregate: %d bytes; canonical documents: %d", len(raw), prepared.state.info.DocumentBytes)
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			if _, err := store.Register(context.Background(), prepared); err != nil {
				t.Fatal("large aggregate publication failed", err)
			}
			result, err := store.Compare(context.Background(), prepared)
			if err != nil || !result.Equal || result.Contents != 12 {
				t.Fatal("large aggregate comparison shrank document admission", err)
			}
		})
	}
}

func TestCatalogComparisonMetadataBytesService(t *testing.T) {
	declared := declaration.Declaration{}
	for index := range declaration.MaxWorkflows {
		workflow := health.Declarations().Workflows[0]
		workflow.Version = strconv.Itoa(index)
		workflow.Modes = append(workflow.Modes, declaration.ModeDeclaration{Code: "third", Revision: "1"}, declaration.ModeDeclaration{Code: "fourth", Revision: "1"})
		declared.Workflows = append(declared.Workflows, workflow)
	}
	request := authored(t, declared)
	start := len(request.Resources)
	for _, workflow := range declared.Workflows {
		request.Resources = append(request.Resources, Resource{Target: Target{Kind: Version, Version: workflow.Version}, Field: Description})
		for _, mode := range workflow.Modes {
			request.Resources = append(request.Resources, Resource{Target: Target{Kind: Mode, Version: workflow.Version, Mode: mode.Code}, Field: Description})
		}
	}
	baseline := preparedPublication(t, request)
	remaining := MaxMetadataBytes - baseline.state.info.MetadataBytes
	for index := start; index < len(request.Resources) && remaining > 0; index++ {
		size := min(MaxTextBytes, remaining)
		request.Resources[index].Text.English = strings.Repeat("x", size)
		remaining -= size
	}
	if remaining != 0 {
		t.Fatal("metadata fixture cannot reach exact cap")
	}
	prepared := preparedPublication(t, request)
	if prepared.state.info.MetadataBytes != MaxMetadataBytes {
		t.Fatal("metadata fixture is not exact")
	}
	for index := start; index < len(request.Resources); index++ {
		if len(request.Resources[index].Text.English) < MaxTextBytes {
			request.Resources[index].Text.English += "x"
			break
		}
	}
	if _, err := Prepare(context.Background(), request); !errors.Is(err, ErrLimit) {
		t.Fatal("one more metadata byte was accepted", err)
	}
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.comparisonCatalog(t)
			if _, err := store.Register(context.Background(), prepared); err != nil {
				t.Fatal("exact metadata cap failed publication", err)
			}
			equal, err := store.Compare(context.Background(), prepared)
			if err != nil || !equal.Equal {
				t.Fatal("exact metadata cap failed comparison", err)
			}
			fixture.exec(t, "UPDATE "+fixture.table("i18n_text")+" SET value="+fixture.params(1)+" WHERE resource_id IN (SELECT id FROM "+fixture.table("i18n_resource")+" WHERE code LIKE '%11:description')", strings.Repeat("y", MaxTextBytes))
			failed, err := store.Compare(context.Background(), prepared)
			if !errors.Is(err, ErrLimit) || failed.Complete {
				t.Fatal("stored metadata bypassed aggregate comparison bound", err)
			}
		})
	}
}

func TestCatalogComparisonExactNamespaceService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			declared := health.Declarations()
			declared.Workflows[0].Namespace = "团队-é-'\\source"
			prepared := preparedPublication(t, authored(t, declared))
			if _, err := store.Register(context.Background(), prepared); err != nil {
				t.Fatal(err)
			}
			same, err := store.Compare(context.Background(), prepared)
			if err != nil || !same.Equal {
				t.Fatal("JSON selection changed exact UTF-8/key bytes", err)
			}
			declared.Workflows[0].Namespace = "团队-e\u0301-'\\source"
			other, err := store.Compare(context.Background(), preparedPublication(t, authored(t, declared)))
			if err != nil || other.DefinitionExists || other.Equal {
				t.Fatal("Unicode-distinct namespace aliased", err)
			}
		})
	}
}
func TestCatalogCompareService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			ctx := context.Background()
			input := registrationInput(t)
			input.Category = &Category{Code: "initial", Name: &Text{English: "Initial"}}
			input.Tags = map[string]string{"team": "initial"}
			prepared := preparedPublication(t, input)
			missing, err := store.Compare(ctx, prepared)
			if err != nil || !missing.Complete || missing.Equal || missing.DefinitionExists || missing.Missing != missing.Contents+missing.Resources {
				if native, ok := postgres.InspectError(err); ok {
					t.Log("SQLSTATE", native.Code, "position", native.Position)
				}
				if native, ok := mysql.InspectError(err); ok {
					t.Log("MySQL", native.Number)
				}
				t.Fatal("missing comparison failed", err)
			}
			published, err := store.Register(ctx, prepared)
			if err != nil {
				t.Fatal(err)
			}
			before := fixture.snapshot(t)
			equal, err := store.Compare(ctx, prepared)
			if err != nil || !equal.Complete || !equal.Equal || equal.DefinitionID != published.DefinitionID || equal.Matched != equal.Contents+equal.Resources {
				t.Fatal("equal comparison failed", err)
			}
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("comparison mutated catalog")
			}
			fixture.exec(t, "UPDATE "+fixture.table("workflow_definition")+" SET enabled=TRUE,category_id=NULL,row_revision=row_revision+1")
			fixture.exec(t, "DELETE FROM "+fixture.table("workflow_category")+" WHERE code='initial'")
			fixture.exec(t, "UPDATE "+fixture.table("workflow_definition_tag")+" SET tag_value='managed'")
			equal, err = store.Compare(ctx, prepared)
			if err != nil || !equal.Equal {
				t.Fatal("initial metadata or management state became drift", err)
			}
			changed := registrationInput(t)
			changed.Resources[0].Text.English = "Different"
			difference, err := store.Compare(ctx, preparedPublication(t, changed))
			if err != nil || difference.Equal || difference.Different != 1 || difference.Problems[0].Kind != "english_text" {
				t.Fatal("English conflict not distinguished", err)
			}
			declared := health.Declarations()
			declared.Workflows[0].Modes[0].Values = map[string]declaration.Decimal{"attempt_timeout": "1200"}
			difference, err = store.Compare(ctx, preparedPublication(t, authored(t, declared)))
			if err != nil || difference.Equal || difference.Different != 1 || difference.Problems[0].Kind != "immutable_content" {
				t.Fatal("immutable difference not distinguished", err)
			}
			declared = health.Declarations()
			declared.Workflows[0].Version = "new"
			absent, err := store.Compare(ctx, preparedPublication(t, authored(t, declared)))
			if err != nil || !absent.Complete || absent.Equal || absent.Missing == 0 {
				t.Fatal("missing selection treated as unavailability", err)
			}
			last, err := store.LastPublication()
			if err != nil || last != published {
				t.Fatal("comparison rewrote publication history", err)
			}
		})
	}
}

func TestCatalogComparisonIntegrityService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			ctx := context.Background()
			input := registrationInput(t)
			prepared := preparedPublication(t, input)
			published, err := store.Register(ctx, prepared)
			if err != nil {
				t.Fatal(err)
			}
			content := prepared.state.contents[0]
			fixture.exec(t, "INSERT INTO "+fixture.table("workflow_version")+" (workflow_definition_id,version_code,spec_format,fingerprint_profile,spec_sha256,spec_document) VALUES ("+fixture.params(6)+")", published.DefinitionID, "corrupt", declaration.SpecFormat, declaration.SpecProfile, make([]byte, 32), content.Bytes)
			declared := health.Declarations()
			declared.Workflows[0].Version = "corrupt"
			result, err := store.Compare(ctx, preparedPublication(t, authored(t, declared)))
			if !errors.Is(err, ErrStored) || !errors.Is(err, declaration.ErrIntegrity) || result.Complete {
				t.Fatal("corrupt stored content became ordinary difference", err)
			}
			fixture.exec(t, "INSERT INTO "+fixture.table("workflow_version")+" (workflow_definition_id,version_code,spec_format,fingerprint_profile,spec_sha256,spec_document) VALUES ("+fixture.params(6)+")", published.DefinitionID, "future", "unsupported-format", declaration.SpecProfile, make([]byte, 32), content.Bytes)
			declared.Workflows[0].Version = "future"
			result, err = store.Compare(ctx, preparedPublication(t, authored(t, declared)))
			if !errors.Is(err, ErrStored) || !errors.Is(err, declaration.ErrUnsupported) || result.Complete {
				t.Fatal("unsupported content became ordinary difference", err)
			}
			fixture.exec(t, "INSERT INTO "+fixture.table("i18n_locale")+" (code,native_name) VALUES ('zh-CN','简体中文')")
			locale := fixture.scalar(t, "SELECT id FROM "+fixture.table("i18n_locale")+" WHERE code='zh-CN'")
			resource := fixture.scalar(t, "SELECT id FROM "+fixture.table("i18n_resource")+" WHERE scope='workflow.definition'")
			fixture.exec(t, "INSERT INTO "+fixture.table("i18n_text")+" (resource_id,locale_id,value) VALUES ("+fixture.params(3)+")", resource, locale, "translated")
			fixture.exec(t, "DELETE FROM "+fixture.table("i18n_text")+" WHERE resource_id="+resource+" AND locale_id=(SELECT id FROM "+fixture.table("i18n_locale")+" WHERE code='en')")
			result, err = store.Compare(ctx, prepared)
			if !errors.Is(err, ErrStored) || result.Complete {
				t.Fatal("translation masked missing English", err)
			}
			if !store.Released() {
				t.Fatal("failed comparison leaked transaction")
			}
		})
	}
}
func TestCatalogComparisonAnchorService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			store := fixture.catalog(t, Options{})
			ctx := context.Background()
			prepared := preparedPublication(t, registrationInput(t))
			if _, err := store.Register(ctx, prepared); err != nil {
				t.Fatal(err)
			}
			raw, err := prepareComparison(prepared)
			if err != nil {
				t.Fatal(err)
			}
			err = store.read(ctx, func(work context.Context, tx *publicationTx) error {
				query, args := tx.comparisonSQL(prepared.state, raw)
				rows, err := tx.readAnchor(work, MaxComparisonRows, query, args...)
				if err != nil {
					return err
				}
				anchor, err := tx.comparisonRows(rows, prepared.state)
				if err != nil {
					return err
				}
				fixture.exec(t, "UPDATE "+fixture.table("i18n_text")+" SET value='Changed after anchor' WHERE resource_id=(SELECT id FROM "+fixture.table("i18n_resource")+" WHERE scope='workflow.definition')")
				comparison, err := tx.compareAnchored(work, prepared.state, anchor)
				if err == nil && !comparison.Equal {
					t.Fatal("comparison spliced later English into earlier anchor")
				}
				return err
			})
			if err != nil {
				t.Fatal("coherent comparison failed", err)
			}
			fresh, err := store.Compare(ctx, prepared)
			if err != nil || fresh.Equal || fresh.Different != 1 {
				t.Fatal("fresh comparison reused stale metadata", err)
			}
		})
	}
}

type comparisonCLIReport struct {
	Checked   bool `json:"checked"`
	Compared  int  `json:"compared"`
	Complete  bool `json:"complete"`
	Equal     bool `json:"equal"`
	Selected  int  `json:"selected_workflows"`
	Omitted   int  `json:"omitted_workflows"`
	Workflows []struct {
		Code   string      `json:"code"`
		Result *Comparison `json:"result"`
	} `json:"workflows"`
}

func TestProjectWorkflowCompareService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			_, raw, err := fixture.command(t, nil, "--output=json", "workflow", "compare")
			var observed struct {
				Data comparisonCLIReport `json:"data"`
			}
			if err == nil || json.Unmarshal(raw, &observed) != nil || !observed.Data.Complete || observed.Data.Equal || observed.Data.Compared != 2 {
				t.Fatalf("missing catalog was not a completed mismatch: %s; %v", raw, err)
			}
			if _, raw, err := fixture.command(t, nil, "--output=json", "workflow", "register"); err != nil {
				t.Fatalf("registration failed: %s; %v", raw, err)
			}
			before := fixture.snapshot(t)
			_, raw, err = fixture.command(t, nil, "--output=json", "workflow", "compare")
			if err != nil || json.Unmarshal(raw, &observed) != nil || !observed.Data.Equal || observed.Data.Compared != 2 {
				t.Fatalf("restart comparison failed: %s; %v", raw, err)
			}
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("compare command mutated data")
			}
			_, raw, err = fixture.command(t, []string{"FATHOMRY_CATALOG_CONFLICT=1"}, "--output=json", "workflow", "compare")
			if err == nil || json.Unmarshal(raw, &observed) != nil || !observed.Data.Complete || observed.Data.Equal || observed.Data.Compared != 2 {
				t.Fatalf("one mismatch suppressed other workflows: %s; %v", raw, err)
			}
			_, raw, err = fixture.command(t, []string{"FATHOMRY_CATALOG_BAD_SECOND=1"}, "--output=json", "workflow", "compare", "--namespace=example", "--workflow=server-health", "--version=1", "--mode=patient", "--revision=1")
			if err != nil || json.Unmarshal(raw, &observed) != nil || !observed.Data.Equal || observed.Data.Selected != 1 || observed.Data.Omitted != 1 {
				t.Fatalf("selective compare differs from vet/register: %s; %v", raw, err)
			}
			_, raw, err = fixture.command(t, []string{"FATHOMRY_CATALOG_BAD_SECOND=1", "FATHOMRY_CATALOG_OFFLINE=1"}, "--output=json", "workflow", "compare")
			if err == nil || json.Unmarshal(raw, &observed) != nil || observed.Data.Checked || observed.Data.Compared != 0 {
				t.Fatal("failed preflight entered comparison", err)
			}
			if _, _, err := fixture.command(t, []string{"FATHOMRY_CATALOG_OFFLINE=1"}, "--lang=zh-CN", "workflow", "compare", "--help"); err != nil {
				t.Fatal("comparison help resolved DB")
			}
			empty := persistenceTarget(t, dialect, false)
			if _, raw, err := empty.command(t, nil, "--output=json", "workflow", "compare"); err == nil || len(raw) != 0 {
				t.Fatal("uninstalled schema became completed comparison")
			}
		})
	}
}
func TestCatalogRegistrationService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			value := fixture.catalog(t, Options{})
			prepared := preparedPublication(t, registrationInput(t))
			initial, err := value.Register(context.Background(), prepared)
			if err != nil || initial.Effect != Applied || initial.Acknowledgement != CommitAcknowledged || !initial.CleanupComplete || initial.DefinitionID == "" || !initial.Changes.DefinitionCreated {
				t.Fatal("public registration did not acknowledge creation", initial.Effect, err)
			}
			before := fixture.snapshot(t)
			repeated, err := value.Register(context.Background(), prepared)
			if err != nil || repeated.Effect != Unchanged || repeated.DefinitionID != initial.DefinitionID || repeated.Changes != (Changes{}) || !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("public no-op changed state", err)
			}
			request := registrationInput(t)
			request.Resources[0].Text.English = "Conflict"
			refused, err := value.Register(context.Background(), preparedPublication(t, request))
			if !errors.Is(err, ErrConflict) || refused.Effect != RolledBack || refused.Acknowledgement != RollbackAcknowledged || refused.DefinitionID != "" || refused.Changes != (Changes{}) || !refused.CleanupComplete {
				t.Fatal("conflict result lost atomicity", refused.Effect, err)
			}
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("conflict changed persisted state")
			}
			last, err := value.LastPublication()
			if err != nil || last != refused {
				t.Fatal("retained report differs", err)
			}
			if err := value.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if fixture.scalar(t, "SELECT 1") != "1" {
				t.Fatal("catalog closed borrowed source")
			}
			if _, err := value.Register(context.Background(), prepared); !errors.Is(err, ErrState) {
				t.Fatal("closed catalog admitted work")
			}
		})
	}
}
func TestCatalogBusyAndCancellationService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			fixture.exec(t, "INSERT INTO "+fixture.table("workflow_category")+" (namespace,code) VALUES ('example','shared')")
			request := registrationInput(t)
			request.Category = &Category{Code: "shared"}
			prepared := preparedPublication(t, request)
			lifetime, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			blocker, err := fixture.begin(lifetime)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = finishPublication(cleanup, blocker, false)
			}()
			if _, err := blocker.row(lifetime, "workflow_category", "id", []string{"code"}, []any{"shared"}, "FOR UPDATE"); err != nil {
				t.Fatal(err)
			}
			value := fixture.catalog(t, Options{})
			ctx, cancel := context.WithCancelCause(lifetime)
			cause := errors.New("catalog-cancellation-canary")
			type response struct {
				publication Publication
				err         error
			}
			done := make(chan response, 1)
			go func() { result, err := value.Register(ctx, prepared); done <- response{result, err} }()
			fixture.waitCategoryLock(t)
			if _, err := value.Register(lifetime, prepared); !errors.Is(err, ErrState) {
				t.Fatal("overlapping registration admitted")
			}
			if err := value.Close(lifetime); !errors.Is(err, ErrState) {
				t.Fatal("overlapping close not reported")
			}
			cancel(cause)
			result := <-done
			if result.err == nil || !errors.Is(result.err, cause) || result.publication.Effect == Applied || result.publication.Changes != (Changes{}) {
				t.Fatal("canceled operation claimed publication", result.err)
			}
			if err := value.Close(lifetime); err != nil || !value.Released() {
				t.Fatal("canceled operation cleanup not retained", err)
			}
			last, err := value.LastPublication()
			if err != nil || !last.CleanupComplete {
				t.Fatal(err)
			}
			if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "0" {
				t.Fatal("canceled publication remained")
			}
		})
	}
}

type publicationCLIReport struct {
	Effect    string `json:"effect"`
	Checked   bool   `json:"checked"`
	Complete  bool   `json:"complete"`
	Selected  int    `json:"selected_workflows"`
	Omitted   int    `json:"omitted_workflows"`
	Attempted int    `json:"attempted"`
	Workflows []struct {
		Code   string       `json:"code"`
		Result *Publication `json:"result"`
	} `json:"workflows"`
}

func (fixture *persistenceFixture) command(t *testing.T, environment []string, args ...string) (publicationCLIReport, []byte, error) {
	t.Helper()
	var selected any
	if fixture.pg != nil {
		var value postgres.Settings
		loadPersistenceSettings(t, "postgresql", &value)
		value.ParserHome = os.Getenv("HOME")
		if fixture.port != 0 {
			value.Port = fixture.port
		}
		selected = value
	} else {
		var value mysql.Settings
		loadPersistenceSettings(t, "mysql", &value)
		value.Database = fixture.target
		if fixture.port != 0 {
			value.Port = fixture.port
		}
		selected = value
	}
	config := filepath.Join(t.TempDir(), "settings.json")
	raw, err := json.Marshal(selected)
	if err != nil {
		t.Fatal("fixture config unavailable")
	}
	if err := os.WriteFile(config, raw, 0600); err != nil {
		t.Fatal("fixture config unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	parameters := append([]string{fixture.dialect, fixture.target, config}, args...)
	command := exec.CommandContext(ctx, persistenceInstaller, parameters...)
	command.Env = append(os.Environ(), environment...)
	var output, diagnostic bytes.Buffer
	command.Stdout, command.Stderr = &output, &diagnostic
	err = command.Run()
	if ctx.Err() != nil {
		t.Fatal("project CLI exceeded its bound")
	}
	var envelope struct {
		Data publicationCLIReport `json:"data"`
	}
	_ = json.Unmarshal(output.Bytes(), &envelope)
	return envelope.Data, output.Bytes(), err
}
func TestProjectWorkflowRegisterService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			for _, args := range [][]string{{"--lang=zh-CN", "workflow", "register", "--help"}, {"--output=json", "workflow", "vet", "--workflow=server-health"}} {
				if _, _, err := fixture.command(t, []string{"FATHOMRY_CATALOG_OFFLINE=1"}, args...); err != nil {
					t.Fatal("offline command invoked resolver", err)
				}
			}
			bad, _, err := fixture.command(t, []string{"FATHOMRY_CATALOG_BAD_SECOND=1", "FATHOMRY_CATALOG_OFFLINE=1"}, "--output=json", "workflow", "register")
			if err == nil || bad.Checked || bad.Attempted != 0 || bad.Effect != "not_started" {
				t.Fatal("invalid project performed publication", bad, err)
			}
			if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "0" {
				t.Fatal("preflight partially published")
			}
			missing, _, err := fixture.command(t, []string{"FATHOMRY_CATALOG_MISSING_METADATA=1", "FATHOMRY_CATALOG_OFFLINE=1"}, "--output=json", "workflow", "register")
			if err == nil || missing.Checked || missing.Attempted != 0 {
				t.Fatal("missing metadata ignored", err)
			}
			selected, raw, err := fixture.command(t, []string{"FATHOMRY_CATALOG_BAD_SECOND=1"}, "--output=json", "workflow", "register", "--namespace=example", "--workflow=server-health", "--version=1", "--mode=patient", "--revision=1")
			if err != nil || !selected.Complete || !selected.Checked || selected.Selected != 1 || selected.Omitted != 1 || selected.Attempted != 1 || selected.Effect != "applied" {
				t.Fatalf("selected actual CLI registration failed: %s; %v", raw, err)
			}
			if len(selected.Workflows) != 1 || selected.Workflows[0].Result == nil || selected.Workflows[0].Result.Acknowledgement != CommitAcknowledged {
				t.Fatal("commit evidence missing")
			}
			if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_mode")) != "1" {
				t.Fatal("mode selection published omitted modes")
			}
			all, raw, err := fixture.command(t, nil, "--output=json", "workflow", "register")
			if err != nil || !all.Complete || all.Selected != 2 || all.Attempted != 2 || all.Effect != "applied" {
				t.Fatalf("project batch failed: %s; %v", raw, err)
			}
			before := fixture.snapshot(t)
			repeat, raw, err := fixture.command(t, nil, "--output=json", "workflow", "register")
			if err != nil || !repeat.Complete || repeat.Effect != "unchanged" || !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatalf("restarted equal registration changed state: %s; %v", raw, err)
			}
		})
	}
}
func TestProjectRegistrationKeepsEarlierAggregateService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			if _, raw, err := fixture.command(t, nil, "--output=json", "workflow", "register", "--workflow=server-health"); err != nil {
				t.Fatalf("initial selection failed: %s; %v", raw, err)
			}
			report, raw, err := fixture.command(t, []string{"FATHOMRY_CATALOG_CONFLICT=1"}, "--output=json", "workflow", "register")
			if err == nil || report.Complete || report.Attempted != 2 || report.Effect != "partial" || len(report.Workflows) != 2 {
				t.Fatalf("batch failure lost partial outcome: %s; %v", raw, err)
			}
			if report.Workflows[0].Code != "second-health" || report.Workflows[0].Result == nil || report.Workflows[0].Result.Effect != Applied ||
				report.Workflows[1].Result == nil || report.Workflows[1].Result.Effect != RolledBack {
				t.Fatal("per-workflow effects collapsed")
			}
			if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "2" {
				t.Fatal("earlier committed aggregate was lost")
			}
		})
	}
}
func TestProjectRegistrationDoesNotInstallSchemaService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceTarget(t, dialect, false)
			report, raw, err := fixture.command(t, nil, "--output=json", "workflow", "register")
			if err == nil || report.Complete || report.Attempted != 1 {
				t.Fatalf("empty schema was admitted: %s; %v", raw, err)
			}
			if fixture.scalar(t, "SELECT count(*) FROM information_schema.tables WHERE table_schema="+fixture.params(1), fixture.target) != "0" {
				t.Fatal("register auto-installed schema")
			}
		})
	}
}

func TestProjectRegistrationOutputFailureService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			path := filepath.Join(t.TempDir(), "report.json")
			_, output, err := fixture.command(t, []string{"FATHOMRY_CATALOG_OUTPUT_FAILURE=1", "FATHOMRY_CATALOG_REPORT_PATH=" + path}, "--output=json", "workflow", "register", "--workflow=server-health")
			if err == nil || len(output) != 0 {
				t.Fatal("output refusal became delivery success")
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal("mutation report not retained")
			}
			var retained struct {
				Data publicationCLIReport `json:"data"`
			}
			if err := json.Unmarshal(raw, &retained); err != nil || retained.Data.Effect != "applied" || len(retained.Data.Workflows) != 1 || retained.Data.Workflows[0].Result.Acknowledgement != CommitAcknowledged {
				t.Fatal("output failure erased known commit", err)
			}
			if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "1" {
				t.Fatal("known committed registration disappeared")
			}
		})
	}
}
func TestProjectRegistrationUnknownStopsBatchService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			var address string
			var port uint16
			if dialect == "postgresql" {
				var selected postgres.Settings
				loadPersistenceSettings(t, dialect, &selected)
				address, port = selected.Address, selected.Port
			} else {
				var selected mysql.Settings
				loadPersistenceSettings(t, dialect, &selected)
				address, port = selected.Address, selected.Port
			}
			match := func(payload []byte) bool {
				if dialect == "mysql" {
					return len(payload) > 1 && payload[0] == 3 && strings.EqualFold(string(payload[1:]), "COMMIT")
				}
				return strings.EqualFold(strings.Trim(string(payload), "\x00"), "COMMIT")
			}
			proxy := newCatalogReplyProxy(t, dialect, net.JoinHostPort(address, strconv.Itoa(int(port))), match, true)
			proxied := *fixture
			proxied.port = proxy.port()
			report, raw, err := proxied.command(t, nil, "--output=json", "workflow", "register")
			if err == nil || report.Complete || report.Attempted != 1 || report.Effect != "unknown" || len(report.Workflows) != 2 ||
				report.Workflows[0].Result == nil || report.Workflows[0].Result.Acknowledgement != AcknowledgementUnknown || report.Workflows[1].Result != nil {
				t.Fatalf("unknown commit was replayed or batch continued: %s; %v", raw, err)
			}
			if proxy.commits.Load() != 1 || fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "1" {
				t.Fatal("fault control did not observe exactly one committed aggregate")
			}
		})
	}
}
func TestCatalogUnknownCommitService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			fixture := persistenceService(t, dialect)
			proxied := &persistenceFixture{dialect: dialect, target: fixture.target, quoted: fixture.quoted}
			match := func(payload []byte) bool {
				if dialect == "mysql" {
					return len(payload) > 1 && payload[0] == 3 && strings.EqualFold(string(payload[1:]), "COMMIT")
				}
				return strings.EqualFold(strings.Trim(string(payload), "\x00"), "COMMIT")
			}
			var proxy *catalogReplyProxy
			if dialect == "postgresql" {
				var selected postgres.Settings
				loadPersistenceSettings(t, dialect, &selected)
				proxy = newCatalogReplyProxy(t, dialect, net.JoinHostPort(selected.Address, strconv.Itoa(int(selected.Port))), match, true)
				selected.Port = proxy.port()
				proxied.pg = persistencePostgres(t, selected)
			} else {
				var selected mysql.Settings
				loadPersistenceSettings(t, dialect, &selected)
				proxy = newCatalogReplyProxy(t, dialect, net.JoinHostPort(selected.Address, strconv.Itoa(int(selected.Port))), match, true)
				selected.Port, selected.Database = proxy.port(), fixture.target
				proxied.my = persistenceMySQL(t, selected)
			}
			value := proxied.catalog(t, Options{})
			prepared := preparedPublication(t, registrationInput(t))
			result, err := value.Register(context.Background(), prepared)
			if err == nil || result.Effect != Unknown || result.Acknowledgement != AcknowledgementUnknown || result.DefinitionID != "" || result.Changes != (Changes{}) || !result.CleanupComplete {
				t.Fatal("public result fabricated commit/rollback", result.Effect, err)
			}
			observed, err := value.Compare(context.Background(), prepared)
			if err != nil || !observed.Equal {
				t.Fatal("explicit coherent reconciliation failed", err)
			}
			original, err := value.LastPublication()
			if err != nil || original != result || original.Acknowledgement != AcknowledgementUnknown {
				t.Fatal("state observation rewrote original acknowledgement", err)
			}
			if err := value.Close(context.Background()); err != nil {
				t.Fatal("settled unknown transaction misclassified as cleanup failure", err)
			}
			if proxy.commits.Load() != 1 {
				t.Fatal("catalog automatically retried")
			}
			if fixture.scalar(t, "SELECT count(*) FROM "+fixture.table("workflow_definition")) != "1" {
				t.Fatal("unknown control did not persist")
			}
			retry := fixture.catalog(t, Options{})
			repeated, err := retry.Register(context.Background(), prepared)
			if err != nil || repeated.Effect != Unchanged {
				t.Fatal("explicit equal retry failed", err)
			}
		})
	}
}
