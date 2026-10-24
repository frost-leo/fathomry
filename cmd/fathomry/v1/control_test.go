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
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	createcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create"
	schema "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema"
	"github.com/frost-leo/fathomry/failure/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"github.com/spf13/cobra"
)

func requireControlBorrow(t *testing.T, invocation *command.Invocation, name string) {
	t.Helper()
	scope, err := invocation.Scope()
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := scope.Inspect()
	if err != nil || len(bindings) != 1 {
		t.Fatal("command did not register exactly one owned binding", err)
	}
	binding := bindings[0]
	if binding.Name != name || binding.Policy != resource.Fixed || !binding.Active || binding.Borrowers != 1 {
		t.Fatal("command entered without its fixed resource lease")
	}
}

func TestControlCompositionOwnsDatabase(t *testing.T) {
	for _, failed := range []bool{false, true} {
		selected := resolverSettings("mysql")
		calls := 0
		var owner *schema.Runner
		var owned *resource.Scope
		cause := errors.New("private-operation-canary")
		var output, diagnostic bytes.Buffer
		err := invoke(context.Background(), []string{"probe", "--output=json"}, Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic},
			func(invocation *command.Invocation) *cobra.Command {
				root := invocation.Group("fathomry", "fathomry.command_line.root")
				leaf := &cobra.Command{Use: "probe", Short: "fathomry.command_line.root"}
				invocation.Bind(leaf, func(ctx context.Context, _ []string) error {
					return withDatabase(ctx, invocation, func(context.Context) (DatabaseConfiguration, error) {
						calls++
						return selected, nil
					},
						func(value *schema.Runner) error {
							requireControlBorrow(t, invocation, "control-schema")
							owner = value
							owned, _ = invocation.Scope()
							if failed {
								return command.Fail(command.ErrExecution, cause)
							}
							return nil
						})
				})
				root.AddCommand(leaf)
				return root
			})
		if calls != 1 || owner == nil || !owner.Released() || owned == nil || !owned.ShutdownComplete() {
			t.Fatal("entry lost database ownership", err)
		}

		if failed {
			var retained *ExecutionError
			if !errors.Is(err, cause) || !errors.As(err, &retained) || !retained.CleanupComplete() {
				t.Fatal("failed entry lost cleanup or original cause", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(diagnostic.String(), "private-") {
			t.Fatal("entry exposed private causes")
		}
	}
}

func TestControlCatalogBudgetDiagnostics(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		for _, limit := range []string{"rows", "bytes"} {
			for _, locale := range []string{"en", "zh-CN"} {
				t.Run(dialect+"/"+limit+"/"+locale, func(t *testing.T) {
					selected := resolverSettings(dialect)
					if dialect == "postgresql" {
						if limit == "rows" {
							selected.PostgreSQL.MaxRows = 1
						} else {
							selected.PostgreSQL.MaxResultBytes = 1024
						}
					} else if limit == "rows" {
						selected.MySQL.MaxRows = 1
					} else {
						selected.MySQL.MaxResultBytes = 1024
					}
					var output, diagnostic bytes.Buffer
					err := invoke(context.Background(), []string{"probe", "--output=json"}, Options{Language: locale, Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic}, func(invocation *command.Invocation) *cobra.Command {
						root := invocation.Group("fathomry", "fathomry.command_line.root")
						leaf := &cobra.Command{Use: "probe", Short: "fathomry.command_line.root"}
						invocation.Bind(leaf, func(ctx context.Context, _ []string) error {
							return withCatalog(ctx, invocation, func(context.Context) (DatabaseConfiguration, error) { return selected, nil }, 0, 2, nil)
						})
						root.AddCommand(leaf)
						return root
					})
					core, known := failure.Inspect(err)
					if !known || core.Diagnostic().Definition.Identifier != "fathomry.database_control.limit_exceeded" || !errors.Is(err, databasecmd.ErrLimit) || output.Len() != 0 {
						t.Fatal("catalog admission lost its registered diagnostic", err)
					}
					message := "A control database admission limit was exceeded."
					if locale == "zh-CN" {
						message = "控制数据库操作超出准入限制。"
					}
					if !strings.Contains(diagnostic.String(), message) {
						t.Fatal("catalog admission diagnostic was not localized")
					}
				})
			}
		}
	}
}

func TestAdministrativeFileLoading(t *testing.T) {
	for name, content := range map[string]string{
		"admin.json": `{"user":"admin","password":"private-admin-canary"}`,
		"admin.yaml": "user: admin\npassword: private-admin-canary\n",
		"admin.toml": "user='admin'\npassword='private-admin-canary'\n",
	} {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		value, err := loadAdministration(context.Background(), path)
		if err != nil || value.User != "admin" || value.Password != "private-admin-canary" {
			t.Fatal("explicit admin file did not load", err)
		}
	}
	for _, content := range []string{`{"user":"admin","address":"private-canary"}`, `{"user":"admin","user":"other"}`, `{"user":null}`, `[]`} {
		path := filepath.Join(t.TempDir(), "invalid.json")
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadAdministration(context.Background(), path); err == nil {
			t.Fatal("invalid or redirecting admin configuration accepted")
		}
	}
}

func TestControlCompositionOwnsProvisioner(t *testing.T) {
	selected := resolverSettings("mysql")
	path := filepath.Join(t.TempDir(), "admin.json")
	if err := os.WriteFile(path, []byte(`{"user":"admin","password":"private-admin-canary"}`), 0600); err != nil {
		t.Fatal(err)
	}
	var owner *createcmd.Creator
	var owned *resource.Scope
	var output, diagnostic bytes.Buffer
	err := invoke(context.Background(), []string{"probe"}, Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic}, func(invocation *command.Invocation) *cobra.Command {
		root := invocation.Group("fathomry", "fathomry.command_line.root")
		leaf := &cobra.Command{Use: "probe", Short: "fathomry.command_line.root"}
		invocation.Bind(leaf, func(ctx context.Context, _ []string) error {
			return withProvisioner(ctx, invocation, func(context.Context) (DatabaseConfiguration, error) {
				return selected, nil
			}, path, func(value *createcmd.Creator) error {
				requireControlBorrow(t, invocation, "control-create")
				owner = value
				owned, _ = invocation.Scope()
				return nil
			})
		})
		root.AddCommand(leaf)
		return root
	})
	if err != nil || owner == nil || !owner.Released() || owned == nil || !owned.ShutdownComplete() {
		t.Fatal("provisioning scope abandoned ownership", err)
	}

}

func TestControlClientsBorrowResourceGenerations(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					connection, err := listener.Accept()
					if err != nil {
						return
					}
					_ = connection.Close()
				}
			}()
			t.Cleanup(func() { listener.Close(); <-done })
			selected, err := normalizeDatabase(resolverSettings(dialect))
			if err != nil {
				t.Fatal(err)
			}
			port := uint16(listener.Addr().(*net.TCPAddr).Port)
			if selected.PostgreSQL != nil {
				selected.PostgreSQL.Port = port
			} else {
				selected.MySQL.Port = port
			}
			policy, err := databasePolicy(selected)
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := framework.New(context.Background(), framework.Options{Operations: policy.Runtime})
			if err != nil {
				t.Fatal(err)
			}
			observed := make(chan adapters.Info, 8)
			var ping func(context.Context) error
			var lazy func(context.Context) bool
			var finish func(context.Context) error
			if dialect == "postgresql" {
				inbox, err := adapters.NewInbox[postgres.Result](policy.Evidence)
				if err != nil {
					t.Fatal(err)
				}
				receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(_ context.Context, value adapters.Snapshot[postgres.Result]) error {
					observed <- value.Info()
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				finish = receiver.Finish
				client, err := bindPostgres(context.Background(), runtime.Resources(), false, policy.Budget, postgres.Dependencies{Runtime: runtime.Operations(), Evidence: inbox})
				if err != nil {
					t.Fatal(err)
				}
				ping = func(ctx context.Context) error { _, err := client.Ping(ctx); return err }
				lazy = func(ctx context.Context) bool {
					stats, err := client.Stats(ctx)
					return err == nil && stats.TotalResources == 0
				}
			} else {
				inbox, err := adapters.NewInbox[mysql.Result](policy.Evidence)
				if err != nil {
					t.Fatal(err)
				}
				receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(_ context.Context, value adapters.Snapshot[mysql.Result]) error {
					observed <- value.Info()
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				finish = receiver.Finish
				client, err := bindMySQL(context.Background(), runtime.Resources(), false, policy.Budget, mysql.Dependencies{Runtime: runtime.Operations(), Evidence: inbox})
				if err != nil {
					t.Fatal(err)
				}
				ping = func(ctx context.Context) error { _, err := client.Ping(ctx); return err }
				lazy = func(ctx context.Context) bool {
					stats, err := client.Stats(ctx)
					return err == nil && stats.OpenConnections == 0
				}
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := runtime.Close(ctx); err != nil || !runtime.Resources().ShutdownComplete() {
					t.Error("resource shutdown incomplete", err)
				}
				if err := finish(ctx); err != nil {
					t.Error("source evidence not drained", err)
				}
			})
			snapshot, err := settings.New(databaseSelection{target: selected}, databaseSelection.copy)
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
			if !lazy(context.Background()) {
				t.Fatal("resource construction dialed")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := ping(ctx); err == nil {
				t.Fatal("closed protocol peer reported readiness")
			}
			select {
			case info := <-observed:
				if !strings.HasSuffix(info.Operation, ".ping") || info.Source.Name != "control-target" || info.Source.Generation != 1 || !info.Released {
					t.Fatal("SQL bypassed the Resource lease")
				}
			case <-ctx.Done():
				t.Fatal("released operation evidence missing")
			}
			bindings, err := runtime.Resources().Inspect()
			if err != nil || len(bindings) != 1 || bindings[0].Borrowers != 0 || !bindings[0].Active {
				t.Fatal("operation leaked or closed its source", err)
			}
		})
	}
}

type pendingControl struct {
	pending bool
	cause   error
}

func (value *pendingControl) Close(context.Context) error { return value.cause }
func (value *pendingControl) Released() bool              { return !value.pending }

func TestControlCleanupContinuation(t *testing.T) {
	selected, err := normalizeDatabase(resolverSettings("mysql"))
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("retained-cleanup-canary")
	operation := &pendingControl{pending: true, cause: cause}
	instance, err := buildControl(context.Background(), databaseSelection{target: selected, workspace: "test.workspace", workBytes: 1 << 20}, func(clients controlClients) (*pendingControl, error) {
		stats, err := clients.mysql.Stats(context.Background())
		if err != nil || stats.OpenConnections != 0 {
			t.Fatal("construction dialed", err)
		}
		return operation, nil
	})
	if err != nil || instance == nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		operation.pending = false
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = instance.Release(ctx)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := instance.Release(ctx)
	if first.Complete || !errors.Is(first.Err, cause) {
		t.Fatal("pending cleanup was erased", first.Err)
	}
	operation.pending = false
	for range 2 {
		final := instance.Release(ctx)
		if !final.Complete || !errors.Is(final.Err, cause) {
			t.Fatal("completed cleanup evidence was lost", final.Err)
		}
	}
}
