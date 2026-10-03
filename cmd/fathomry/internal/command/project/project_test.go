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

package project

import (
	"bytes"
	"context"
	"errors"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

func repository(t testing.TB) string {
	t.Helper()
	root, err := filepath.Abs("../../../../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}
func testRequest(t testing.TB, mode, encoding string) request {
	t.Helper()
	provider := "viper"
	if mode == "remote" {
		provider = "nacos"
	}
	return request{destination: filepath.Join(t.TempDir(), "space parent", "project"), name: "demo", module: "example.org/demo", mode: mode, provider: provider, encoding: encoding, frameworkSource: repository(t)}
}
func testPlan(t testing.TB, mode, encoding string) plan {
	t.Helper()
	input := testRequest(t, mode, encoding)
	if err := os.MkdirAll(filepath.Dir(input.destination), 0700); err != nil {
		t.Fatal(err)
	}
	tree, err := prepare(input)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}
func testCatalogs(t testing.TB) command.Catalogs {
	t.Helper()
	components := append(i18n.CoreComponents(), command.Component(), Component())
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	catalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	return command.Catalogs{Errors: catalog, Messages: messages}
}
func invoke(t testing.TB, args []string, writer io.Writer) (string, error) {
	t.Helper()
	var diagnostic bytes.Buffer
	err := command.Run(context.Background(), args, command.Options{Input: strings.NewReader(""), Output: writer, ErrorOutput: &diagnostic}, testCatalogs(t), func(invocation *command.Invocation) *cobra.Command {
		root := invocation.Group("fathomry", "fathomry.command_line.root")
		root.AddCommand(New(invocation))
		return root
	})
	return diagnostic.String(), err
}

func TestNew(t *testing.T) {
	t.Run("localized_help_is_offline", func(t *testing.T) {
		for _, locale := range []string{"en", "zh-CN"} {
			var output bytes.Buffer
			diagnostic, err := invoke(t, []string{"new", "--help", "--lang", locale}, &output)
			if err != nil || diagnostic != "" || !strings.Contains(output.String(), "--config-source") || !strings.Contains(output.String(), "--config-provider") {
				t.Fatal("new help unavailable", err)
			}
		}
	})
	t.Run("four_profiles", func(t *testing.T) {
		for _, mode := range []string{"local", "remote"} {
			for _, encoding := range []string{"yaml", "toml"} {
				t.Run(mode+"/"+encoding, func(t *testing.T) {
					input := testRequest(t, mode, encoding)
					if err := os.MkdirAll(filepath.Dir(input.destination), 0700); err != nil {
						t.Fatal(err)
					}
					var output bytes.Buffer
					diagnostic, err := invoke(t, []string{"new", input.destination, "--name=demo", "--module=example.org/demo", "--framework-source", input.frameworkSource, "--config-source", mode, "--config-provider", input.provider, "--config-format", encoding, "--output=json"}, &output)
					if err != nil || diagnostic != "" || !strings.Contains(output.String(), "\"status\":\"created\"") {
						t.Fatal("creation failed", err, diagnostic)
					}
					tree, err := prepare(input)
					if err != nil {
						t.Fatal(err)
					}
					if len(tree.files) != 21 {
						t.Fatal("unexpected project surface")
					}
					for _, file := range tree.files {
						raw, err := os.ReadFile(filepath.Join(input.destination, file.name))
						if err != nil || !bytes.Equal(raw, file.content) {
							t.Fatal("incomplete generated file", file.name, err)
						}
						if strings.HasSuffix(file.name, ".go") {
							if _, err := parser.ParseFile(token.NewFileSet(), file.name, raw, 0); err != nil {
								t.Fatal(err)
							}
						}
					}
					boot, err := os.ReadFile(filepath.Join(input.destination, "internal/bootstrap/boot.go"))
					if err != nil {
						t.Fatal(err)
					}
					for _, forbidden := range []string{"configuration.Bootstrap", "configuration.Startup", "findProvider", "resolveBootstrap", "configuration/file/", "configuration/nacos/"} {
						if bytes.Contains(boot, []byte(forbidden)) {
							t.Fatal("project retained implicit library composition")
						}
					}
					if !bytes.Contains(boot, []byte("configuration.Load(")) || !bytes.Contains(boot, []byte("configuration.Watch(")) {
						t.Fatal("project did not consume the complete Framework scenario")
					}
					for _, file := range tree.files {
						if strings.HasSuffix(file.name, ".go") && (bytes.Contains(file.content, []byte(frameworkModule+"/adapters/")) || bytes.Contains(file.content, []byte(frameworkModule+"/internal/"))) {
							t.Fatal("generated project requires Adapter or Internal imports", file.name)
						}
					}
					if _, err := os.Stat(filepath.Join(input.destination, "go.sum")); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("generation fabricated checksums")
					}
					_, err = invoke(t, []string{"new", input.destination, "--name=demo", "--module=example.org/demo", "--framework-source", input.frameworkSource}, io.Discard)
					if !errors.Is(err, ErrDestination) {
						t.Fatal("existing destination merged", err)
					}
				})
			}
		}
	})
	t.Run("bad_choices_do_not_create_destination", func(t *testing.T) {
		for _, extra := range [][]string{{"--config-source=file"}, {"--config-source=remote"}, {"--config-source=remote", "--config-provider=viper"}, {"--config-format=xml"}, {"--config-provider="}, {"--name=con"}, {"--module=invalid"}} {
			input := testRequest(t, "local", "yaml")
			args := append([]string{"new", input.destination, "--name=demo", "--module=example.org/demo", "--framework-source", input.frameworkSource}, extra...)
			if _, err := invoke(t, args, io.Discard); !errors.Is(err, command.ErrUsage) {
				t.Fatal("invalid options not refused", err)
			}
			if _, err := os.Stat(input.destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid input changed filesystem")
			}
		}
	})
	t.Run("output_failure_retains_completed_project", func(t *testing.T) {
		input := testRequest(t, "local", "yaml")
		_ = os.MkdirAll(filepath.Dir(input.destination), 0700)
		marker := errors.New("output refused")
		_, err := invoke(t, []string{"new", input.destination, "--name=demo", "--module=example.org/demo", "--framework-source", input.frameworkSource}, rejectWriter{marker})
		if !errors.Is(err, command.ErrOutput) || !errors.Is(err, marker) {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(input.destination, "internal/bootstrap/boot.go")); err != nil {
			t.Fatal("completed project removed", err)
		}
	})
	t.Run("module_path_components", func(t *testing.T) {
		for _, module := range []string{"example.org/vendor/demo", "example.org/demo/vendor", "example.org/vendor"} {
			input := testRequest(t, "local", "yaml")
			var output bytes.Buffer
			_, err := invoke(t, []string{"new", input.destination, "--name=demo", "--module=" + module, "--framework-source", filepath.Join(t.TempDir(), "missing")}, &output)
			if !errors.Is(err, command.ErrUsage) || output.Len() != 0 {
				t.Fatal("unsupported module reached dependency preparation or reported creation", err)
			}
			if _, err := os.Lstat(input.destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unsupported module changed destination")
			}
		}
		for _, module := range []string{"example.org/Vendor/demo", "example.org/vendors/demo", "vendor.example.org/demo"} {
			input := testRequest(t, "local", "yaml")
			input.module = module
			if _, err := prepare(input); err != nil {
				t.Fatal("ordinary module component refused", err)
			}
		}
	})
}

type rejectWriter struct{ err error }

func (writer rejectWriter) Write([]byte) (int, error) { return 0, writer.err }

func TestCreation(t *testing.T) {
	t.Run("exclusive_competing_creators", func(t *testing.T) {
		tree := testPlan(t, "local", "yaml")
		results := make(chan error, 2)
		start := make(chan struct{})
		var group sync.WaitGroup
		for range 2 {
			group.Go(func() { <-start; results <- create(context.Background(), tree, writeProjectFile) })
		}
		close(start)
		group.Wait()
		close(results)
		success := 0
		for err := range results {
			if err == nil {
				success++
			} else if !errors.Is(err, ErrDestination) {
				t.Fatal(err)
			}
		}
		if success != 1 {
			t.Fatal("creation was not exclusive")
		}
	})
	t.Run("partial_write_and_cancellation_are_retained", func(t *testing.T) {
		for _, cancelAfterWrite := range []bool{false, true} {
			tree := testPlan(t, "local", "yaml")
			ctx, cancel := context.WithCancelCause(context.Background())
			marker := errors.New("private-write-canary")
			calls := 0
			err := create(ctx, tree, func(root *os.Root, file projectFile) error {
				calls++
				if calls == 2 && !cancelAfterWrite {
					return marker
				}
				if err := writeProjectFile(root, file); err != nil {
					return err
				}
				if cancelAfterWrite {
					cancel(marker)
				}
				return nil
			})
			cancel(nil)
			if !errors.Is(err, ErrPartial) || !errors.Is(err, marker) || strings.Contains(err.Error(), "private-write-canary") {
				t.Fatal("partial effect lost", err)
			}
			if _, err := os.Stat(filepath.Join(tree.destination, tree.files[0].name)); err != nil {
				t.Fatal("partial output removed")
			}
		}
		tree := testPlan(t, "local", "yaml")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := create(ctx, tree, writeProjectFile); !errors.Is(err, command.ErrCanceled) {
			t.Fatal(err)
		}
		if _, err := os.Stat(tree.destination); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("canceled admission created a directory")
		}
	})
	t.Run("existing_file_and_symlink_are_not_overwritten", func(t *testing.T) {
		for _, symlink := range []bool{false, true} {
			tree := testPlan(t, "local", "yaml")
			target := tree.destination
			if symlink {
				target = filepath.Join(filepath.Dir(target), "existing")
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, tree.destination); err != nil {
					t.Skip("symlink permission unavailable")
				}
			} else if err := os.WriteFile(target, []byte("original"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := create(context.Background(), tree, writeProjectFile); !errors.Is(err, ErrDestination) {
				t.Fatal(err)
			}
			if !symlink {
				raw, _ := os.ReadFile(target)
				if string(raw) != "original" {
					t.Fatal("existing data overwritten")
				}
			}
		}
	})
}

func TestProjectNames(t *testing.T) {
	for _, name := range []string{"demo", "a-1", "orderflow"} {
		if !validProjectName(name) {
			t.Fatal(name)
		}
	}
	for _, name := range []string{"", "../escape", "Upper", "con", "lpt1", "vendor", "testdata", "space name"} {
		if validProjectName(name) {
			t.Fatal("invalid project name admitted")
		}
	}
}
