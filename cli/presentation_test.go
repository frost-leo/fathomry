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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

func TestInvalidPresentationPreparationPreventsEffects(t *testing.T) {
	for _, mode := range []string{"invalid-resource", "missing-binding", "missing-project-resource"} {
		calls := 0
		target := filepath.Join(t.TempDir(), "effect")
		status, err, output, _ := capture(context.Background(), []string{"leaf"}, func(words *text) *cobra.Command {
			if mode == "invalid-resource" {
				catalog, err := i18n.Prepare(i18n.Source{Name: "invalid", Data: []byte(`{"schema":"unknown"}`)})
				words.catalog = catalog
				words.retain(err)
			} else if mode == "missing-project-resource" {
				data, err := resources.ReadFile("resources/en.json")
				if err != nil {
					t.Fatal(err)
				}
				words.catalog, err = i18n.Prepare(i18n.Source{Name: "root", Data: data})
				if err != nil {
					t.Fatal(err)
				}
			}
			root := commands(words)
			if mode == "missing-binding" {
				root.Annotations["fathomry.short.id"] = "missing"
			}
			root.AddCommand(&cobra.Command{Use: "leaf", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error {
				calls++
				return os.WriteFile(target, []byte("accepted"), 0600)
			}})
			return root
		})
		if status != 1 || err == nil || calls != 0 || output != "" {
			t.Fatal(mode, status, err, calls, output)
		}
		if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("effect before preparation", err)
		}
	}
}

func TestIgnoredHardHelpUsageFailureAfterEffect(t *testing.T) {
	for _, mode := range []string{"help", "usage"} {
		for _, brokenCatalog := range []bool{false, true} {
			for _, withCauses := range []bool{false, true} {
				t.Run(mode+fmtBool(brokenCatalog)+fmtBool(withCauses), func(t *testing.T) {
					effect := filepath.Join(t.TempDir(), "accepted")
					operation, cleanup, writeErr := errors.New("operation-CANARY"), errors.New("cleanup-CANARY"), errors.New("diagnostic-CANARY")
					calls, closes, writes := 0, 0, 0
					var output bytes.Buffer
					status, err := run(context.Background(), []string{"leaf"}, Streams{
						Stdin: strings.NewReader(""), Stdout: &output, Stderr: writerFunc(func(data []byte) (int, error) {
							writes++
							if len(data) > 96 || bytes.Contains(data, []byte("CANARY")) {
								t.Fatal("unsafe/unbounded diagnostic")
							}
							if brokenCatalog && string(data) != "fathomry.cli.presentation_failed\n" {
								t.Fatal("wrong final fallback", string(data))
							}
							return 0, writeErr
						}),
					}, func(words *text) *cobra.Command {
						root := commands(words)
						root.AddCommand(&cobra.Command{Use: "leaf", Args: cobra.NoArgs, RunE: func(command *cobra.Command, _ []string) error {
							calls++
							defer func() { closes++ }()
							if err := os.WriteFile(effect, []byte("accepted"), 0600); err != nil {
								return err
							}
							if brokenCatalog {
								words.catalog = nil
							} else {
								command.Annotations = map[string]string{"fathomry.short.id": "missing"}
							}
							if mode == "help" {
								_ = command.Help()
							} else {
								_ = command.Usage()
							}
							// A second ignored native call must not hide the first presentation error.
							_ = command.Help()
							if withCauses {
								return errors.Join(operation, cleanup, context.Canceled)
							}
							return nil
						}})
						return root
					})
					expected := i18n.ErrMessage
					if brokenCatalog {
						expected = i18n.ErrCatalog
					}
					if status != 1 || !errors.Is(err, expected) || !errors.Is(err, writeErr) || writes != 1 || calls != 1 || closes != 1 || output.Len() != 0 {
						t.Fatal(status, err, writes, calls, closes)
					}
					if withCauses && (!errors.Is(err, operation) || !errors.Is(err, cleanup) || !errors.Is(err, context.Canceled)) {
						t.Fatal("original causes lost", err)
					}
					if data, err := os.ReadFile(effect); err != nil || string(data) != "accepted" {
						t.Fatal("effect lost", err)
					}
				})
			}
		}
	}
}

type writerFunc func([]byte) (int, error)

func (write writerFunc) Write(data []byte) (int, error) { return write(data) }
func fmtBool(value bool) string {
	if value {
		return "-yes"
	}
	return "-no"
}

func TestHardPresentationIsIndependentOfUsage(t *testing.T) {
	status, err, _, _ := capture(context.Background(), []string{"leaf"}, func(words *text) *cobra.Command {
		root := commands(words)
		root.AddCommand(&cobra.Command{Use: "leaf", RunE: func(*cobra.Command, []string) error { t.Fatal("operation ran"); return nil }, Args: func(command *cobra.Command, _ []string) error {
			command.Annotations = map[string]string{"fathomry.short.id": "missing"}
			_ = command.Help()
			return errArguments
		}})
		return root
	})
	if status != 1 || !errors.Is(err, ErrUsage) || !errors.Is(err, i18n.ErrMessage) {
		t.Fatal(status, err)
	}
}

// Fault injection changes only the selected language reader, not the operation.
// Root parsing has already completed; project rendering still runs after create.
type invalidDisplayLanguage struct{}

func (invalidDisplayLanguage) String() string   { return "invalid_locale" }
func (invalidDisplayLanguage) Set(string) error { return nil }
func (invalidDisplayLanguage) Type() string     { return "string" }

func TestActualProjectRenderAndDiagnosticFailureChain(t *testing.T) {
	source, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	for _, validSource := range []bool{false, true} {
		for _, failedDiagnostic := range []bool{false, true} {
			target := filepath.Join(t.TempDir(), "app")
			selectedSource := source
			if !validSource {
				selectedSource = filepath.Join(t.TempDir(), "missing")
			}
			writerCause := errors.New("diagnostic-CANARY")
			calls, writes := 0, 0
			var output, diagnostic bytes.Buffer
			diagnosticWriter := writerFunc(func(data []byte) (int, error) {
				writes++
				if bytes.Contains(data, []byte("CANARY")) || bytes.Contains(data, []byte(selectedSource)) {
					t.Fatal("native error leaked")
				}
				if failedDiagnostic {
					return 0, writerCause
				}
				return diagnostic.Write(data)
			})
			status, err := run(context.Background(), []string{"new", target, "--module=example.org/app", "--fathomry-source=" + selectedSource}, Streams{strings.NewReader(""), &output, diagnosticWriter}, func(words *text) *cobra.Command {
				root := commands(words)
				command, _, err := root.Find([]string{"new"})
				if err != nil {
					t.Fatal(err)
				}
				execute := command.RunE
				command.RunE = func(command *cobra.Command, args []string) error {
					calls++
					command.Flag("lang").Value = invalidDisplayLanguage{}
					return execute(command, args)
				}
				return root
			})
			if status != 1 || !errors.Is(err, i18n.ErrLocale) || calls != 1 || writes != 1 || output.Len() != 0 {
				t.Fatal(status, err, calls, writes)
			}
			if failedDiagnostic && !errors.Is(err, writerCause) {
				t.Fatal("writer cause lost")
			}
			if validSource {
				if errors.Is(err, ErrProjectCreation) {
					t.Fatal("completed creation was reclassified as incomplete")
				}
				for _, name := range []string{"go.mod", "main.go", "README.md", ".gitignore"} {
					if data, err := os.ReadFile(filepath.Join(target, name)); err != nil || len(data) == 0 {
						t.Fatal("complete effect lost", name, err)
					}
				}
			} else {
				if !errors.Is(err, ErrProjectSource) {
					t.Fatal("project source condition lost")
				}
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatal("source cause lost")
				}
				if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("failed preparation created target")
				}
			}
		}
	}
}

func TestRawOccurrenceTreesUseHostGenericPresentation(t *testing.T) {
	first, _ := failure.New("example.first.failed")
	second, _ := failure.New("example.second.failed")
	for _, operation := range []error{first, errors.Join(first, second), errors.Join(second, first), errors.New("PRIVATE-CANARY")} {
		status, err, _, diagnostic := capture(context.Background(), []string{"leaf", "--lang=zh-CN"}, func(words *text) *cobra.Command {
			root := commands(words)
			root.AddCommand(&cobra.Command{Use: "leaf", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error { return operation }})
			return root
		})
		words := newText()
		words.language = "zh-CN"
		if status != 1 || !errors.Is(err, operation) || diagnostic != words.get("failed")+"\n" || strings.Contains(diagnostic, "example.") || strings.Contains(diagnostic, "PRIVATE") {
			t.Fatal(status, err, diagnostic)
		}
	}
}
