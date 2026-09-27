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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/cli/internal/command/project"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

func customizedCatalog(t *testing.T) *i18n.Catalog {
	t.Helper()
	sources, err := project.Sources()
	if err != nil {
		t.Fatal(err)
	}
	var source map[string]any
	if err := json.Unmarshal(sources[0].Data, &source); err != nil {
		t.Fatal(err)
	}
	for _, raw := range source["messages"].([]any) {
		message := raw.(map[string]any)
		switch message["id"] {
		case "fathomry.cli.project:sourceUnavailable":
			message["forms"].(map[string]any)["other"] = "source-condition-canary"
		case "fathomry.cli.project:destinationExists":
			message["forms"].(map[string]any)["other"] = "destination-condition-canary"
		}
	}
	sources[0].Data, err = json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately omit stale auxiliary definitions; the loader never drops them.
	sources = sources[:1]
	for _, name := range []string{"en", "zh-cn"} {
		data, err := resources.ReadFile("resources/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, i18n.Source{Name: "root." + name, Data: data})
	}
	catalog, err := i18n.Prepare(sources...)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestActualConditionBindingsAndEnglishFallback(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	custom := customizedCatalog(t)
	for _, locale := range []string{"en", "zh-CN"} {
		for _, kind := range []string{"source", "exists"} {
			target := filepath.Join(t.TempDir(), "app")
			source, condition, canary := root, ErrProjectSource, "source-condition-canary"
			if kind == "source" {
				source = filepath.Join(t.TempDir(), "missing")
			} else {
				condition, canary = ErrProjectExists, "destination-condition-canary"
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
			}
			args := []string{"new", target, "--module=example.org/app", "--fathomry-source=" + source, "--lang=" + locale}
			status, returned, _, diagnostic := capture(context.Background(), args, func(words *text) *cobra.Command {
				words.catalog = custom
				return commands(words)
			})
			ownedOccurrence(t, returned, condition)
			if status != 1 || !strings.Contains(diagnostic, canary) {
				t.Fatal(kind, status, returned, diagnostic)
			}
			selection, err := custom.Resolve("fathomry.cli.project:"+map[string]string{"source": "sourceUnavailable", "exists": "destinationExists"}[kind], locale)
			if err != nil {
				t.Fatal(err)
			}
			metadata, _ := selection.Metadata()
			if metadata.Resource.Locale != "en" || locale == "zh-CN" && metadata.Fallback != i18n.MissingTranslation {
				t.Fatal("false fallback provenance", metadata)
			}
			if kind == "source" {
				if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("unexpected creation")
				}
			}
		}
	}
}

func TestSameCodeDistinctOccurrencesStayDistinct(t *testing.T) {
	first, _ := failure.New("example.operation.failed", errors.New("first"))
	second, _ := failure.New("example.operation.failed", errors.New("second"))
	for _, supplied := range []error{first, second, errors.Join(first, second), errors.Join(second, first)} {
		status, returned, _, _ := capture(context.Background(), []string{"leaf"}, func(words *text) *cobra.Command {
			root := commands(words)
			root.AddCommand(&cobra.Command{Use: "leaf", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error { return supplied }})
			return root
		})
		if status != 1 || returned != supplied {
			t.Fatal("semantic owner occurrence or opaque aggregate replaced")
		}
	}
}
