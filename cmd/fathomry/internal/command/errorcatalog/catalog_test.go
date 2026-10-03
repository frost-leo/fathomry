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

package errorcatalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

func TestCommandResources(t *testing.T) {
	own, err := i18n.Prepare(Component())
	if err != nil {
		t.Fatal(err)
	}
	entries, err := own.Inspect()
	if err != nil || len(entries) == 0 {
		t.Fatal("command resource bundle is empty", err)
	}
	for _, value := range entries {
		if value.Component != "error_catalog" || !strings.HasPrefix(value.ID, "fathomry.error_catalog.") {
			t.Fatal("command does not own its resource identity", value.ID)
		}
	}
	for _, included := range []bool{false, true} {
		components := append(i18n.CoreComponents(), command.Component())
		if included {
			components = append(components, Component())
		}
		var definitions []failure.Definition
		for _, component := range components {
			definitions = append(definitions, component.Definitions...)
		}
		errorsCatalog, err := failure.Prepare(definitions...)
		if err != nil {
			t.Fatal(err)
		}
		translations, err := i18n.Prepare(components...)
		if err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"error"}, {"error", "list"}, {"error", "components"}, {"error", "explain", "0xA0010001"}} {
			for _, format := range []string{"text", "json"} {
				for _, locale := range []string{"en", "zh-CN"} {
					var output, diagnostic bytes.Buffer
					input := append([]string{"--output", format, "--lang", locale}, args...)
					err := command.Run(context.Background(), input, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic},
						command.Catalogs{Errors: errorsCatalog, Messages: translations}, func(invocation *command.Invocation) *cobra.Command {
							root := invocation.Group("fathomry", "fathomry.command_line.root")
							root.AddCommand(New(invocation))
							return root
						})
					if included {
						if err != nil || diagnostic.Len() != 0 || output.Len() == 0 {
							t.Fatal("own bundle was not independently sufficient", input, err, diagnostic.String())
						}
					} else {
						if !errors.Is(err, command.ErrOptions) || errors.Is(err, command.ErrExecution) || output.Len() != 0 || diagnostic.Len() == 0 {
							t.Fatal("missing own bundle was admitted", input, err, output.String())
						}
					}
				}
			}
		}
	}
}

func TestCatalogOperationsWithoutCommandResources(t *testing.T) {
	definitions := failure.Definitions()
	errorsCatalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	translations, err := i18n.Prepare(i18n.CoreComponents()...)
	if err != nil {
		t.Fatal(err)
	}
	catalogs := command.Catalogs{Errors: errorsCatalog, Messages: translations}
	ctx := context.Background()
	values, err := listErrors(ctx, catalogs, listOptions{locale: "en", query: "INVALID_CODE"})
	if err != nil || len(values) != 1 || values[0].Definition.Code != failure.ErrCode {
		t.Fatal("list depends on CLI assembly", err)
	}
	value, err := explainError(ctx, catalogs, "0xA0010001", "zh-CN")
	if err != nil || value.Locale != "zh-CN" || value.Definition.Code != failure.ErrCode {
		t.Fatal("explanation depends on CLI assembly", err)
	}
	owners, err := listComponents(ctx, errorsCatalog, command.OwnerFilter{Component: "failure"})
	if err != nil || len(owners) != 1 {
		t.Fatal("component operation depends on UI resources", err)
	}
	if _, err := listErrors(ctx, catalogs, listOptions{locale: ""}); !errors.Is(err, command.ErrUsage) {
		t.Fatal("direct list skipped admission", err)
	}
	if _, err := explainError(ctx, catalogs, "invalid-private-value", "en"); !errors.Is(err, command.ErrUsage) {
		t.Fatal("direct explanation skipped admission", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := listErrors(canceled, command.Catalogs{}, listOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := explainError(canceled, command.Catalogs{}, "", ""); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := listComponents(canceled, nil, command.OwnerFilter{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestExtensionCatalog(t *testing.T) {
	definitions := []failure.Definition{
		{Code: 0xA4410001, Identifier: "example.source.rejected", Module: "example", Component: "source", Revision: 3,
			Message: "The fixture input was rejected.", Description: "Only empty fixture input is refused.",
			Details: failure.Contract{ID: "example.source.details", Version: 2}},
		{Code: 0xA4420001, Identifier: "other.source.rejected", Module: "other", Component: "source", Revision: 1, Message: "The other fixture was rejected."},
		{Code: 0xA4430001, Identifier: "example.secondary.failed", Module: "example", Component: "secondary", Revision: 1, Message: "The secondary fixture failed."},
	}
	components := append(i18n.CoreComponents(), command.Component(), Component())
	for _, definition := range definitions {
		data, err := json.Marshal(map[string]any{
			"schema": i18n.Schema, "profile": i18n.Profile, "module": definition.Module, "component": definition.Component, "locale": "en",
			"messages": []any{map[string]any{"id": string(definition.Identifier), "contract": "v1", "context": "Static fixture explanation.",
				"args": map[string]any{}, "cardinal": false, "forms": map[string]string{"other": definition.Message}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		components = append(components, i18n.Component{Module: definition.Module, Name: definition.Component, BaseLocale: "en",
			Directory: ".", Resources: fstest.MapFS{"en.json": &fstest.MapFile{Data: data}}, Definitions: []failure.Definition{definition}})
	}
	all := []failure.Definition{}
	for _, component := range components {
		all = append(all, component.Definitions...)
	}
	errorsCatalog, err := failure.Prepare(all...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	execute := func(args ...string) (string, string, error) {
		var output, diagnostic bytes.Buffer
		err := command.Run(context.Background(), args,
			command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic},
			command.Catalogs{Errors: errorsCatalog, Messages: messages}, func(invocation *command.Invocation) *cobra.Command {
				root := invocation.Group("fathomry", "fathomry.command_line.root")
				root.AddCommand(New(invocation))
				return root
			})
		return output.String(), diagnostic.String(), err
	}
	for _, operation := range []string{"list", "components"} {
		for _, item := range []struct {
			flags []string
			count int
		}{
			{[]string{"--module", "example"}, 2}, {[]string{"--component", "source"}, 2},
			{[]string{"--module", "other", "--component", "source"}, 1},
		} {
			args := append([]string{"--output", "json", "error", operation}, item.flags...)
			output, diagnostic, err := execute(args...)
			var result struct {
				Data []json.RawMessage `json:"data"`
			}
			if err != nil || diagnostic != "" || json.Unmarshal([]byte(output), &result) != nil || len(result.Data) != item.count {
				t.Fatal("independent owner selection failed", args, err, output)
			}
		}
		output, _, err := execute("error", operation, "--module", "other", "--component", "secondary")
		if !errors.Is(err, command.ErrNotFound) || output != "" {
			t.Fatal("unknown intersection succeeded", operation, err)
		}
	}
	t.Run("description_search_and_baseline_preservation", func(t *testing.T) {
		output, diagnostic, err := execute("--output", "json", "--lang", "zh-CN", "error", "list", "--query", "EMPTY FIXTURE")
		var result struct {
			Data []explanation `json:"data"`
		}
		if err != nil || diagnostic != "" || json.Unmarshal([]byte(output), &result) != nil || len(result.Data) != 1 {
			t.Fatal(err, output)
		}
		value := result.Data[0]
		if value.Definition != definitions[0] || value.Domain != failure.DomainConfiguration || value.Facility != 0x441 ||
			value.Number != 1 || value.RequestedLocale != "zh-CN" || value.Locale != "en" || value.Fallback != "unsupported-locale" {
			t.Fatal("extension contract or fallback lost", value)
		}
	})
	t.Run("text_details_and_revision", func(t *testing.T) {
		output, diagnostic, err := execute("error", "explain", "example.source.rejected")
		for _, field := range []string{"Contract revision: 3", "Details contract: example.source.details / 2",
			"Baseline description: Only empty fixture input is refused.", "Facility: 0x441"} {
			if err != nil || diagnostic != "" || !strings.Contains(output, field) {
				t.Fatal("missing text metadata", field, err, output)
			}
		}
	})
}
