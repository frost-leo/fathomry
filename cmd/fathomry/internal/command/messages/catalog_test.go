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

package messages

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

func fixtureCatalogs(t testing.TB) command.Catalogs {
	t.Helper()
	message := func(id string, kinds map[string]string, cardinal bool, forms map[string]string) map[string]any {
		arguments := map[string]any{}
		for name, kind := range kinds {
			arguments[name] = map[string]string{"kind": kind, "meaning": "Public fixture value."}
		}
		return map[string]any{"id": id, "contract": "v1", "context": "Public fixture message.",
			"args": arguments, "cardinal": cardinal, "forms": forms}
	}
	document := func(module, component, locale string, messages []map[string]any) []byte {
		value, err := json.Marshal(map[string]any{"schema": i18n.Schema, "profile": i18n.Profile,
			"module": module, "component": component, "locale": locale, "messages": messages})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	files := fstest.MapFS{"en.json": &fstest.MapFile{Data: document("example", "source", "en", []map[string]any{
		message("example.source.greeting", map[string]string{"name": "string"}, false, map[string]string{"other": "Hello {name}."}),
		message("example.source.items", map[string]string{"label": "string"}, true, map[string]string{"one": "{count} item at {label}", "other": "{count} items at {label}"}),
		message("example.source.scalars", map[string]string{"label": "string", "signed": "int64", "unsigned": "uint64", "enabled": "bool"},
			false, map[string]string{"other": "{label}: {signed} / {unsigned} / {enabled}"}),
		message("example.source.sparse", map[string]string{}, true, map[string]string{"other": "{count} entries"}),
	})}}
	source := i18n.Component{Module: "example", Name: "source", BaseLocale: "en", Directory: ".", Resources: files}
	baseline, err := i18n.Prepare(source)
	if err != nil {
		t.Fatal(err)
	}
	for locale, translations := range map[string]map[string]map[string]string{
		"fr":    {"greeting": {"other": "Bonjour {name}."}},
		"zh-CN": {"greeting": {"other": "Hello {name} (zh-CN)."}, "items": {"other": "{count} items at {label} (zh-CN)"}},
		"ru":    {"items": {"one": "{count} one at {label}", "few": "{count} few at {label}", "many": "{count} many at {label}", "other": "{count} other at {label}"}},
	} {
		messages := []map[string]any{}
		for name, forms := range translations {
			id := "example.source." + name
			exact, err := baseline.Lookup(id, "en")
			if err != nil || !exact.TranslationExists {
				t.Fatal("fixture baseline missing", err)
			}
			messages = append(messages, map[string]any{"id": id, "source": exact.Definition.SourceDigest, "forms": forms})
		}
		files[locale+".json"] = &fstest.MapFile{Data: document("example", "source", locale, messages)}
	}
	other := i18n.Component{Module: "other", Name: "source", BaseLocale: "fr", Directory: ".",
		Resources: fstest.MapFS{"fr.json": &fstest.MapFile{Data: document("other", "source", "fr", []map[string]any{
			message("other.source.notice", map[string]string{}, false, map[string]string{"other": "Bonjour."}),
		})}}}
	secondary := i18n.Component{Module: "example", Name: "secondary", BaseLocale: "en", Directory: ".",
		Resources: fstest.MapFS{"en.json": &fstest.MapFile{Data: document("example", "secondary", "en", []map[string]any{
			message("example.secondary.notice", map[string]string{}, false, map[string]string{"other": "Secondary notice."}),
		})}}}
	components := append(i18n.CoreComponents(), command.Component(), Component(), source, other, secondary)
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	errorsCatalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	return command.Catalogs{Errors: errorsCatalog, Messages: messages}
}

func execute(catalogs command.Catalogs, args ...string) (string, string, error) {
	var output, diagnostic bytes.Buffer
	err := command.Run(context.Background(), args, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic},
		catalogs, func(invocation *command.Invocation) *cobra.Command {
			root := invocation.Group("fathomry", "fathomry.command_line.root")
			root.AddCommand(New(invocation))
			return root
		})
	return output.String(), diagnostic.String(), err
}

func resultData[T any](t testing.TB, output, operation string) T {
	t.Helper()
	var value struct {
		Schema  string `json:"schema"`
		Command string `json:"command"`
		Data    T      `json:"data"`
	}
	if err := json.Unmarshal([]byte(output), &value); err != nil || value.Schema != command.Schema || value.Command != operation {
		t.Fatalf("invalid %s envelope: %v / %s", operation, err, output)
	}
	return value.Data
}

func TestCommandResources(t *testing.T) {
	own, err := i18n.Prepare(Component())
	if err != nil {
		t.Fatal(err)
	}
	entries, err := own.Inspect()
	if err != nil || len(entries) == 0 {
		t.Fatal("empty command bundle", err)
	}
	for _, value := range entries {
		if value.Component != "message_catalog" || !strings.HasPrefix(value.ID, "fathomry.message_catalog.") {
			t.Fatal("wrong resource owner", value.ID)
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
		catalogs := command.Catalogs{Errors: errorsCatalog, Messages: translations}
		for _, args := range [][]string{{"i18n"}, {"i18n", "list"}, {"i18n", "locales"}, {"i18n", "show", "fathomry.failure.invalid_code"},
			{"i18n", "render", "fathomry.failure.invalid_code"}, {"i18n", "coverage", "zh-CN", "--strict"}} {
			for _, format := range []string{"text", "json"} {
				for _, locale := range []string{"en", "zh-CN"} {
					input := append([]string{"--output", format, "--lang", locale}, args...)
					output, diagnostic, err := execute(catalogs, input...)
					if included {
						if err != nil || diagnostic != "" || output == "" {
							t.Fatal("own bundle not independently sufficient", input, err, diagnostic)
						}
					} else if !errors.Is(err, command.ErrOptions) || errors.Is(err, command.ErrExecution) || output != "" || diagnostic == "" {
						t.Fatal("missing own bundle admitted", input, err, output)
					}
				}
			}
		}
	}
}

func TestCatalogOperationsWithoutCommandResources(t *testing.T) {
	catalog, err := i18n.Prepare(i18n.CoreComponents()...)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	requested := "ZH-cn"
	values, err := listMessages(ctx, catalog, listOptions{owners: command.OwnerFilter{Component: "failure"}, locale: &requested})
	if err != nil || len(values) != len(failure.Definitions()) || values[0].Locale != "zh-CN" || requested != "ZH-cn" {
		t.Fatal("direct list changed input or needs UI resources", err)
	}
	value, err := lookupMessage(ctx, catalog, "fathomry.failure.invalid_code", "fr")
	if err != nil || !value.MessageExists || value.TranslationExists {
		t.Fatal("lookup used fallback", err)
	}
	locales, err := inspectLocales(ctx, catalog, command.OwnerFilter{Component: "failure"})
	if err != nil || len(locales) != 1 || len(locales[0].Locales) != 2 {
		t.Fatal("direct locales need UI resources", err)
	}
	coverage, err := inspectCoverage(ctx, catalog, command.OwnerFilter{Component: "failure"}, "fr")
	if err != nil || coverage.Complete || coverage.Missing != len(failure.Definitions()) {
		t.Fatal("domain incompleteness became an execution error", err)
	}
	rendered, err := renderMessage(ctx, catalog, "fathomry.failure.invalid_code", "en", renderOptions{})
	if err != nil || rendered.Text != failure.Definitions()[0].Message {
		t.Fatal("render needs CLI resources", err)
	}
	empty := ""
	if _, err := listMessages(ctx, catalog, listOptions{locale: &empty}); !errors.Is(err, command.ErrUsage) {
		t.Fatal("explicit empty locale admitted", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := listMessages(canceled, nil, listOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := lookupMessage(canceled, nil, "", ""); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := inspectLocales(canceled, nil, command.OwnerFilter{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := inspectCoverage(canceled, nil, command.OwnerFilter{}, ""); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := renderMessage(canceled, nil, "", "", renderOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestCatalogQueries(t *testing.T) {
	catalogs := fixtureCatalogs(t)
	for _, item := range []struct {
		name  string
		flags []string
		count int
	}{
		{"module", []string{"--module", "example"}, 9},
		{"component", []string{"--component", "source"}, 9},
		{"joint", []string{"--module", "other", "--component", "source"}, 1},
		{"canonical_locale", []string{"--module", "example", "--locale", "ZH-cn"}, 2},
		{"exact_not_fallback", []string{"--module", "example", "--locale", "en-US"}, 0},
		{"identifier_search", []string{"--query", "EXAMPLE.SOURCE.GREETING"}, 3},
		{"pattern_search", []string{"--query", "BONJOUR"}, 2},
		{"empty_literal_search", []string{"--module", "example", "--query", "[missing]"}, 0},
	} {
		t.Run(item.name, func(t *testing.T) {
			args := append([]string{"--output", "json", "i18n", "list"}, item.flags...)
			output, diagnostic, err := execute(catalogs, args...)
			if err != nil || diagnostic != "" {
				t.Fatal(err, diagnostic)
			}
			values := resultData[[]entry](t, output, "i18n.list")
			if values == nil || len(values) != item.count {
				t.Fatalf("wanted %d entries, got %d", item.count, len(values))
			}
		})
	}
	t.Run("exact_lookup_and_text_metadata", func(t *testing.T) {
		output, diagnostic, err := execute(catalogs, "--output", "json", "i18n", "show", "example.source.items", "--locale", "fr")
		if err != nil || diagnostic != "" {
			t.Fatal(err)
		}
		value := resultData[lookup](t, output, "i18n.show")
		if !value.MessageExists || value.TranslationExists || value.Definition != nil || value.RequestedLocale != "fr" || value.CanonicalLocale != "fr" {
			t.Fatal("show hid a missing exact translation", value)
		}
		output, diagnostic, err = execute(catalogs, "i18n", "show", "example.source.items", "--locale", "en")
		for _, field := range []string{"Base language: en", "Message contract: v1", "Arguments: 1",
			"label (string): Public fixture value.", "Cardinal message: true", "{count} item at {label}", "{count} items at {label}"} {
			if err != nil || diagnostic != "" || !strings.Contains(output, field) {
				t.Fatal("show omitted declaration", field, err, output)
			}
		}
	})
	t.Run("component_specific_locales", func(t *testing.T) {
		output, diagnostic, err := execute(catalogs, "--output", "json", "i18n", "locales", "--component", "source")
		if err != nil || diagnostic != "" {
			t.Fatal(err)
		}
		values := resultData[[]localeOwner](t, output, "i18n.locales")
		if len(values) != 2 || values[0].Module != "example" || values[0].BaseLocale != "en" ||
			values[0].Total != 4 || values[1].Module != "other" || values[1].BaseLocale != "fr" ||
			values[1].Total != 1 || len(values[1].Locales) != 1 || values[1].Locales[0] != (localeCount{"fr", 1}) {
			t.Fatal("language support crossed owner boundaries", values)
		}
		counts := map[string]int{"en": 4, "fr": 1, "ru": 1, "zh-CN": 2}
		if len(values[0].Locales) != len(counts) {
			t.Fatal("resource language omitted")
		}
		for _, locale := range values[0].Locales {
			if counts[locale.Locale] != locale.Translated {
				t.Fatal("wrong exact language count", locale)
			}
		}
	})
}

func TestCoverage(t *testing.T) {
	catalogs := fixtureCatalogs(t)
	for _, item := range []struct {
		locale     string
		module     string
		total      int
		translated int
	}{
		{"en", "example", 4, 4}, {"fr", "example", 4, 1}, {"en-US", "example", 4, 0},
		{"ZH-cn", "example", 4, 2}, {"fr", "other", 1, 1}, {"en", "other", 1, 0},
	} {
		for _, strict := range []bool{false, true} {
			name := item.module + "/" + item.locale
			if strict {
				name += "/strict"
			}
			t.Run(name, func(t *testing.T) {
				args := []string{"--output", "json", "i18n", "coverage", item.locale, "--module", item.module, "--component", "source"}
				if strict {
					args = append(args, "--strict")
				}
				output, diagnostic, err := execute(catalogs, args...)
				failed := strict && item.total != item.translated
				if failed {
					if !errors.Is(err, command.ErrCheck) || command.ExitCode(err) != 1 || !json.Valid([]byte(diagnostic)) {
						t.Fatal("strict check status lost", err, diagnostic)
					}
				} else if err != nil || diagnostic != "" {
					t.Fatal("inspection unexpectedly failed", err, diagnostic)
				}
				value := resultData[coverageReport](t, output, "i18n.coverage")
				if value.Total != item.total || value.Translated != item.translated || value.Missing != item.total-item.translated ||
					value.Complete != (value.Missing == 0) || len(value.Components) != 1 || value.Components[0].Missing == nil ||
					len(value.Components[0].Missing) != value.Missing || value.Components[0].Translated != value.Translated {
					t.Fatal("coverage summary differs from exact resources", value)
				}
				if item.locale == "ZH-cn" && value.Locale != "zh-CN" {
					t.Fatal("coverage did not canonicalize")
				}
			})
		}
	}
	t.Run("text_report_survives_failed_check", func(t *testing.T) {
		output, diagnostic, err := execute(catalogs, "i18n", "coverage", "fr", "--module", "example", "--component", "source", "--strict")
		if !errors.Is(err, command.ErrCheck) || diagnostic == "" || !strings.Contains(output, "Translated: 1/4") {
			t.Fatal("text summary discarded", err, output)
		}
		for _, name := range []string{"items", "scalars", "sparse"} {
			if !strings.Contains(output, "example.source."+name) {
				t.Fatal("missing ID absent", name)
			}
		}
	})
}

func TestCatalogRefusals(t *testing.T) {
	catalogs := fixtureCatalogs(t)
	for _, operation := range []string{"list", "locales", "coverage"} {
		for _, item := range []struct {
			flags []string
			code  failure.Code
		}{
			{[]string{"--module", "absent"}, command.ErrNotFound},
			{[]string{"--component", "absent"}, command.ErrNotFound},
			{[]string{"--module", "other", "--component", "secondary"}, command.ErrNotFound},
			{[]string{"--module", "private/value-canary"}, command.ErrUsage},
			{[]string{"--component", "private value-canary"}, command.ErrUsage},
		} {
			args := []string{"--output", "json", "i18n", operation}
			if operation == "coverage" {
				args = append(args, "en", "--strict")
			}
			args = append(args, item.flags...)
			output, diagnostic, err := execute(catalogs, args...)
			if !errors.Is(err, item.code) || output != "" || !json.Valid([]byte(diagnostic)) || strings.Contains(diagnostic, "private") {
				t.Fatal("unknown/invalid owner silently succeeded or leaked", args, err, output, diagnostic)
			}
		}
	}
	for _, locale := range []string{"", "en_US", "und", "en,fr", strings.Repeat("x", 129)} {
		for _, operation := range []string{"list", "show", "render", "coverage"} {
			args := []string{"--output", "json", "i18n", operation}
			if operation == "show" || operation == "render" {
				args = append(args, "other.source.notice")
			}
			if operation == "coverage" {
				args = append(args, locale)
			} else {
				args = append(args, "--locale", locale)
			}
			output, diagnostic, err := execute(catalogs, args...)
			if !errors.Is(err, command.ErrUsage) || output != "" || diagnostic == "" {
				t.Fatal("invalid locale admitted", args, err)
			}
		}
	}
}
