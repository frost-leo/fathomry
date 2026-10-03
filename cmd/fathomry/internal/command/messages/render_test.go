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
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func TestRendering(t *testing.T) {
	catalogs := fixtureCatalogs(t)
	for _, item := range []struct {
		name     string
		id       string
		flags    []string
		text     string
		locale   string
		fallback string
	}{
		{"comma_and_equal", "example.source.greeting", []string{"--arg", "name=a,b=c"}, "Hello a,b=c.", "en", ""},
		{"empty_string", "example.source.greeting", []string{"--arg", "name="}, "Hello .", "en", ""},
		{"explicit_language", "example.source.greeting", []string{"--arg", "name=Ada", "--locale", "fr"}, "Bonjour Ada.", "fr", ""},
		{"canonical_language", "example.source.greeting", []string{"--arg", "name=Ada", "--locale", "ZH-cn"}, "Hello Ada (zh-CN).", "zh-CN", ""},
		{"matched_not_exact", "example.source.greeting", []string{"--arg", "name=Ada", "--locale", "en-US"}, "Hello Ada.", "en", ""},
		{"missing_translation", "example.source.items", []string{"--arg", "label=home", "--count", "2", "--locale", "fr"}, "2 items at home", "en", "missing-translation"},
		{"unsupported_language", "example.source.greeting", []string{"--arg", "name=Ada", "--locale", "de"}, "Hello Ada.", "en", "unsupported-locale"},
		{"non_english_baseline", "other.source.notice", []string{"--locale", "de"}, "Bonjour.", "fr", "unsupported-locale"},
		{"owner_isolation", "example.secondary.notice", []string{"--locale", "fr"}, "Secondary notice.", "en", "unsupported-locale"},
		{"decimal_count", "example.source.items", []string{"--arg", "label=home", "--count", "010"}, "10 items at home", "en", ""},
		{"full_scalar_precision", "example.source.scalars", []string{"--arg", "label=value", "--arg", "signed=-9223372036854775808",
			"--arg", "unsigned=18446744073709551615", "--arg", "enabled=true"}, "value: -9223372036854775808 / 18446744073709551615 / true", "en", ""},
		{"decimal_scalars", "example.source.scalars", []string{"--arg", "label=", "--arg", "signed=9223372036854775807",
			"--arg", "unsigned=010", "--arg", "enabled=false"}, ": 9223372036854775807 / 10 / false", "en", ""},
	} {
		t.Run(item.name, func(t *testing.T) {
			args := append([]string{"--output", "json", "i18n", "render", item.id}, item.flags...)
			output, diagnostic, err := execute(catalogs, args...)
			if err != nil || diagnostic != "" {
				t.Fatal(err, diagnostic)
			}
			value := resultData[rendered](t, output, "i18n.render")
			if value.ID != item.id || value.Text != item.text || value.Locale != item.locale || value.Fallback != item.fallback ||
				value.RequestedLocale == "" || value.CanonicalLocale == "" || value.MatchedLocale == "" {
				t.Fatal("rendering differs from the declared resource", value)
			}
			if item.name == "canonical_language" && (value.RequestedLocale != "ZH-cn" || value.CanonicalLocale != "zh-CN") {
				t.Fatal("requested and canonical locales collapsed", value)
			}
		})
	}
	t.Run("plural_selection_and_full_uint64", func(t *testing.T) {
		for _, locale := range []string{"en", "ru", "zh-CN"} {
			for _, count := range []uint64{0, 1, 2, 11, 21, 1<<53 + 1, 1<<64 - 1} {
				digits := strconv.FormatUint(count, 10)
				output, diagnostic, err := execute(catalogs, "--output", "json", "i18n", "render", "example.source.items",
					"--locale", locale, "--arg", "label=home", "--count", digits)
				if err != nil || diagnostic != "" {
					t.Fatal(err, diagnostic)
				}
				value := resultData[rendered](t, output, "i18n.render")
				selection, err := catalogs.Messages.Resolve("example.source.items", locale)
				if err != nil {
					t.Fatal(err)
				}
				native, err := selection.Render([]i18n.Argument{{Name: "label", Value: "home"}}, &count)
				if err != nil || value.Text != native.Text || value.Category != native.Category || value.Variant != native.Variant ||
					value.FormFallback != native.FormFallback || !strings.Contains(value.Text, digits) {
					t.Fatal("CLI altered integer plural selection", locale, count, err, value)
				}
			}
		}
	})
	t.Run("form_fallback_distinct_from_message_fallback", func(t *testing.T) {
		output, diagnostic, err := execute(catalogs, "--output", "json", "i18n", "render", "example.source.sparse", "--count", "1", "--locale", "en")
		if err != nil || diagnostic != "" {
			t.Fatal(err)
		}
		value := resultData[rendered](t, output, "i18n.render")
		if value.Text != "1 entries" || value.Category != i18n.One || value.Variant != i18n.Other || !value.FormFallback || value.Fallback != "" {
			t.Fatal("plural-form fallback hidden or confused with message fallback", value)
		}
	})
	t.Run("interface_language_independent_of_resource_language", func(t *testing.T) {
		output, diagnostic, err := execute(catalogs, "--lang", "zh-CN", "i18n", "render", "example.source.greeting", "--arg", "name=Ada", "--locale", "fr")
		if err != nil || diagnostic != "" || !strings.HasPrefix(output, "Bonjour Ada.\n") || !strings.Contains(output, "请求语言: fr") || !strings.Contains(output, "语言: fr") {
			t.Fatal("interface locale altered rendering or was ignored", err, output)
		}
		output, diagnostic, err = execute(catalogs, "--lang", "zh-CN", "--output", "json", "i18n", "render", "example.source.greeting", "--arg", "name=Ada")
		if err != nil || diagnostic != "" {
			t.Fatal(err)
		}
		if resultData[rendered](t, output, "i18n.render").Locale != "zh-CN" {
			t.Fatal("default rendering preference ignored")
		}
	})
	t.Run("exact_string_bound", func(t *testing.T) {
		for _, size := range []int{i18n.MaxStringBytes, i18n.MaxStringBytes + 1} {
			output, diagnostic, err := execute(catalogs, "--output", "json", "i18n", "render", "example.source.greeting",
				"--arg", "name="+strings.Repeat("x", size))
			if size == i18n.MaxStringBytes {
				if err != nil || diagnostic != "" || len(resultData[rendered](t, output, "i18n.render").Text) != size+7 {
					t.Fatal("exact bound refused", err)
				}
			} else if !errors.Is(err, command.ErrLimit) || output != "" {
				t.Fatal("string limit lost", err)
			}
		}
	})
}

func TestRenderRefusals(t *testing.T) {
	catalogs := fixtureCatalogs(t)
	for _, item := range []struct {
		name  string
		id    string
		flags []string
	}{
		{"missing_argument", "example.source.greeting", nil},
		{"missing_equal", "example.source.greeting", []string{"--arg", "private-value-canary"}},
		{"empty_name", "example.source.greeting", []string{"--arg", "=private-value-canary"}},
		{"unknown_argument", "example.source.greeting", []string{"--arg", "other=private-value-canary"}},
		{"duplicate_argument", "example.source.greeting", []string{"--arg", "name=public", "--arg", "name=private-value-canary"}},
		{"missing_count", "example.source.items", []string{"--arg", "label=private-value-canary"}},
		{"extra_count", "example.source.greeting", []string{"--arg", "name=private-value-canary", "--count", "0"}},
		{"reserved_count_argument", "example.source.items", []string{"--arg", "label=public", "--arg", "count=1", "--count", "1"}},
	} {
		t.Run(item.name, func(t *testing.T) {
			args := append([]string{"--output", "json", "i18n", "render", item.id}, item.flags...)
			output, diagnostic, err := execute(catalogs, args...)
			if !errors.Is(err, command.ErrUsage) || command.ExitCode(err) != 2 || output != "" || !json.Valid([]byte(diagnostic)) || strings.Contains(diagnostic, "private-value-canary") {
				t.Fatal("invalid render arguments admitted or disclosed", err, output, diagnostic)
			}
		})
	}
	for _, count := range []string{"", "-1", "+1", "1.0", "1e3", "0x10", "1_000", "18446744073709551616", "private-count-canary"} {
		output, diagnostic, err := execute(catalogs, "--output", "json", "i18n", "render", "example.source.sparse", "--count", count)
		if !errors.Is(err, command.ErrUsage) || output != "" || strings.Contains(diagnostic, "private-count-canary") {
			t.Fatal("invalid count accepted or disclosed", count, err)
		}
	}
	for _, item := range []struct {
		name   string
		values []string
	}{
		{"signed", []string{"", "9223372036854775808", "-9223372036854775809", "0x10", "1.5"}},
		{"unsigned", []string{"", "-1", "18446744073709551616", "0x10", "1e3"}},
		{"enabled", []string{"", "1", "True", "FALSE", "private-bool-canary"}},
	} {
		for _, invalid := range item.values {
			args := []string{"--output", "json", "i18n", "render", "example.source.scalars"}
			for _, parameter := range []struct{ name, value string }{{"label", "public"}, {"signed", "1"}, {"unsigned", "2"}, {"enabled", "false"}} {
				value := parameter.value
				if parameter.name == item.name {
					value = invalid
				}
				args = append(args, "--arg", parameter.name+"="+value)
			}
			output, diagnostic, err := execute(catalogs, args...)
			if !errors.Is(err, command.ErrUsage) || output != "" || strings.Contains(diagnostic, "private-bool-canary") {
				t.Fatal("scalar type/overflow refusal lost", item.name, invalid, err)
			}
		}
	}
	t.Run("argument_count_bound", func(t *testing.T) {
		args := []string{"--output", "json", "i18n", "render", "example.source.greeting"}
		for range i18n.MaxArguments + 1 {
			args = append(args, "--arg", "name=public")
		}
		output, _, err := execute(catalogs, args...)
		if !errors.Is(err, command.ErrLimit) || output != "" {
			t.Fatal("argument count bound lost", err)
		}
	})
	t.Run("unknown_message", func(t *testing.T) {
		output, diagnostic, err := execute(catalogs, "--output", "json", "i18n", "render", "absent.source.private-canary")
		if !errors.Is(err, command.ErrNotFound) || output != "" || strings.Contains(diagnostic, "private-canary") {
			t.Fatal("unknown message rendered or leaked", err)
		}
	})
}

func FuzzRenderArguments(f *testing.F) {
	catalogs := fixtureCatalogs(f)
	for _, seed := range []string{"name=Ada", "name=a,b=c", "name=", "name=first\nname=second", "unknown=private", "name=\xff", "=value"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > command.MaxArgumentBytes+1 {
			return
		}
		args := []string{"--output", "json", "i18n", "render", "example.source.greeting"}
		for _, argument := range strings.Split(input, "\n") {
			args = append(args, "--arg", argument)
		}
		output, diagnostic, err := execute(catalogs, args...)
		if err != nil {
			if output != "" || diagnostic == "" {
				t.Fatal("failed render emitted output or lacked diagnostic")
			}
		} else {
			if diagnostic != "" {
				t.Fatal("successful render emitted diagnostic")
			}
			resultData[rendered](t, output, "i18n.render")
		}
		if len(output) > command.MaxOutputBytes || len(diagnostic) > command.MaxOutputBytes {
			t.Fatal("unbounded output")
		}
	})
}
