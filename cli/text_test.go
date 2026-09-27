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
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/cli/internal/command/project"
	"github.com/frost-leo/fathomry/cli/internal/testdata/commandfamily"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func TestLanguageOrderFallbackAndMachineIdentity(t *testing.T) {
	for _, test := range []struct {
		args   []string
		status int
		phrase string
	}{
		{nil, 0, "Usage"},
		{[]string{"--lang", "ZH-cn", "--help"}, 0, "用法"},
		{[]string{"--lang=zh-CN", "--bad"}, 2, "命令或参数无效"},
		{[]string{"--bad", "--lang=zh-CN"}, 2, "Invalid command"},
		{[]string{"--lang=zh-CN", "--lang=invalid"}, 2, "命令或参数无效"},
		{[]string{"--lang=zh-CN", "--lang=en", "--bad"}, 2, "Invalid command"},
		{[]string{"--lang=invalid", "--lang=zh-CN"}, 2, "Invalid command"},
		{[]string{"--lang"}, 2, "Invalid command"},
		{[]string{"missing", "--lang=zh-CN"}, 2, "命令或参数无效"},
		{[]string{"--", "--lang=zh-CN"}, 2, "Invalid command"},
		{[]string{"help", "sample", "second", "--lang=zh-CN"}, 0, "Second family-local leaf"},
	} {
		status, err, out, diagnostic := capture(context.Background(), test.args, withFamily(&commandfamily.Backend{}))
		if status != test.status || !strings.Contains(out+diagnostic, test.phrase) {
			t.Fatalf("%v: %d %v %q %q", test.args, status, err, out, diagnostic)
		}
	}
	for _, language := range []string{"en", "zh-CN"} {
		backend := &commandfamily.Backend{}
		status, err, out, _ := capture(context.Background(), []string{"sample", "nested", "record", "--value=SECRET", "--lang=" + language}, withFamily(backend))
		if status != 0 || err != nil || out != "{\"accepted\":true}\n" {
			t.Fatalf("machine bytes changed: %d %v %q", status, err, out)
		}
		status, err, out, _ = capture(context.Background(), []string{"sample", "nested", "record", "--value=SECRET", "--lang=" + language, "--help"}, withFamily(backend))
		if status != 0 || err != nil || strings.Contains(out, "SECRET") || strings.Contains(out, "DEFAULT-CANARY") || !strings.Contains(out, "--value") {
			t.Fatalf("help values: %d %v %q", status, err, out)
		}
	}
}

func TestNoAmbientLocaleOrUnsafeParserDiagnostic(t *testing.T) {
	t.Setenv("LANG", "zh_CN.UTF-8")
	t.Setenv("LC_ALL", "zh_CN.UTF-8")
	status, _, out, _ := capture(context.Background(), nil, commands)
	if status != 0 || !strings.Contains(out, "Usage") {
		t.Fatalf("ambient locale: %d %q", status, out)
	}
	status, err, _, diagnostic := capture(context.Background(), []string{"--lang=TOKEN\x1b[31m\r\n"}, commands)
	if status != 2 || !errors.Is(err, errLanguage) || strings.Contains(diagnostic, "TOKEN") || strings.ContainsAny(diagnostic, "\x1b\r") {
		t.Fatalf("%d %v %q", status, err, diagnostic)
	}
	var native *pflag.InvalidValueError
	if !errors.As(err, &native) || !strings.Contains(native.Error(), "TOKEN") {
		t.Fatal("inspectable native cause was erased")
	}
}

func TestResourcesAreCompleteAndLicensed(t *testing.T) {
	words := newText()
	for _, key := range []string{"root", "help", "helpFlag", "language", "usage", "commands", "flags", "invalid", "failed", "canceled"} {
		if words.english(key) == "" {
			t.Errorf("missing English %s", key)
		}
	}
	words.language = "zh-CN"
	english, err := resources.ReadFile("resources/en.json")
	if err != nil {
		t.Fatal(err)
	}
	chinese, err := resources.ReadFile("resources/zh-cn.json")
	if err != nil {
		t.Fatal(err)
	}
	var translation struct {
		Schema   string            `json:"schema"`
		Profile  string            `json:"profile"`
		Owner    string            `json:"owner"`
		Locale   string            `json:"locale"`
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(chinese, &translation); err != nil {
		t.Fatal(err)
	}
	kept := translation.Messages[:0]
	for _, message := range translation.Messages {
		var identity struct{ ID string }
		if err := json.Unmarshal(message, &identity); err != nil {
			t.Fatal(err)
		}
		if identity.ID != "fathomry.cli:failed" {
			kept = append(kept, message)
		}
	}
	translation.Messages = kept
	chinese, err = json.Marshal(translation)
	if err != nil {
		t.Fatal(err)
	}
	words.catalog, err = i18n.Prepare(i18n.Source{Name: "en", Data: english}, i18n.Source{Name: "zh", Data: chinese})
	if err != nil {
		t.Fatal(err)
	}
	if words.get("failed") != words.english("failed") {
		t.Fatal("English fallback failed")
	}
	header, err := os.ReadFile("../.github/LICENSE_HEADER")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"resources/en.json", "resources/zh-cn.json", "internal/command/project/resources/en.json", "internal/command/project/resources/zh-cn.json", "internal/testdata/commandfamily/resources/en.json", "internal/testdata/commandfamily/resources/zh-cn.json"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var resource struct {
			License  []string `json:"license_notice"`
			Messages json.RawMessage
		}
		if err := json.Unmarshal(data, &resource); err != nil {
			t.Fatal(err)
		}
		if strings.Join(resource.License, "\n") != strings.TrimSpace(string(header)) {
			t.Errorf("incomplete license %s", path)
		}
		values := map[string]string{}
		if strings.Contains(path, "testdata") {
			if err := json.Unmarshal(resource.Messages, &values); err != nil {
				t.Fatal(err)
			}
		} else {
			var messages []struct {
				ID    string
				Forms map[string]string
			}
			if err := json.Unmarshal(resource.Messages, &messages); err != nil {
				t.Fatal(err)
			}
			for _, message := range messages {
				values[message.ID] = message.Forms["other"]
			}
		}
		for key, value := range values {
			if value == "" || strings.ContainsAny(value, "\x1b\r\n") {
				t.Errorf("invalid text %s %s", path, key)
			}
		}
	}
}

func TestNativeGoTestShorthandCompatibility(t *testing.T) {
	// pflag deliberately skips Go-test shorthand prefixes, even outside go test.
	for _, args := range [][]string{{"-test.unknown=x"}, {"-htest.unknown=x"}} {
		status, err, out, _ := capture(context.Background(), args, commands)
		if status != 0 || err != nil || !strings.Contains(out, "Usage") {
			t.Fatalf("native grammar changed: %v %d %v", args, status, err)
		}
	}
	status, _, _, _ := capture(context.Background(), []string{"--test.unknown=x"}, commands)
	if status != 2 {
		t.Fatalf("long-flag negative control: %d", status)
	}
}

func TestNativeMetadataUsesEnglishCatalog(t *testing.T) {
	words := newText()
	words.language = "zh-CN"
	root := commands(words)
	if err := prepare(root, words); err != nil {
		t.Fatal(err)
	}
	if root.Short != words.english("root") || root.PersistentFlags().Lookup("lang").Usage != words.english("language") ||
		root.PersistentFlags().Lookup("help").Usage != words.english("helpFlag") {
		t.Fatal("native English metadata was missing or selected by display language")
	}
	ids := root.PersistentFlags().Lookup("help").Annotations["fathomry.usage.id"]
	if len(ids) != 1 || ids[0] != "fathomry.cli:helpFlag" {
		t.Fatal("missing help resource binding")
	}
}

func TestEmptyBindingsDoNotSilentlyBecomeLegacyMetadata(t *testing.T) {
	for _, mode := range []string{"command", "flag-empty", "flag-many"} {
		words := newText()
		root := commands(words)
		if err := prepare(root, words); err != nil {
			t.Fatal(err)
		}
		switch mode {
		case "command":
			root.Annotations["fathomry.short.id"] = ""
		case "flag-empty":
			root.PersistentFlags().Lookup("help").Annotations["fathomry.usage.id"] = nil
		case "flag-many":
			root.PersistentFlags().Lookup("help").Annotations["fathomry.usage.id"] = []string{"fathomry.cli:helpFlag", "extra"}
		}
		if err := validateBindings(root, words); err == nil {
			t.Fatal("invalid preparation binding", mode)
		}
		// A fresh invocation verifies the rendering gate without a prior retained failure.
		fresh := newText()
		if err := renderHelp(&bytes.Buffer{}, root, fresh); err == nil || fresh.failure() == nil {
			t.Fatal("unretained hard binding failure", mode)
		}
	}
}

func TestExplicitCatalogEnglishFallbackIsSuccessful(t *testing.T) {
	sources, err := project.Sources()
	if err != nil {
		t.Fatal(err)
	}
	english, err := resources.ReadFile("resources/en.json")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := i18n.Prepare(sources[0], i18n.Source{Name: "root", Data: english})
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"--lang=zh-CN", "--help"}, {"new", "--lang=zh-CN", "--help"}} {
		status, err, out, diagnostic := capture(context.Background(), args, func(words *text) *cobra.Command {
			words.catalog = catalog
			return commands(words)
		})
		if status != 0 || err != nil || diagnostic != "" || !strings.Contains(out, "Create a Go project") || !strings.Contains(out, "Usage") {
			t.Fatal(status, err, out, diagnostic)
		}
	}
}
