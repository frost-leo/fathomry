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

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func execute(args []string, language string) (string, string, error) {
	var output, diagnostic bytes.Buffer
	err := Run(context.Background(), args, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic, Language: language})
	return output.String(), diagnostic.String(), err
}

func TestOfficialCommands(t *testing.T) {
	t.Run("offline_help_and_scope", func(t *testing.T) {
		for _, args := range [][]string{nil, {"--help"}, {"error"}, {"error", "--help"}, {"help", "error", "explain"}, {"i18n", "--help"}, {"help", "help"}} {
			output, diagnostic, err := execute(args, "en")
			if err != nil || diagnostic != "" || !strings.Contains(output, "Usage:") {
				t.Fatalf("help %q: %v / %s", args, err, diagnostic)
			}
			if strings.Contains(output, "version") || strings.Contains(output, "completion") || strings.Contains(output, "new ") {
				t.Fatal("deferred command advertised")
			}
		}
		if _, err := settings.Default(); !errors.Is(err, settings.ErrUnconfigured) {
			t.Fatal("offline CLI configured application settings")
		}
	})
	t.Run("numeric_symbolic_and_localized_explanation", func(t *testing.T) {
		definition := failure.Definitions()[0]
		for _, identity := range []string{definition.Code.String(), strconv.FormatUint(uint64(definition.Code), 10), string(definition.Identifier)} {
			for _, locale := range []string{"en", "zh-CN", "fr"} {
				output, diagnostic, err := execute([]string{"error", "explain", identity, "--output", "json", "--lang", locale}, "")
				if err != nil || diagnostic != "" {
					t.Fatal("explanation failed", err)
				}
				var value struct {
					Schema  string `json:"schema"`
					Command string `json:"command"`
					Data    struct {
						Definition failure.Definition `json:"definition"`
						Message    string             `json:"message"`
						Locale     string             `json:"locale"`
						Fallback   string             `json:"fallback"`
					} `json:"data"`
				}
				if json.Unmarshal([]byte(output), &value) != nil || value.Schema != command.Schema || value.Command != "error.explain" || value.Data.Definition != definition {
					t.Fatal("unstable machine identity")
				}
				if locale == "zh-CN" {
					if value.Data.Locale != "zh-CN" || value.Data.Message == definition.Message {
						t.Fatal("Chinese explanation absent")
					}
				} else if value.Data.Locale != "en" || value.Data.Message != definition.Message {
					t.Fatal("baseline/fallback differs")
				}
				if locale == "fr" && value.Data.Fallback != "unsupported-locale" {
					t.Fatal("fallback hidden")
				}
			}
		}
	})
	t.Run("all_current_public_owners", func(t *testing.T) {
		output, diagnostic, err := execute([]string{"error", "components", "--output", "json"}, "")
		if err != nil || diagnostic != "" {
			t.Fatal(err)
		}
		var value struct {
			Data []failure.Component `json:"data"`
		}
		if json.Unmarshal([]byte(output), &value) != nil || len(value.Data) != len(failure.Allocations()) {
			t.Fatal("public component missing from atlas")
		}
		seen := map[failure.Facility]bool{}
		for _, item := range value.Data {
			seen[item.Facility] = true
		}
		for _, allocation := range failure.Allocations() {
			if !seen[allocation.Facility] {
				t.Fatal("allocated owner missing")
			}
		}
		output, diagnostic, err = execute([]string{"error", "list", "--module", "fathomry", "--component", "failure", "--output", "json"}, "")
		var list struct {
			Data []failure.Definition `json:"data"`
		}
		if err != nil || diagnostic != "" || json.Unmarshal([]byte(output), &list) != nil || len(list.Data) != len(failure.Definitions()) {
			t.Fatal("owner filter differs")
		}
		output, _, err = execute([]string{"error", "explain", "0xA0450001", "--lang", "zh-CN"}, "")
		if err != nil || !strings.Contains(output, "配置声明") {
			t.Fatal("Framework metadata was not composed offline")
		}
	})
	t.Run("exact_translation_and_coverage", func(t *testing.T) {
		for _, locale := range []string{"en", "zh-CN", "fr"} {
			output, diagnostic, err := execute([]string{"i18n", "show", "fathomry.failure.invalid_code", "--locale", locale, "--output", "json"}, "")
			if err != nil || diagnostic != "" {
				t.Fatal(err)
			}
			var value struct {
				Data struct {
					Message     bool             `json:"message_exists"`
					Translation bool             `json:"translation_exists"`
					Definition  *json.RawMessage `json:"definition"`
				} `json:"data"`
			}
			if json.Unmarshal([]byte(output), &value) != nil || !value.Data.Message || value.Data.Translation != (locale != "fr") || ((value.Data.Definition != nil) != (locale != "fr")) {
				t.Fatal("exact lookup silently used fallback")
			}
		}
		for _, locale := range []string{"zh-CN", "fr"} {
			output, diagnostic, err := execute([]string{"i18n", "coverage", locale, "--output", "json"}, "")
			if err != nil || diagnostic != "" {
				t.Fatal(err)
			}
			var value struct {
				Data []struct {
					Missing []string `json:"missing"`
				} `json:"data"`
			}
			if json.Unmarshal([]byte(output), &value) != nil || len(value.Data) != len(failure.Allocations()) {
				t.Fatal("coverage omitted owners")
			}
			for _, item := range value.Data {
				if (len(item.Missing) == 0) != (locale == "zh-CN") {
					t.Fatal("coverage counted fallback as a translation")
				}
			}
		}
		output, _, err := execute([]string{"i18n", "list", "--output", "json"}, "")
		if err != nil || !json.Valid([]byte(output)) || !strings.Contains(output, "\"source_digest\"") || strings.Contains(output, "\"SourceDigest\"") {
			t.Fatal("resource DTO leaked Go field spelling")
		}
	})
	t.Run("language_selection_and_bad_environment_override", func(t *testing.T) {
		output, diagnostic, err := execute([]string{"--help"}, "zh-CN")
		if err != nil || diagnostic != "" || !strings.Contains(output, "用法") {
			t.Fatal("environment preference ignored")
		}
		output, diagnostic, err = execute([]string{"--help", "--lang", "en", "--output", "json"}, "private-environment-canary")
		if err != nil || diagnostic != "" || !json.Valid([]byte(output)) || strings.Contains(output, "private-environment-canary") {
			t.Fatal("explicit locale did not safely override invalid environment")
		}
		_, diagnostic, err = execute([]string{"--help"}, "private-environment-canary")
		if !errors.Is(err, command.ErrUsage) || strings.Contains(diagnostic, "private-environment-canary") {
			t.Fatal("invalid environment leaked or was silently accepted")
		}
	})
}

func TestInvalidCommands(t *testing.T) {
	tests := []struct {
		args []string
		code int
	}{
		{[]string{"unknown-private-canary"}, 2},
		{[]string{"error", "list", "--unknown-private-canary"}, 2},
		{[]string{"error", "list", "extra-private-canary"}, 2},
		{[]string{"error", "explain"}, 2},
		{[]string{"error", "explain", "private-value-canary"}, 2},
		{[]string{"error", "explain", "0xA7FFFFFF"}, 1},
		{[]string{"error", "explain", "example.source.unknown"}, 1},
		{[]string{"error", "list", "--module", "fathomry"}, 2},
		{[]string{"error", "list", "--module", "fathomry", "--component", "missing"}, 1},
		{[]string{"error", "list", "--module", "private/value-canary", "--component", "missing"}, 2},
		{[]string{"i18n", "show", "unknown-private-canary"}, 1},
		{[]string{"i18n", "coverage", "en_US"}, 2},
		{[]string{"--lang", "en_US", "--help"}, 2},
		{[]string{"--lang=", "--help"}, 2},
		{[]string{"--output", "private-output-canary", "error", "list"}, 2},
		{[]string{"help", "absent-private-canary"}, 2},
		{[]string{"version"}, 2},
		{[]string{"--version"}, 2},
		{[]string{"new", "unwanted"}, 2},
		{[]string{"completion"}, 2},
		{[]string{"__complete", "error"}, 2},
		{[]string{"__completeNoDesc", "error"}, 2},
	}
	for index, item := range tests {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			args := append([]string{"--output", "json"}, item.args...)
			output, diagnostic, err := execute(args, "en")
			if command.ExitCode(err) != item.code || output != "" || diagnostic == "" || strings.Contains(diagnostic, "private-") {
				t.Fatalf("unsafe/incorrect refusal: %q: %v / %s", item.args, err, diagnostic)
			}
			// The hidden completion RPC disables native flag parsing entirely;
			// invalid output selections also use the safe default text envelope.
			if !strings.HasPrefix(item.args[0], "__complete") && item.args[0] != "--output" && !json.Valid([]byte(diagnostic)) {
				t.Fatal("structured diagnostic malformed")
			}
		})
	}
}

type refusedWriter struct{ calls int }

func (writer *refusedWriter) Write([]byte) (int, error) { writer.calls++; return 0, io.ErrClosedPipe }

func TestHelpOutputFailure(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"help", "error"}} {
		writer := &refusedWriter{}
		var diagnostic bytes.Buffer
		err := Run(context.Background(), args, command.Options{Input: strings.NewReader(""), Output: writer, ErrorOutput: &diagnostic})
		if !errors.Is(err, command.ErrOutput) || !errors.Is(err, io.ErrClosedPipe) || writer.calls != 1 || command.ExitCode(err) != 1 {
			t.Fatal("Cobra help counterexample was not fixed", err)
		}
	}
}

func TestConcurrentInvocations(t *testing.T) {
	var group sync.WaitGroup
	for index := range 12 {
		group.Go(func() {
			locale := "en"
			if index%2 == 1 {
				locale = "zh-CN"
			}
			for range 4 {
				output, diagnostic, err := execute([]string{"error", "explain", "0xA0010001", "--lang", locale, "--output", "json"}, "")
				if err != nil || diagnostic != "" || !strings.Contains(output, "\"locale\":\""+locale+"\"") {
					t.Error("concurrent invocation state leaked")
				}
			}
		})
	}
	group.Wait()
}

func FuzzArguments(f *testing.F) {
	for _, seed := range []string{"", "--help", "error\nlist", "--lang\nzh-CN\ni18n\ncoverage\nfr", "--output\njson\nerror\nexplain\n0xA0010001", "__complete\n--help", "\xff", "--\n-private"} {
		f.Add(seed)
	}
	catalogs, err := catalogs()
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > command.MaxArgumentBytes+1 {
			return
		}
		args := strings.Split(input, "\n")
		// Real command assembly exercises Cobra's grammar without ambient process state.
		var output, diagnostic bytes.Buffer
		err := command.Run(context.Background(), args, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic, Language: "en"}, catalogs, commands)
		if command.ExitCode(err) == 0 && diagnostic.Len() != 0 {
			t.Fatal("success emitted an error")
		}
		if command.ExitCode(err) != 0 && output.Len() != 0 {
			t.Fatal("failed finite command emitted a success response")
		}
		if output.Len() > command.MaxOutputBytes || diagnostic.Len() > command.MaxOutputBytes {
			t.Fatal("output bounds lost")
		}
	})
}
