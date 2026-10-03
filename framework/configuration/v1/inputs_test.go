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

package configuration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

func TestInputs(t *testing.T) {
	empty := func(string) (string, bool) { return "", false }
	binding := InputBinding{Name: "choice", Flag: "choice", Environment: "CHOICE", Default: "default", HasDefault: true, Dotenv: true}
	path := filepath.Join(t.TempDir(), "selected.env")
	write(t, path, "CHOICE=file\n")
	base := InputOptions{Lookup: empty, DotenvFlag: "dotenv", DotenvEnvironment: "SELECTED_DOTENV", Bindings: []InputBinding{binding}}
	t.Run("presence_precedence_and_detached_records", func(t *testing.T) {
		for _, test := range []struct {
			args        []string
			environment map[string]string
			want        string
			records     int
		}{
			{nil, nil, "default", 0},
			{[]string{"--dotenv", path}, nil, "file", 1},
			{[]string{"--dotenv", path}, map[string]string{"CHOICE": "environment"}, "environment", 1},
			{[]string{"--dotenv", path, "--choice=argument"}, map[string]string{"CHOICE": "environment"}, "argument", 1},
			{[]string{"--dotenv="}, map[string]string{"SELECTED_DOTENV": path}, "default", 0},
			{[]string{"--choice="}, map[string]string{"CHOICE": "environment"}, "", 0},
		} {
			options := base
			options.Arguments = test.args
			calls := map[string]int{}
			options.Lookup = func(name string) (string, bool) {
				calls[name]++
				value, present := test.environment[name]
				return value, present
			}
			values, err := ReadInputs(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			value, present := values.Lookup("choice")
			if !present || value != test.want || len(values.Records()) != test.records {
				t.Fatal("selection/presence changed")
			}
			for _, count := range calls {
				if count != 1 {
					t.Fatal("input looked up repeatedly")
				}
			}
			records := values.Records()
			if len(records) > 0 {
				if !records[0].Info().Released {
					t.Fatal("file owner escaped")
				}
				records[0] = Record{}
				if !values.Records()[0].Info().Released {
					t.Fatal("records aliased")
				}
			}
		}
		options := base
		options.Bindings = []InputBinding{{Name: "absent", Environment: "ABSENT"}, {Name: "empty", HasDefault: true}}
		values, err := ReadInputs(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		if _, present := values.Lookup("absent"); present {
			t.Fatal("absence lost")
		}
		if value, present := values.Lookup("empty"); !present || value != "" {
			t.Fatal("empty default lost")
		}
	})
	t.Run("strict_names_arguments_and_bounds", func(t *testing.T) {
		for _, args := range [][]string{{"--unknown=x"}, {"positional"}, {"--choice=x", "--choice=y"}, {"--dotenv=" + path, "--dotenv=" + path}, {strings.Repeat("x", 65537)}} {
			options := base
			options.Arguments = args
			if _, err := ReadInputs(context.Background(), options); err == nil {
				t.Fatal("invalid arguments admitted")
			}
		}
		for _, bindings := range [][]InputBinding{
			{binding, binding}, {{Name: "a", Flag: "dotenv"}}, {{Name: "a", Environment: "SELECTED_DOTENV"}},
			{{Name: "a", Dotenv: true}}, {{Name: "bad name"}}, {{Name: "a", Environment: "BAD-NAME"}},
			{{Name: "a", Flag: "a"}, {Name: "b", Flag: "a"}}, {{Name: "a", Environment: "A"}, {Name: "b", Environment: "A"}},
		} {
			options := base
			options.Bindings = bindings
			called := false
			options.Lookup = func(string) (string, bool) { called = true; return "", false }
			if _, err := ReadInputs(context.Background(), options); err == nil || called {
				t.Fatal("invalid declaration reached acquisition")
			}
		}
		if _, err := ReadInputs(nil, base); err == nil {
			t.Fatal("nil context")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := ReadInputs(ctx, base); !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation lost", err)
		}
	})
	t.Run("value_limit_independent_of_flag_syntax", func(t *testing.T) {
		for _, name := range []string{"value", strings.Repeat("v", 64)} {
			for _, size := range []int{(64 << 10) - 1, 64 << 10, (64 << 10) + 1} {
				value := strings.Repeat("x", size)
				for _, args := range [][]string{{"--" + name, value}, {"--" + name + "=" + value}, {"-" + name + "=" + value}} {
					inputs, err := ReadInputs(context.Background(), InputOptions{Arguments: args, Lookup: empty, Bindings: []InputBinding{{Name: "value", Flag: name}}})
					selected, present := inputs.Lookup("value")
					if size > 64<<10 {
						if !errors.Is(err, ErrLimit) || present {
							t.Fatal("oversized selected value was not refused as a limit")
						}
					} else if err != nil || !present || selected != value {
						t.Fatalf("valid flag value refused: value=%d flag=%d tokens=%d: %v", size, len(name), len(args), err)
					}
				}
			}
		}
	})
	t.Run("independent_aggregate_budgets", func(t *testing.T) {
		bindings := make([]InputBinding, 5)
		for index := range bindings {
			bindings[index] = InputBinding{Name: fmt.Sprintf("value%d", index), Flag: fmt.Sprintf("value%d", index), Environment: fmt.Sprintf("VALUE%d", index)}
		}
		value := strings.Repeat("x", 64<<10)
		lookup := func(string) (string, bool) { return value, true }
		if _, err := ReadInputs(context.Background(), InputOptions{Lookup: lookup, Bindings: bindings[:4]}); err != nil {
			t.Fatal("exact selected-text aggregate refused", err)
		}
		if _, err := ReadInputs(context.Background(), InputOptions{Lookup: lookup, Bindings: bindings}); !errors.Is(err, ErrLimit) {
			t.Fatal("selected-text aggregate exceeded")
		}
		var args []string
		for _, binding := range bindings[:4] {
			args = append(args, "--"+binding.Flag+"="+value)
		}
		called := false
		options := InputOptions{Arguments: args, Lookup: func(string) (string, bool) { called = true; return "", false }, Bindings: bindings}
		if _, err := ReadInputs(context.Background(), options); !errors.Is(err, ErrLimit) || called {
			t.Fatal("raw argv aggregate exceeded before lookup")
		}
		for _, argument := range []string{"--value0=\x00", "--value0=\xff", strings.Repeat("x", (256<<10)+1)} {
			options.Arguments = []string{argument}
			if _, err := ReadInputs(context.Background(), options); !errors.Is(err, ErrLimit) || called {
				t.Fatal("invalid raw argv reached lookup")
			}
		}
	})
	t.Run("maximum_dotenv_without_newline", func(t *testing.T) {
		value := strings.Repeat("x", configsource.MaxDotenvBytes-len("CHOICE="))
		write(t, path, "CHOICE="+value)
		options := base
		options.Arguments = []string{"--dotenv", path}
		inputs, err := ReadInputs(context.Background(), options)
		selected, present := inputs.Lookup("choice")
		if err != nil || !present || selected != value || len(inputs.Records()) != 1 || !inputs.Records()[0].Info().Released {
			t.Fatal("maximum literal input lost data or released evidence", err)
		}
	})
	t.Run("literal_files_and_process_independence", func(t *testing.T) {
		t.Setenv("CHOICE", "ambient")
		values, err := ReadInputs(context.Background(), base)
		if err != nil {
			t.Fatal(err)
		}
		if value, _ := values.Lookup("choice"); value != "default" {
			t.Fatal("ambient process consulted")
		}
		for _, raw := range []string{"CHOICE=one\nCHOICE=two\n", "UNDECLARED=secret-canary\n", "CHOICE=\"bad", "CHOICE=" + strings.Repeat("x", configsource.MaxDotenvBytes)} {
			write(t, path, raw)
			options := base
			options.Arguments = []string{"--dotenv", path, "--choice=shadow"}
			result, err := ReadInputs(context.Background(), options)
			if err == nil || len(result.Records()) != 1 {
				t.Fatal("invalid selected file accepted or lost evidence")
			}
			if strings.Contains(err.Error(), "secret-canary") {
				t.Fatal("diagnostics exposed values")
			}
		}
		if os.Getenv("CHOICE") != "ambient" {
			t.Fatal("process environment mutated")
		}
		options := base
		options.Arguments = []string{"--dotenv", path + ".missing"}
		if _, err := ReadInputs(context.Background(), options); !errors.Is(err, ErrMissing) {
			t.Fatal("missing dotenv not distinct", err)
		}
	})
}
func TestProviderDocumentBindings(t *testing.T) {
	directory := t.TempDir()
	base, environment := filepath.Join(directory, "base.json"), filepath.Join(directory, "environment.json")
	write(t, base, `{"value":"base"}`)
	write(t, environment, `{"value":"selected"}`)
	type model struct {
		Value string `json:"value"`
	}
	documents := []File{{Path: environment, Kind: Environment, Encoding: JSON}, {Path: base, Kind: Base, Encoding: JSON}}
	provider, err := Viper(ViperOptions{Documents: documents})
	if err != nil {
		t.Fatal(err)
	}
	documents[0].Path = base
	documents[0].Kind = Base
	result, err := Load(context.Background(), Declaration[model]{Schema: Schema[model]{Version: 1}}, Dependencies{Provider: provider})
	if err != nil {
		t.Fatal(err)
	}
	accepted, _ := result.State.Capture()
	value, _ := accepted.ValueCopy()
	if value.Value != "selected" {
		t.Fatal("document policy detached from its location or aliased")
	}
	t.Run("same_declaration_can_be_watched", func(t *testing.T) {
		watch, err := Watch(context.Background(), Declaration[model]{Schema: Schema[model]{Version: 1}}, Dependencies{Provider: provider}, WatchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer watch.Close(context.Background())
		nextDecision(t, watch, true)
		current, err := watch.Capture()
		if err != nil {
			t.Fatal(err)
		}
		observed, _ := current.ValueCopy()
		if observed != value {
			t.Fatal("Load and Watch disagreed on an admitted declaration")
		}
	})
	t.Run("literal_paths_are_not_canonicalized", func(t *testing.T) {
		alias := directory + string(filepath.Separator) + "." + string(filepath.Separator) + "base.json"
		if _, err := Viper(ViperOptions{Documents: []File{{Path: base, Kind: Base, Encoding: JSON}, {Path: alias, Kind: Environment, Encoding: JSON}}}); err != nil {
			t.Fatal("distinct native-supported literal paths were canonicalized", err)
		}
	})
	for _, documents := range [][]File{
		nil, {{Path: base, Kind: Base, Encoding: "unknown"}}, {{Path: base, Kind: Variables, Encoding: JSON}},
		{{Path: base, Kind: Base, Encoding: JSON}, {Path: environment, Kind: Base, Encoding: JSON}},
		{{Path: base, Kind: Base, Encoding: JSON}, {Path: base, Kind: Environment, Encoding: JSON}},
		{{Path: base, Kind: Base, Encoding: JSON}, {Path: base, Kind: Override, Encoding: JSON, Optional: true}},
		{{Path: "relative.json", Kind: Base, Encoding: JSON}},
	} {
		if provider, err := Viper(ViperOptions{Documents: documents}); !errors.Is(err, ErrDeclaration) || provider.state != nil {
			t.Fatal("invalid source declaration")
		}
	}
	snapshot, _ := accepted.ValueCopy()
	if !reflect.DeepEqual(value, snapshot) {
		t.Fatal("detached snapshot changed")
	}
}

func TestInputDiagnostics(t *testing.T) {
	for _, value := range []any{
		NacosConnection{Password: "private-credential-canary"},
		NacosOptions{Connection: NacosConnection{Password: "private-credential-canary"}},
		InputOptions{Arguments: []string{"private-credential-canary"}},
		InputBinding{Name: "password", Default: "private-credential-canary"},
		Variable{Value: "private-credential-canary"},
	} {
		if strings.Contains(fmt.Sprintf("%+v", value), "canary") {
			t.Fatal("default formatting leaked input")
		}
	}
}

func FuzzInputs(f *testing.F) {
	for _, argument := range []string{"--value=selected", "--value=", "--unknown=secret", "--value"} {
		f.Add(argument, "environment", true)
	}
	f.Fuzz(func(t *testing.T, argument, environment string, present bool) {
		if len(argument) > 8192 || len(environment) > 8192 {
			return
		}
		calls := 0
		inputs, err := ReadInputs(context.Background(), InputOptions{
			Arguments: []string{argument},
			Lookup: func(name string) (string, bool) {
				calls++
				if name != "VALUE" {
					t.Fatal("undeclared lookup")
				}
				return environment, present
			},
			Bindings: []InputBinding{{Name: "value", Flag: "value", Environment: "VALUE"}},
		})
		if calls > 1 {
			t.Fatal("repeated lookup")
		}
		if err != nil {
			return
		}
		value, _ := inputs.Lookup("value")
		if !utf8.ValidString(value) || strings.ContainsRune(value, 0) || len(inputs.Records()) != 0 {
			t.Fatal("invalid input publication")
		}
	})
}
