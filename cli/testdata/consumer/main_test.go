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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/cli"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/pflag"
)

func checkOccurrence(t *testing.T, err error, condition failure.Condition) *failure.Error {

	t.Helper()
	current, ok := failure.Inspect(err)
	if !ok || current.Diagnostic().Condition != condition || !errors.Is(err, condition) {
		t.Fatalf("missing public condition %s: %T", condition, err)
	}
	return current
}

func TestPublicCLIConditionConsumer(t *testing.T) {
	TestPublicDefinitionQueriesBeforeRun(t)
	for _, locale := range []string{"en", "zh-CN"} {
		streams := cli.Streams{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard}
		status, err := cli.Run(context.Background(), []string{"--lang=" + locale, "--help"}, streams)
		if status != 0 || err != nil {
			t.Fatal("success changed", status, err)
		}
		status, err = cli.Run(context.Background(), []string{"--lang=" + locale, "--lang=PRIVATE-CANARY"}, streams)
		current := checkOccurrence(t, err, cli.ErrLanguage)
		var parser *pflag.InvalidValueError
		if status != 2 || !errors.As(err, &parser) || !strings.Contains(parser.Error(), "PRIVATE-CANARY") {
			t.Fatal("native parser evidence lost", status)
		}
		if current.Error() != string(cli.ErrLanguage) || strings.Contains(fmt.Sprintf("%#v", err), "PRIVATE-CANARY") {
			t.Fatal("unsafe owned projection")
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Error("failure", "error", err)
		if strings.Contains(output.String(), "PRIVATE-CANARY") || !strings.Contains(output.String(), string(cli.ErrLanguage)) {
			t.Fatal("unsafe log")
		}
		if _, encodeErr := json.Marshal(err); !errors.Is(encodeErr, failure.ErrSerialization) {
			t.Fatal("runtime serialized")
		}
		_, second := cli.Run(context.Background(), []string{"missing"}, streams)
		if checkOccurrence(t, second, cli.ErrUsage) == current {
			t.Fatal("reused occurrence")
		}

		// A small compatible local source fixture keeps these semantic refusal checks
		// independent of native SDK types and of the framework checkout location.
		source := t.TempDir()
		if err := os.WriteFile(filepath.Join(source, "go.mod"), []byte("module github.com/frost-leo/fathomry\ngo 1.27.0\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(source, "cli"), 0700); err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"source", "exists", "destination", "overlap", "identity"} {
			selectedSource := source
			target := filepath.Join(t.TempDir(), "app")
			module := "example.org/app"
			condition := cli.ErrProjectSource
			switch kind {
			case "source":
				selectedSource = filepath.Join(t.TempDir(), "missing")
			case "exists":
				condition = cli.ErrProjectExists
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
			case "destination":
				condition = cli.ErrProjectDestination
				target = filepath.Join(t.TempDir(), "missing", "app")
			case "overlap":
				condition = cli.ErrProjectOverlap
				target = filepath.Join(source, "nested")
			case "identity":
				condition = cli.ErrProjectIdentity
				module = "github.com/frost-leo/fathomry"
			}
			status, err = cli.Run(context.Background(), []string{"new", target, "--module=" + module, "--fathomry-source=" + selectedSource, "--lang=" + locale}, streams)
			checkOccurrence(t, err, condition)
			if kind == "source" || kind == "destination" || kind == "exists" {
				want := os.ErrNotExist
				if kind == "exists" {
					want = os.ErrExist
				}
				var pathError *os.PathError
				if !errors.Is(err, want) || !errors.As(err, &pathError) {
					t.Fatal("native filesystem evidence lost", kind)
				}
			}
			expected := 1
			if kind == "identity" {
				expected = 2
			}
			if status != expected {
				t.Fatal(kind, status)
			}
			if kind != "exists" {
				if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("refusal mutated destination", kind)
				}
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, err := cli.Run(ctx, nil, cli.Streams{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard})
	if status != 130 || !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation reclassified", status)
	}
	status, err = cli.Run(nil, nil, cli.Streams{})
	checkOccurrence(t, err, cli.ErrInputs)
	if status != 1 {
		t.Fatal("invalid invocation status")
	}
}

func TestPublicDefinitionQueriesBeforeRun(t *testing.T) {
	catalogs, err := cli.Catalogs()
	if err != nil {
		t.Fatal(err)
	}
	modules, err := catalogs.Errors.Modules()
	if err != nil || len(modules) != 3 {
		t.Fatal(modules, err)
	}
	root, exists, err := catalogs.Errors.Module("fathomry.cli")
	if err != nil || !exists || root.Parent != "" || len(root.Children) != 1 || root.Children[0] != "fathomry.cli.project" {
		t.Fatal(root, err)
	}
	direct, _, _ := catalogs.Errors.Definitions("fathomry.cli", false)
	subtree, _, _ := catalogs.Errors.Definitions("fathomry.cli", true)
	if len(direct) != 5 || len(subtree) != 14 {
		t.Fatal("incomplete CLI inventory", direct, subtree)
	}
	for _, condition := range []failure.Condition{cli.ErrInputs, cli.ErrDefinition, cli.ErrUsage, cli.ErrLanguage, cli.ErrOutput, cli.ErrProjectArguments, cli.ErrProjectIdentity, cli.ErrProjectSource, cli.ErrProjectDestination, cli.ErrProjectExists, cli.ErrProjectOverlap, cli.ErrProjectPreparation, cli.ErrProjectCreation, cli.ErrProjectPresentation} {
		definition, exists, err := catalogs.Errors.Lookup(condition)
		if err != nil || !exists || definition.Contract != "v1" {
			t.Fatal("missing condition", condition)
		}
	}
	bindings, err := catalogs.Bindings.Inspect()
	if err != nil || len(bindings) != 33 {
		t.Fatal("incomplete bindings", len(bindings), err)
	}
	resources, err := catalogs.Messages.Inspect()
	if err != nil || len(resources) != 38 {
		t.Fatal("incomplete resources", len(resources), err)
	}
	expected, err := json.Marshal(resources)
	if err != nil {
		t.Fatal(err)
	}
	for _, binding := range bindings {
		message, err := catalogs.Messages.Lookup(binding.Message, "en")
		if err != nil || !message.TranslationExists || message.Definition.Contract != binding.MessageContract {
			t.Fatal("dangling binding", binding.ID)
		}
	}
	contract, exists, _ := catalogs.Errors.Contract("fathomry.cli.project:creation")
	if !exists || contract.Use != failure.PresentationInput || contract.Access != failure.OwnerFacts {
		t.Fatal("invented public error facts", contract)
	}
	root.Children[0] = "changed"
	contract.Fields[0].Values[0] = "changed"
	resources[0].Forms[0].Pattern = "changed"
	current, _ := catalogs.Messages.Inspect()
	actual, _ := json.Marshal(current)
	if !bytes.Equal(actual, expected) {
		t.Fatal("mutable query")
	}
	_, err = cli.Run(context.Background(), []string{"--lang=zh-CN", "--help"}, cli.Streams{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	again, _ := cli.Catalogs()
	current, _ = again.Messages.Inspect()
	actual, _ = json.Marshal(current)
	if !bytes.Equal(actual, expected) {
		t.Fatal("invocation changed atlas")
	}
	*catalogs.Errors = failure.DefinitionCatalog{}
	*catalogs.Messages = i18n.Catalog{}
	*catalogs.Bindings = i18n.Bindings{}
	untouched, err := cli.Catalogs()
	if err != nil {
		t.Fatal(err)
	}
	if found, _, err := untouched.Errors.Lookup(cli.ErrOutput); err != nil || found.Condition != cli.ErrOutput {
		t.Fatal("caller handle changed CLI defaults")
	}
}
