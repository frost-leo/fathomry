/*
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

package root

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	projectcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project"
	"github.com/frost-leo/fathomry/i18n"
)

func TestProjectCommandHelpAndRefusals(t *testing.T) {
	parent := t.TempDir()
	for _, args := range [][]string{
		{"help", "new"}, {"new", "--help"}, {"new", "bad-name", "--sdk-bundle", "/private-canary/missing", "--help"},
	} {
		status, out, diagnostic := execute(args...)
		if status != 0 || !strings.Contains(out, "fathomry new") || diagnostic != "" || strings.Contains(out, "private-canary") {
			t.Fatalf("new help: %d %q %q", status, out, diagnostic)
		}
	}
	for _, args := range [][]string{
		{"new"}, {"new", "sample"}, {"new", "sample", "--module", "example.org/business"},
		{"new", "sample", "--module", "example.org/business", "--framework-version", "latest", "--directory", parent},
		{"new", "../escape", "--module", "example.org/business", "--framework-version", "v0.0.0-gh83", "--directory", parent},
		{"new", "sample", "--module", "example.org/business", "--framework-version", "v0.0.0-gh83", "--configuration", "private-canary", "--directory", parent},
		{"new", "sample", "--version"}, {"--version", "new", "sample"}, {"new", "sample", "--output=json"},
	} {
		status, out, diagnostic := execute(args...)
		if status != 2 || out != "" || diagnostic == "" || strings.Contains(diagnostic, "private-canary") {
			t.Fatalf("new refusal: %v => %d %q %q", args, status, out, diagnostic)
		}
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatal("refused command wrote files")
	}
	catalog, err := i18n.Prepare(projectcmd.Resources())
	if err != nil || len(catalog.Snapshot().Stale) != 0 {
		t.Fatal("stale project translations", err)
	}
	status, out, diagnostic := execute("new", "--help", "--lang", "zh-CN")
	if status != 0 || !strings.Contains(out, "创建") || diagnostic != "" {
		t.Fatal("localized help", status, out, diagnostic)
	}
}

func TestProjectCommandCreatesAndPreservesEffects(t *testing.T) {
	parent := t.TempDir()
	args := []string{"new", "sample", "--directory", parent, "--module", "example.org/business", "--framework-version", "v0.0.0-gh83", "--lang", "zh-CN"}
	status, out, diagnostic := execute(args...)
	if status != 0 || !strings.Contains(out, "已创建") || diagnostic != "" {
		t.Fatal("creation command", status, out, diagnostic)
	}
	if _, err := os.Stat(filepath.Join(parent, "sample", "internal/configuration/configuration.go")); err != nil {
		t.Fatal(err)
	}
	declaration, err := os.ReadFile(filepath.Join(parent, "sample", "internal/resource/configuration.go"))
	if err != nil || !strings.Contains(string(declaration), "const DefaultLocale = \"en\"") {
		t.Fatal("creation-tool language changed project defaults", err)
	}
	status, out, diagnostic = execute(args...)
	if status != 1 || out != "" || !strings.Contains(diagnostic, "已存在") {
		t.Fatal("overwrite refusal", status, out, diagnostic)
	}
}

type failedProjectOutput struct{}

func (failedProjectOutput) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestProjectOutputFailureDoesNotUndoCreation(t *testing.T) {
	parent := t.TempDir()
	var diagnostic bytes.Buffer
	args := []string{"new", "sample", "--directory", parent, "--module", "example.org/business", "--framework-version", "v0.0.0-gh83"}
	status := Run(context.Background(), args, strings.NewReader(""), failedProjectOutput{}, &diagnostic)
	if status != 1 {
		t.Fatal("output failure not reported", status)
	}
	if _, err := os.Stat(filepath.Join(parent, "sample", "go.mod")); err != nil {
		t.Fatal("creation was incorrectly erased")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status = Run(ctx, args, strings.NewReader(""), io.Discard, io.Discard)
	if status != 130 {
		t.Fatal("cancelled command status", status)
	}
	if _, err := os.Stat(filepath.Join(parent, "sample", "go.mod")); errors.Is(err, os.ErrNotExist) {
		t.Fatal("pre-existing project removed")
	}
}
