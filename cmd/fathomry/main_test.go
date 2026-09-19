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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func buildCLI(t *testing.T, linkerFlags string, vcs bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fathomry")
	args := []string{"build", "-trimpath", "-o", path}
	if vcs {
		args = append(args, "-buildvcs=true")
	} else {
		args = append(args, "-buildvcs=false")
	}
	if linkerFlags != "" {
		args = append(args, "-ldflags", linkerFlags)
	}
	args = append(args, "./cmd/fathomry")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), args...)
	command.Dir = filepath.Join("..", "..")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %q: %v\n%s", linkerFlags, err, output)
	}
	return path
}

func executeCLI(t *testing.T, path string, args ...string) (int, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, path, args...)
	command.Dir = t.TempDir()
	command.Stdin = strings.NewReader("")
	var out, diagnostic bytes.Buffer
	command.Stdout, command.Stderr = &out, &diagnostic
	err := command.Run()
	if err == nil {
		return 0, out.String(), diagnostic.String()
	}
	if exit, ok := err.(*exec.ExitError); ok && ctx.Err() == nil {
		return exit.ExitCode(), out.String(), diagnostic.String()
	}
	t.Fatalf("artifact execution: %v context=%v", err, ctx.Err())
	return 0, "", ""
}

func TestActualArtifactCommandsAndEnvironment(t *testing.T) {
	path := buildCLI(t, "", true)
	for _, test := range []struct {
		args   []string
		status int
		out    string
		err    string
	}{
		{nil, 0, "Usage:", ""},
		{[]string{"--lang=zh-CN", "--help"}, 0, "用法：", ""},
		{[]string{"--lang=fr", "--help"}, 0, "Usage:", ""},
		{[]string{"help", "version", "--lang=zh-CN"}, 0, "用法：", ""},
		{[]string{"version"}, 0, "Application module:", ""},
		{[]string{"--version", "--output=json"}, 0, `"schema":"fathomry.cli.version/v1"`, ""},
		{[]string{"version", "--unknown"}, 2, "", "Invalid command"},
		{[]string{"missing", "--help"}, 2, "", "Invalid command"},
		{[]string{"completion", "--help"}, 2, "", "Invalid command"},
		{[]string{"__complete", "version"}, 2, "", "Invalid command"},
	} {
		status, out, diagnostic := executeCLI(t, path, test.args...)
		if status != test.status || !strings.Contains(out, test.out) || !strings.Contains(diagnostic, test.err) ||
			test.status == 0 && diagnostic != "" || test.status != 0 && out != "" {
			t.Fatalf("args=%q status=%d stdout=%q stderr=%q", test.args, status, out, diagnostic)
		}
	}
	poisoned := exec.Command(path, "version", "--output=json")
	poisoned.Dir = t.TempDir()
	poisoned.Env = []string{
		"HOME=" + poisoned.Dir, "XDG_CONFIG_HOME=" + filepath.Join(poisoned.Dir, "config"),
		"PATH=" + poisoned.Dir, "LANG=invalid", "LC_ALL=invalid", "FATHOMRY_CONFIG=malformed",
	}
	if runtime.GOOS != "windows" {
		canary := "#!/bin/sh\ntouch '" + filepath.Join(poisoned.Dir, "git-invoked") + "'\nexit 99\n"
		if err := os.WriteFile(filepath.Join(poisoned.Dir, "git"), []byte(canary), 0700); err != nil {
			t.Fatal(err)
		}
	}
	poisoned.Stdin = strings.NewReader("")
	var out, diagnostic bytes.Buffer
	poisoned.Stdout, poisoned.Stderr = &out, &diagnostic
	if err := poisoned.Run(); err != nil || diagnostic.Len() != 0 {
		t.Fatalf("offline artifact: %v stdout=%q stderr=%q", err, out.String(), diagnostic.String())
	}
	var record map[string]any
	if err := json.Unmarshal(out.Bytes(), &record); err != nil || record["declaration"] != nil {
		t.Fatalf("unstamped record: %v %q", err, out.String())
	}
	for _, name := range []string{"git-invoked", "config"} {
		if _, err := os.Stat(filepath.Join(poisoned.Dir, name)); !os.IsNotExist(err) {
			t.Fatalf("incidental %s: %v", name, err)
		}
	}
}

func TestCommandDependencyDirection(t *testing.T) {
	for _, test := range []struct {
		packagePath string
		banned      []string
	}{
		{"./cmd/fathomry/internal/cli", []string{
			"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/",
			"github.com/frost-leo/fathomry/version",
		}},
		{"./cmd/fathomry", []string{
			"github.com/frost-leo/fathomry/internal/broker/",
			"github.com/frost-leo/fathomry/internal/browser/",
			"github.com/frost-leo/fathomry/internal/cache/",
			"github.com/frost-leo/fathomry/internal/configsource/",
			"github.com/frost-leo/fathomry/internal/database/",
			"github.com/frost-leo/fathomry/internal/httpclient/",
			"github.com/frost-leo/fathomry/internal/sqlengine/",
			"github.com/frost-leo/fathomry/internal/telemetry/",
			"go.temporal.io/", "github.com/spf13/viper", "github.com/duckdb/",
		}},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		command := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "list", "-deps", test.packagePath)
		command.Dir = filepath.Join("..", "..")
		output, err := command.Output()
		cancel()
		if err != nil {
			t.Fatalf("list %s: %v", test.packagePath, err)
		}
		for _, path := range strings.Fields(string(output)) {
			for _, banned := range test.banned {
				if strings.HasPrefix(path, banned) {
					t.Errorf("%s imports %s across boundary", test.packagePath, path)
				}
			}
		}
	}
}

func TestActualArtifactDeclarations(t *testing.T) {
	revisionCommand := exec.Command("git", "rev-parse", "HEAD")
	revisionCommand.Dir = filepath.Join("..", "..")
	revisionBytes, err := revisionCommand.Output()
	if err != nil {
		t.Fatal(err)
	}
	revision := strings.TrimSpace(string(revisionBytes))
	statusCommand := exec.Command("git", "status", "--porcelain")
	statusCommand.Dir = filepath.Join("..", "..")
	statusBytes, err := statusCommand.Output()
	if err != nil {
		t.Fatal(err)
	}
	tree := "clean"
	if len(statusBytes) > 0 {
		tree = "dirty"
	}
	const release = "v1.2.3+cli77"
	const buildTime = "2026-09-19T12:34:56Z"
	stamp := func(release, revision, tree, buildTime, target string) string {
		values := []string{release, revision, tree, buildTime}
		fields := []string{"release", "gitRevision", "tree", "buildTime"}
		var flags []string
		for index, field := range fields {
			flags = append(flags, "-X=github.com/frost-leo/fathomry/version."+target+field+"="+values[index])
		}
		return strings.Join(flags, " ")
	}
	path := buildCLI(t, stamp(release, revision, tree, buildTime, ""), true)
	status, out, diagnostic := executeCLI(t, path, "version", "--output=json")
	if status != 0 || diagnostic != "" {
		t.Fatalf("stamped report: status=%d stdout=%q stderr=%q", status, out, diagnostic)
	}
	type claim struct {
		Release   string `json:"release"`
		Revision  string `json:"revision"`
		Tree      string `json:"tree"`
		BuildTime string `json:"build_time"`
	}
	var report struct {
		Declaration claim `json:"declaration"`
	}
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatal(err)
	}
	if report.Declaration.Release != release || report.Declaration.Revision != revision ||
		report.Declaration.Tree != tree || report.Declaration.BuildTime != buildTime {
		t.Fatalf("independent four-field readback: %+v", report.Declaration)
	}
	_, root, _ := executeCLI(t, path, "--version", "--output=json")
	if root != out {
		t.Fatal("root and subcommand reports differ")
	}
	for _, test := range []struct {
		name, flags string
		status      int
	}{
		{"partial", "-X=github.com/frost-leo/fathomry/version.release=v1.2.3", 1},
		{"invalid", stamp("not-a-version", revision, tree, buildTime, ""), 1},
		{"conflict", stamp(release, strings.Repeat("0", len(revision)), tree, buildTime, ""), 1},
		{"all-misspelled", stamp(release, revision, tree, buildTime, "wrong"), 0},
		{"unexpected-valid", stamp("v1.2.4", revision, tree, buildTime, ""), 0},
		{"stripped", "-s -w " + stamp(release, revision, tree, buildTime, ""), 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := buildCLI(t, test.flags, true)
			status, out, diagnostic := executeCLI(t, path, "version", "--output=json")
			if status != test.status || test.status == 0 && diagnostic != "" || test.status != 0 && out != "" {
				t.Fatalf("status=%d stdout=%q stderr=%q", status, out, diagnostic)
			}
			if test.status != 0 {
				helpStatus, help, _ := executeCLI(t, path, "--help")
				if helpStatus != 0 || !strings.Contains(help, "Usage:") {
					t.Fatal("invalid declaration disabled help")
				}
			}
			if test.name == "all-misspelled" && !strings.Contains(out, `"declaration":null`) {
				t.Fatal("misspelled stamp did not fail independent expectation")
			}
			if test.name == "unexpected-valid" && strings.Contains(out, `"release":"`+release+`"`) {
				t.Fatal("unexpected complete declaration matched the producer expectation")
			}
			if test.name == "stripped" {
				var stripped struct {
					Declaration claim `json:"declaration"`
				}
				if err := json.Unmarshal([]byte(out), &stripped); err != nil ||
					stripped.Declaration.Release != release || stripped.Declaration.Revision != revision ||
					stripped.Declaration.Tree != tree || stripped.Declaration.BuildTime != buildTime {
					t.Fatalf("stripped readback: %v %+v", err, stripped.Declaration)
				}
			}
		})
	}
	noVCS := buildCLI(t, "", false)
	status, out, diagnostic = executeCLI(t, noVCS, "version", "--output=json")
	if status != 0 || diagnostic != "" || !strings.Contains(out, `"tree":"unknown"`) || !strings.Contains(out, `"revision":{"state":"unknown","value":null}`) {
		t.Fatalf("no-VCS facts invented: status=%d stdout=%q stderr=%q", status, out, diagnostic)
	}
}
