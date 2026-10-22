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

package declaration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

func consumerEnvironment(offline bool) []string {
	var environment []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		switch name {
		case "GOWORK", "GOTOOLCHAIN", "GOFLAGS":
			continue
		}
		if strings.HasPrefix(name, "FATHOMRY_") {
			continue
		}
		if offline && (name == "GOPROXY" || name == "GOSUMDB") {
			continue
		}
		environment = append(environment, entry)
	}
	environment = append(environment, "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=")
	if offline {
		environment = append(environment, "GOPROXY=off", "GOSUMDB=off")
	}
	return environment
}

func TestIndependentConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("independent module and actual CLI qualification")
	}
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	source, err := os.ReadFile("testdata/health/declaration.go")
	if err != nil {
		t.Fatal(err)
	}
	for name, sourcePath := range map[string]string{"main.go": "testdata/consumer/main.go", "main_test.go": "testdata/consumer/main_test.go", "health/declaration.go": "testdata/health/declaration.go"} {
		raw, err := os.ReadFile(sourcePath)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte("fathomry/internal/")) || bytes.Contains(raw, []byte("fathomry/cmd/")) {
			t.Fatal("private consumer import")
		}
		path := filepath.Join(project, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	module := fmt.Sprintf("module example.org/workflow-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))
	if err := os.WriteFile(filepath.Join(project, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	goTool := filepath.Join(runtime.GOROOT(), "bin", "go")
	if runtime.GOOS == "windows" {
		goTool += ".exe"
	}
	run := func(directory string, offline bool, program string, args ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		process := exec.CommandContext(ctx, program, args...)
		process.Dir = directory
		process.Env = consumerEnvironment(offline)
		output, err := process.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("command timed out: %s %v", filepath.Base(program), args)
		}
		return output, err
	}
	require := func(directory string, offline bool, program string, args ...string) []byte {
		t.Helper()
		output, err := run(directory, offline, program, args...)
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", filepath.Base(program), args, err, output)
		}
		return output
	}
	require(project, false, goTool, "mod", "tidy")
	require(project, true, goTool, "mod", "tidy", "-diff")
	selected := require(project, true, goTool, "list", "-m", "-f", "{{.Replace.Dir}}", "github.com/frost-leo/fathomry")
	if strings.TrimSpace(string(selected)) != root {
		t.Fatal("wrong providing module", string(selected))
	}
	dependencies := require(project, true, goTool, "list", "-deps", "github.com/frost-leo/fathomry/framework/workflow/declaration/v1")
	for _, forbidden := range []string{"fathomry/adapters/", "fathomry/framework/configuration/", "go.temporal.io/sdk"} {
		if bytes.Contains(dependencies, []byte(forbidden)) {
			t.Fatal("pure declaration imported runtime", forbidden)
		}
	}
	require(project, true, goTool, "vet", "./...")
	require(project, true, goTool, "test", "-mod=readonly", "-race", "-count=1", "./...")
	binaries := t.TempDir()
	exporter := filepath.Join(binaries, "exporter")
	official := filepath.Join(binaries, "fathomry")
	if runtime.GOOS == "windows" {
		exporter += ".exe"
		official += ".exe"
	}
	require(project, true, goTool, "build", "-mod=readonly", "-o", exporter, ".")
	require(root, true, goTool, "build", "-mod=readonly", "-o", official, "./cmd/fathomry")
	original := require(project, true, exporter)
	golden, err := os.ReadFile("testdata/health.bundle.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, bytes.TrimSpace(golden)) {
		t.Fatal("project Go declarations did not generate the independent expected bundle")
	}
	first := filepath.Join(project, "first.json")
	if err := os.WriteFile(first, original, 0600); err != nil {
		t.Fatal(err)
	}
	parseReport := func(output []byte) wf.Report {
		t.Helper()
		var value struct {
			Schema  string
			Command string
			Data    wf.Report
		}
		if err := json.Unmarshal(output, &value); err != nil || value.Schema != "fathomry.cli/v1" || value.Command != "workflow.vet" {
			t.Fatal("invalid CLI envelope", err, string(output))
		}
		return value.Data
	}
	baseline := parseReport(require(project, true, official, "workflow", "vet", first, "--output", "json", "--lang", "en"))
	if !baseline.Passed || baseline.Workflows != 1 || baseline.Modes != 2 {
		t.Fatal(baseline)
	}
	t.Run("documented_regeneration", func(t *testing.T) {
		testRegenerationRecipe(t, project, goTool, binaries, source, original)
	})
	oldToken := "Type: \"health/read-cpu\", Revision: \"1\""
	newSource := strings.Replace(string(source), oldToken, "Type: \"health/read-cpu\", Revision: \"2\"", 1)
	if newSource == string(source) {
		t.Fatal("mutation did not change source")
	}
	sourcePath := filepath.Join(project, "health", "declaration.go")
	if err := os.WriteFile(sourcePath, []byte(newSource), 0600); err != nil {
		t.Fatal(err)
	}
	stale := parseReport(require(project, true, official, "workflow", "vet", first, "--output", "json", "--lang", "en"))
	if !stale.Passed || stale.Entries[0].SHA256 != baseline.Entries[0].SHA256 {
		t.Fatal("checker pretended to inspect source")
	}
	require(project, true, goTool, "build", "-mod=readonly", "-o", exporter, ".")
	secondRaw := require(project, true, exporter)
	if bytes.Equal(secondRaw, original) {
		t.Fatal("explicit regeneration did not observe changed source")
	}
	second := filepath.Join(project, "second.json")
	if err := os.WriteFile(second, secondRaw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	process := exec.CommandContext(ctx, official, "workflow", "vet", first, second, "--output", "json", "--lang", "en")
	process.Dir = project
	process.Env = consumerEnvironment(true)
	var stdout, stderr bytes.Buffer
	process.Stdout = &stdout
	process.Stderr = &stderr
	err = process.Run()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 1 || ctx.Err() != nil {
		t.Fatal("conflicting supplied versions did not fail", err)
	}
	conflict := parseReport(stdout.Bytes())
	if conflict.Passed || len(conflict.Conflicts) != 1 || conflict.UncomparedModes != 2 || stderr.Len() == 0 {
		t.Fatal(conflict, stderr.String())
	}
	badSource := strings.Replace(newSource, "Code: \"server-health\"", "Code: \"INVALID\"", 1)
	if badSource == newSource {
		t.Fatal("invalid mutation did not change source")
	}
	if err := os.WriteFile(sourcePath, []byte(badSource), 0600); err != nil {
		t.Fatal(err)
	}
	require(project, true, goTool, "build", "-mod=readonly", "-o", exporter, ".")
	failed, err := run(project, true, exporter)
	if err == nil || len(failed) != 0 {
		t.Fatal("failed exporter produced a successful artifact", err, string(failed))
	}
	stillOriginal, err := os.ReadFile(first)
	if err != nil || !bytes.Equal(stillOriginal, original) {
		t.Fatal("previous artifact mutated")
	}
}

func testRegenerationRecipe(t *testing.T, project, goTool, binaries string, source, original []byte) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the documented POSIX recipe is not qualified on Windows")
	}
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("the documented regeneration recipe requires sh")
	}
	document, err := os.ReadFile("testdata/consumer/README.md")
	if err != nil {
		t.Fatal(err)
	}
	const start = `GOWORK=off "$go_tool" build -mod=readonly -o "$job/exporter" .`
	_, tail, found := strings.Cut(string(document), start)
	if !found {
		t.Fatal("documented exporter build command is missing")
	}
	recipe, _, found := strings.Cut(start+tail, "\n```")
	if !found {
		t.Fatal("documented regeneration block is incomplete")
	}
	run := func() ([]byte, []byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		process := exec.CommandContext(ctx, shell, "-c", "set +e\ngo_tool=$1\njob=$2\n"+recipe, "regenerate", goTool, binaries)
		process.Dir = project
		process.Env = consumerEnvironment(true)
		var stdout, stderr bytes.Buffer
		process.Stdout, process.Stderr = &stdout, &stderr
		err := process.Run()
		if ctx.Err() != nil {
			t.Fatal("documented regeneration timed out", ctx.Err())
		}
		return stdout.Bytes(), stderr.Bytes(), err
	}
	stdout, stderr, err := run()
	if err != nil {
		t.Fatalf("valid documented regeneration: %v\n%s", err, stderr)
	}
	var result struct{ Data wf.Report }
	if err := json.Unmarshal(stdout, &result); err != nil || !result.Data.Passed {
		t.Fatal("valid recipe did not complete checking", err)
	}
	artifact := filepath.Join(binaries, "health.bundle.json")
	previous := append(bytes.Clone(original), '\n')
	if err := os.WriteFile(artifact, previous, 0600); err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(project, "health", "declaration.go")
	t.Cleanup(func() {
		if err := os.WriteFile(sourcePath, source, 0600); err != nil {
			t.Error(err)
		}
	})
	if err := os.WriteFile(sourcePath, append(bytes.Clone(source), []byte("\nthis is not valid Go\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = run()
	if err == nil || len(stdout) != 0 || len(stderr) == 0 {
		t.Fatal("failed rebuild continued to export or check", err)
	}
	retained, err := os.ReadFile(artifact)
	if err != nil || !bytes.Equal(retained, previous) {
		t.Fatal("failed rebuilding republished the previous artifact", err)
	}
	if _, err := os.Stat(filepath.Join(binaries, "bundle.tmp")); !os.IsNotExist(err) {
		t.Fatal("failed rebuilding entered the export step", err)
	}
}
