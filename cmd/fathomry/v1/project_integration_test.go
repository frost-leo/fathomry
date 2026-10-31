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

package cli_test

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestIndependentProjectEntry(t *testing.T) {
	product, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	job := t.TempDir()
	consumer := filepath.Join(job, "consumer")
	source := os.DirFS("testdata/project")
	if err := fs.WalkDir(source, ".", func(path string, entry fs.DirEntry, problem error) error {
		if problem != nil {
			return problem
		}
		target := filepath.Join(consumer, filepath.FromSlash(path))
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		data, err := fs.ReadFile(source, path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0600)
	}); err != nil {
		t.Fatal(err)
	}
	module := fmt.Sprintf("module example.org/fathomry-startup-consumer\n\ngo 1.27.0\n\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n\nrequire (\n github.com/BurntSushi/toml v1.6.0\n gopkg.in/yaml.v2 v2.4.0\n)\n", filepath.ToSlash(product))
	if err := os.WriteFile(filepath.Join(consumer, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	environment := []string{}
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "FATHOMRY_") {
			environment = append(environment, value)
		}
	}
	environment = append(environment, "GOWORK=off", "GOTOOLCHAIN=local")
	run := func(directory string, env []string, program string, args ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, program, args...)
		command.Dir, command.Env = directory, env
		command.WaitDelay = 5 * time.Second
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("consumer command timed out: %v", args)
		}
		return output, err
	}
	require := func(directory string, env []string, program string, args ...string) []byte {
		t.Helper()
		output, err := run(directory, env, program, args...)
		if err != nil {
			t.Fatalf("consumer %v: %v\n%s", args, err, output)
		}
		return output
	}
	goTool := filepath.Join(runtime.GOROOT(), "bin", "go")
	require(consumer, environment, goTool, "mod", "tidy")
	offline := append(append([]string{}, environment...), "GOPROXY=off")
	require(consumer, offline, goTool, "mod", "tidy", "-diff")
	require(consumer, offline, goTool, "vet", "-mod=readonly", "./...")
	require(consumer, offline, goTool, "test", "-mod=readonly", "-race", "-count=1", "./...")
	binary := filepath.Join(job, "application")
	require(consumer, offline, goTool, "build", "-mod=readonly", "-o", binary, ".")
	sourceResult := require(consumer, offline, goTool, "run", "-mod=readonly", ".", "config", "check", "--output=json")
	sourceHelp := require(consumer, offline, goTool, "run", "-mod=readonly", ".", "--help")
	runtimeDir := filepath.Join(job, "runtime")
	if err := os.Mkdir(runtimeDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(consumer, "config"), filepath.Join(runtimeDir, "config")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(consumer, ".env"), filepath.Join(runtimeDir, ".env")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(consumer, filepath.Join(job, "source-not-at-build-location")); err != nil {
		t.Fatal(err)
	}
	emptyPath := filepath.Join(job, "no-tools")
	if err := os.Mkdir(emptyPath, 0700); err != nil {
		t.Fatal(err)
	}
	isolated := []string{"PATH=" + emptyPath, "HOME=" + runtimeDir}
	binaryResult := require(job, isolated, binary, "config", "check", "--bootstrap", filepath.Join(runtimeDir, ".env"), "--output=json")
	if !bytes.Equal(sourceResult, binaryResult) {
		t.Fatal("source and relocated binary results differ", string(sourceResult), string(binaryResult))
	}
	if got := require(job, isolated, binary, "--help"); !bytes.Equal(sourceHelp, got) {
		t.Fatal("standalone help differs")
	}
	if err := os.Remove(filepath.Join(runtimeDir, "config/environment/development.yaml")); err != nil {
		t.Fatal(err)
	}
	output, err := run(job, isolated, binary, "config", "check", "--bootstrap", filepath.Join(runtimeDir, ".env"), "--output=json")
	if err == nil || !bytes.Contains(output, []byte("identifier")) {
		t.Fatal("missing configuration was not rejected", err, string(output))
	}
	t.Log("hand-authored root project, public-only imports, root execution and source-independent binary passed; not generated/published-module qualification")
}
