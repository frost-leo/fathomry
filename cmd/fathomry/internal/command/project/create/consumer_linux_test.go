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

package create_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

func runTool(t *testing.T, directory string, environment []string, executable string, args ...string) ([]byte, error) {
	t.Helper()
	budget := 3 * time.Minute
	if len(args) >= 2 && args[0] == "mod" && args[1] == "tidy" && !slices.Contains(args, "-diff") {
		budget = 5 * time.Minute
	}
	deadline := time.Now().Add(budget)
	if outer, ok := t.Deadline(); ok && outer.Add(-10*time.Second).Before(deadline) {
		deadline = outer.Add(-10 * time.Second)
	}
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = directory
	command.Env = environment
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = 5 * time.Second
	started := time.Now()
	output, err := command.CombinedOutput()
	t.Logf("phase %s %v: %s", filepath.Base(executable), args, time.Since(started).Round(time.Millisecond))
	if ctx.Err() != nil {
		t.Fatalf("bounded command timed out: %s %v: %v\n%s", executable, args, ctx.Err(), output)
	}
	return output, err
}

func requireTool(t *testing.T, directory string, environment []string, executable string, args ...string) []byte {
	t.Helper()
	output, err := runTool(t, directory, environment, executable, args...)
	if err != nil {
		t.Fatalf("command %s %v failed: %v\n%s", executable, args, err, output)
	}
	return output
}

func TestActualGeneratedPublishedConsumer(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Fatal("this native acceptance profile requires Linux")
	}
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	environment := append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=")
	moduleFile := strings.TrimSpace(string(requireTool(t, ".", environment, goTool, "env", "GOMOD")))
	product := filepath.Dir(moduleFile)
	job := t.TempDir()
	tool := filepath.Join(job, "fathomry")
	requireTool(t, product, environment, goTool, "build", "-mod=readonly", "-o", tool, "./cmd/fathomry")
	candidateDeps := string(requireTool(t, product, environment, goTool, "list", "-mod=readonly", "-deps", "./cmd/fathomry/v1"))
	if strings.Contains(candidateDeps, "/internal/command/project") {
		t.Fatal("public CLI imported developer generation")
	}
	parent := filepath.Join(job, "generation")
	noTools := filepath.Join(job, "no-tools")
	for _, path := range []string{parent, noTools} {
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatal(err)
		}
	}
	noToolEnvironment := []string{"PATH=" + noTools, "HOME=" + job, "FATHOMRY_LANG=zh-CN"}
	pin := "v0.0.0-20261028081050-b28e37d55e53"
	raw := requireTool(t, parent, noToolEnvironment, tool, "--output=json", "project", "new",
		"--name=ordersync", "--module=example.com/generated/app", "--framework-version="+pin)
	var result struct {
		Data struct {
			Target string `json:"target"`
			State  string `json:"state"`
			Files  []struct {
				Path         string
				Complete     bool
				BytesWritten int64 `json:"bytes_written"`
			} `json:"files"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &result) != nil || result.Data.Target != "ordersync" || result.Data.State != "complete" || len(result.Data.Files) != 6 {
		t.Fatal("creation report invalid", string(raw))
	}
	consumer := filepath.Join(parent, "ordersync")
	var paths []string
	err = filepath.WalkDir(consumer, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			relative, err := filepath.Rel(consumer, path)
			if err != nil {
				return err
			}
			paths = append(paths, filepath.ToSlash(relative))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{".gitignore", "README.md", "cmd/ordersync/main.go", "cmd/ordersync/main_test.go", "config/README.md", "go.mod"}
	if !slices.Equal(paths, want) {
		t.Fatal("unexpected initial tree", paths)
	}
	for _, file := range result.Data.Files {
		data, err := os.ReadFile(filepath.Join(consumer, filepath.FromSlash(file.Path)))
		if err != nil || !file.Complete || file.BytesWritten != int64(len(data)) {
			t.Fatal("reported bytes/completion differ", file.Path, err)
		}
	}
	initial, err := os.ReadFile(filepath.Join(consumer, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(initial, []byte("module example.com/generated/app\n")) ||
		!bytes.Contains(initial, []byte("go 1.27.0\n")) ||
		!bytes.Contains(initial, []byte("require github.com/frost-leo/fathomry "+pin+"\n")) ||
		bytes.Contains(initial, []byte("replace")) {
		t.Fatal("generated module differs", string(initial))
	}
	cache := filepath.Join(job, "modules")
	if err := os.Mkdir(cache, 0755); err != nil {
		t.Fatal(err)
	}
	consumerEnvironment := append(slices.Clone(environment), "GOMODCACHE="+cache, "GOFLAGS=-modcacherw")
	offline := append(slices.Clone(consumerEnvironment), "GOPROXY=off")
	rejected, err := runTool(t, consumer, offline, goTool, "test", "-mod=readonly", "./...")
	if err == nil || !bytes.Contains(rejected, []byte("missing go.sum entry")) ||
		!bytes.Contains(rejected, []byte("github.com/frost-leo/fathomry/cmd/fathomry/v1")) {
		t.Fatal("pre-resolution control failed for wrong reason", err, string(rejected))
	}
	requireTool(t, consumer, consumerEnvironment, goTool, "mod", "tidy")
	requireTool(t, consumer, offline, goTool, "mod", "tidy", "-diff")
	requireTool(t, consumer, offline, goTool, "vet", "-mod=readonly", "./...")
	requireTool(t, consumer, offline, goTool, "test", "-mod=readonly", "-race", "-count=1", "./...")
	binary := filepath.Join(job, "ordersync")
	requireTool(t, consumer, offline, goTool, "build", "-mod=readonly", "-o", binary, "./cmd/ordersync")
	sourceHelp := requireTool(t, consumer, offline, goTool, "run", "-mod=readonly", "./cmd/ordersync", "--lang=zh-CN", "--output=json", "help")
	provenance := string(requireTool(t, job, environment, goTool, "version", "-m", binary))
	if !strings.Contains(provenance, "github.com/frost-leo/fathomry\t"+pin+"\t") || strings.Contains(provenance, "\t=>") {
		t.Fatal("unqualified dependency", provenance)
	}
	deps := string(requireTool(t, consumer, offline, goTool, "list", "-mod=readonly", "-deps", "./cmd/ordersync"))
	for _, path := range strings.Fields(deps) {
		for _, forbidden := range []string{
			"github.com/frost-leo/fathomry/adapters/", "github.com/frost-leo/fathomry/framework/",
			"github.com/frost-leo/fathomry/schema/", "github.com/frost-leo/fathomry/internal/",
			"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project",
		} {
			if strings.HasPrefix(path, forbidden) {
				t.Fatal("unexpected consumer dependency", path)
			}
		}
	}
	moved := filepath.Join(job, "moved-source")
	if err := os.Rename(consumer, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(cache, cache+"-moved"); err != nil {
		t.Fatal(err)
	}
	isolated := filepath.Join(job, "standalone")
	if err := os.Mkdir(isolated, 0755); err != nil {
		t.Fatal(err)
	}
	binaryHelp := requireTool(t, isolated, noToolEnvironment, binary, "--lang=zh-CN", "--output=json", "help")
	if !bytes.Equal(sourceHelp, binaryHelp) {
		t.Fatal("successful source/binary help differs")
	}
	refused, err := runTool(t, isolated, noToolEnvironment, binary, "--output=json", "project", "new")
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 2 || !json.Valid(refused) {
		t.Fatal("project shell admitted generator", err, string(refused))
	}
	t.Log("actual six-file generation, cold published resolution, offline checks, relocated source/cache and standalone shell passed")
}
