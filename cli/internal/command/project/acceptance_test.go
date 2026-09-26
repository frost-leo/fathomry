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

package project_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/mod/modfile"
)

func runTool(t *testing.T, directory, executable string, environment []string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = directory
	command.Env = append(os.Environ(), environment...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", executable, args, err, output)
	}
	return output
}

func TestActualGeneratedModuleJourney(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("actual generated-module acceptance currently targets Linux/amd64")
	}
	root, scratch := sourceRoot(t), t.TempDir()
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	env := []string{"GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS="}
	t.Logf("toolchain: %s", bytes.TrimSpace(runTool(t, root, goBinary, env, "version")))
	t.Logf("cache/network conditions: %s", bytes.TrimSpace(runTool(t, root, goBinary, env, "env", "-json", "GOCACHE", "GOMODCACHE", "GOPROXY", "GOSUMDB")))
	generator := filepath.Join(scratch, "fathomry")
	runTool(t, root, goBinary, env, "build", "-o", generator, "./cmd/fathomry")
	invocation := filepath.Join(scratch, "invocation")
	parent := filepath.Join(scratch, "projects")
	for _, directory := range []string{invocation, parent} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(root, filepath.Join(scratch, "source tree")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(parent, filepath.Join(scratch, "parent alias")); err != nil {
		t.Fatal(err)
	}
	before := map[string][32]byte{}
	for _, name := range []string{"go.mod", "go.sum"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		before[name] = sha256.Sum256(data)
	}
	target := filepath.Join(parent, "generated project")
	noTools := []string{"PATH=" + filepath.Join(scratch, "no-tools"), "GOPROXY=off", "GOTOOLCHAIN=local", "GOWORK=off"}
	output := runTool(t, invocation, generator, noTools, "new", "../parent alias/generated project", "--module", "example.org/generated/v2", "--fathomry-source", "../source tree")
	if !bytes.Contains(output, []byte("Project files created.")) {
		t.Fatalf("creation output: %q", output)
	}
	initial := generated(t, target)
	metadata, err := modfile.Parse("go.mod", []byte(initial["go.mod"]), nil)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Module.Mod.Path != "example.org/generated/v2" || metadata.Go.Version != "1.27.0" ||
		len(metadata.Require) != 1 || metadata.Require[0].Mod.Path != "github.com/frost-leo/fathomry" ||
		metadata.Require[0].Mod.Version != "v0.0.0" || len(metadata.Replace) != 1 ||
		metadata.Replace[0].Old.Path != "github.com/frost-leo/fathomry" ||
		metadata.Replace[0].New.Path != "../../source tree" {
		t.Fatalf("incorrect seed: %s", initial["go.mod"])
	}
	if strings.Contains(initial["main.go"], "/internal/") || !strings.Contains(initial["main.go"], "func main() { cli.Main() }") {
		t.Fatalf("wrong public entry: %s", initial["main.go"])
	}
	t.Logf("ordinary tidy: %s", runTool(t, invocation, goBinary, env, "-C", target, "mod", "tidy"))
	if output := runTool(t, invocation, goBinary, env, "-C", target, "mod", "tidy", "-diff"); len(output) != 0 {
		t.Fatalf("second tidy changed output: %s", output)
	}
	if _, err := os.Stat(filepath.Join(target, "go.sum")); err != nil {
		t.Fatal("tidy did not create go.sum", err)
	}
	absent(t, filepath.Join(target, "bin"))
	runTool(t, invocation, goBinary, env, "-C", target, "build", "-mod=readonly", "-o", "./bin/app", ".")
	binary := filepath.Join(target, "bin", "app")
	for _, output := range [][]byte{
		runTool(t, invocation, binary, noTools, "--help"),
		runTool(t, invocation, binary, noTools, "new", "--help"),
		runTool(t, invocation, goBinary, env, "-C", target, "run", "-mod=readonly", ".", "--help"),
	} {
		if !bytes.Contains(output, []byte("new")) || !bytes.Contains(output, []byte("Usage: fathomry")) {
			t.Fatalf("generated executable help: %s", output)
		}
	}
	data, err := os.ReadFile(filepath.Join(target, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	hydrated, err := modfile.Parse("go.mod", data, nil)
	if err != nil || len(hydrated.Replace) != 1 ||
		hydrated.Replace[0].Old != metadata.Replace[0].Old || hydrated.Replace[0].New != metadata.Replace[0].New {
		t.Fatalf("tidy changed replacement: %s %v", data, err)
	}
	packages := strings.Fields(string(runTool(t, target, goBinary, env, "list", "-mod=readonly", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".")))
	expected := []string{
		"example.org/generated/v2",
		"github.com/frost-leo/fathomry/cli",
		"github.com/frost-leo/fathomry/cli/internal/command/project",
		"github.com/spf13/cobra", "github.com/spf13/pflag",
		"golang.org/x/mod/internal/lazyregexp", "golang.org/x/mod/modfile",
		"golang.org/x/mod/module", "golang.org/x/mod/semver",
	}
	slices.Sort(packages)
	slices.Sort(expected)
	if !slices.Equal(packages, expected) {
		t.Fatalf("unexpected production closure:\n%v\nexpected:\n%v", packages, expected)
	}
	t.Logf("exact non-standard production packages: %v", packages)
	build, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	modules := []string{}
	for _, dependency := range build.Deps {
		modules = append(modules, dependency.Path)
		if dependency.Path == "github.com/frost-leo/fathomry" {
			if dependency.Replace == nil || dependency.Replace.Path != "../../source tree" {
				t.Fatalf("binary lost source binding: %+v", dependency)
			}
		} else if dependency.Replace != nil {
			t.Fatalf("unexpected binary replacement: %+v", dependency)
		}
	}
	slices.Sort(modules)
	if !slices.Equal(modules, []string{"github.com/frost-leo/fathomry", "github.com/spf13/cobra", "github.com/spf13/pflag", "golang.org/x/mod"}) {
		t.Fatalf("unrelated SDKs in binary: %v", modules)
	}
	t.Logf("binary Go version: %s; modules: %v", build.GoVersion, modules)
	for name, hash := range before {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || sha256.Sum256(data) != hash {
			t.Fatalf("generation/hydration changed source %s: %v", name, err)
		}
	}
	for _, name := range []string{"main.go", "README.md", ".gitignore"} {
		data, err := os.ReadFile(filepath.Join(target, name))
		if err != nil || string(data) != initial[name] {
			t.Fatalf("user build altered generated %s: %v", name, err)
		}
	}
	grandchild := filepath.Join(parent, "child")
	runTool(t, invocation, binary, noTools, "new", grandchild, "--module", "example.org/child", "--fathomry-source", root)
	generated(t, grandchild)
	coldContext, coldCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer coldCancel()
	cold := exec.CommandContext(coldContext, goBinary, "mod", "tidy")
	cold.Dir = grandchild
	cold.Env = append(append(os.Environ(), env...), "GOMODCACHE="+filepath.Join(scratch, "cold-modules"), "GOPROXY=off", "GOSUMDB=off")
	coldOutput, coldErr := cold.CombinedOutput()
	if coldContext.Err() != nil || coldErr == nil || !bytes.Contains(coldOutput, []byte("module lookup disabled by GOPROXY=off")) {
		t.Fatalf("cold-offline control did not reject dependencies: %v\n%s", coldErr, coldOutput)
	}
	t.Logf("cold-offline dependency control rejected as expected: %s", bytes.TrimSpace(coldOutput))
	t.Run("readme-through-unequal-depth-alias", func(t *testing.T) {
		deepParent := filepath.Join(scratch, "deeper", "physical", "projects")
		if err := os.MkdirAll(deepParent, 0o700); err != nil {
			t.Fatal(err)
		}
		alias := filepath.Join(scratch, "short-alias")
		if err := os.Symlink(deepParent, alias); err != nil {
			t.Fatal(err)
		}
		logicalTarget := filepath.Join(alias, "app")
		physicalTarget := filepath.Join(deepParent, "app")
		runTool(t, invocation, generator, noTools, "new", logicalTarget, "--module", "example.org/readme", "--fathomry-source", "../source tree")
		files := generated(t, physicalTarget)
		_, afterFence, found := strings.Cut(files["README.md"], "```sh\n")
		recipe, _, closed := strings.Cut(afterFence, "\n```")
		if !found || !closed || !strings.HasPrefix(recipe, "cd -P .\n") {
			t.Fatal("README must provide the physical-directory build recipe")
		}
		shellEnv := append(append([]string{}, env...), "PWD="+logicalTarget,
			"PATH="+filepath.Dir(goBinary)+string(os.PathListSeparator)+os.Getenv("PATH"))
		output := runTool(t, logicalTarget, "/bin/sh", shellEnv, "-ec", recipe)
		if bytes.Count(output, []byte("Usage: fathomry")) != 2 {
			t.Fatalf("actual README build/run recipe did not complete: %s", output)
		}
		if output := runTool(t, scratch, goBinary, env, "-C", physicalTarget, "mod", "tidy", "-diff"); len(output) != 0 {
			t.Fatalf("README consumer tidy is unstable: %s", output)
		}
		data, err := os.ReadFile(filepath.Join(physicalTarget, "go.mod"))
		if err != nil {
			t.Fatal(err)
		}
		metadata, err := modfile.Parse("go.mod", data, nil)
		if err != nil || len(metadata.Replace) != 1 || metadata.Replace[0].New.Path != "../../../../source tree" {
			t.Fatalf("wrong physical source binding after README recipe: %s %v", data, err)
		}
	})
}

func TestGeneratedIgnoreSemantics(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("Git is unavailable for ignore-rule semantics")
	}
	target := filepath.Join(t.TempDir(), "app")
	status, err, diagnostic := invoke(context.Background(), arguments(target, sourceRoot(t), "example.org/ignore"), &bytes.Buffer{})
	if status != 0 || err != nil {
		t.Fatalf("create: %d %v %s", status, err, diagnostic)
	}
	generated(t, target)
	runTool(t, target, git, nil, "init", "--quiet")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, git, "-c", "core.excludesFile=/dev/null", "check-ignore", "--no-index", "--stdin")
	command.Dir = target
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	command.Stdin = strings.NewReader("bin/app\napp.exe\nsuite.test\ncoverage.out\ngo.work\ngo.work.sum\n.env\ngo.sum\ngo.mod\nmain.go\nREADME.md\nvendor/example\n")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("explicit ignore check: %v %s", err, output)
	}
	want := []string{"bin/app", "app.exe", "suite.test", "coverage.out", "go.work", "go.work.sum", ".env"}
	if !slices.Equal(strings.Fields(string(output)), want) {
		t.Fatalf("wrong ignore semantics: %s", output)
	}
}

func TestConstructorHasNoExternalRuntimeImports(t *testing.T) {
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	var info struct{ Imports []string }
	output := runTool(t, sourceRoot(t), goBinary, []string{"GOWORK=off", "GOTOOLCHAIN=local"}, "list", "-json", "./cli/internal/command/project")
	if err := json.Unmarshal(output, &info); err != nil {
		t.Fatal(err)
	}
	for _, path := range info.Imports {
		if path == "os/exec" || path == "net" || strings.HasPrefix(path, "net/") ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/") {
			t.Errorf("generator acquires external runtime capabilities: %s", path)
		}
	}
}
