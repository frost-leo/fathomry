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

package create

import (
	"bytes"
	"context"
	"encoding/json"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func TestDependencyIdentities(t *testing.T) {
	imports := map[string]bool{}
	for _, file := range prepared(t).files {
		if !strings.HasSuffix(file.path, ".go") {
			continue
		}
		source, err := parser.ParseFile(token.NewFileSet(), file.path, file.data, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range source.Imports {
			name, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if name != validOptions().Module && !strings.HasPrefix(name, validOptions().Module+"/") {
				imports[name] = true
			}
		}
	}
	var roots []string
	for name := range imports {
		roots = append(roots, name)
	}
	slices.Sort(roots)
	modules := map[string]string{}
	// Tidy includes non-host imports. Windows adds Cobra's Mousetrap module;
	// package-directory scans below ignore build tags on every selected module.
	for _, goos := range []string{"linux", "windows"} {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		goTool := filepath.Join(runtime.GOROOT(), "bin", "go")
		if runtime.GOOS == "windows" {
			goTool += ".exe"
		}
		args := append([]string{"list", "-mod=readonly", "-deps", "-json"}, roots...)
		command := exec.CommandContext(ctx, goTool, args...)
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=",
			"CGO_ENABLED=0", "GOOS="+goos, "GOARCH=amd64")
		command.WaitDelay = 5 * time.Second
		var diagnostic bytes.Buffer
		command.Stderr = &diagnostic
		output, err := command.Output()
		cancel()
		if err != nil {
			t.Fatalf("%s dependency modules: %v\n%s", goos, err, diagnostic.Bytes())
		}
		decoder := json.NewDecoder(bytes.NewReader(output))
		for {
			var item struct {
				Module *struct{ Path, Dir string }
			}
			if err := decoder.Decode(&item); err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			if item.Module != nil {
				if item.Module.Path == "" || item.Module.Dir == "" {
					t.Fatal("unresolved dependency module", item.Module)
				}
				if previous := modules[item.Module.Path]; previous != "" && previous != item.Module.Dir {
					t.Fatal("inconsistent dependency source", item.Module.Path)
				}
				modules[item.Module.Path] = item.Module.Dir
			}
		}
	}
	var identities, packages []string
	for modulePath, directory := range modules {
		identities = append(identities, modulePath)
		found, err := dependencyPackageDirectories(modulePath, os.DirFS(directory))
		if err != nil {
			t.Fatal(modulePath, err)
		}
		packages = append(packages, found...)
	}
	slices.Sort(identities)
	if !slices.Equal(identities, dependencyModules) {
		t.Fatalf("dependency module inventory differs: got %v; want %v", identities, dependencyModules)
	}
	slices.Sort(packages)
	packages = slices.Compact(packages)
	var recorded []string
	for line := range strings.SplitSeq(dependencyPackages, "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			recorded = append(recorded, line)
		}
	}
	if !slices.IsSorted(recorded) || len(slices.Compact(slices.Clone(recorded))) != len(recorded) {
		t.Fatal("dependency package inventory must be sorted and unique")
	}
	if !slices.Equal(packages, recorded) {
		for _, name := range packages {
			if _, found := slices.BinarySearch(recorded, name); !found {
				t.Errorf("missing dependency package directory: %s", name)
			}
		}
		for _, name := range recorded {
			if _, found := slices.BinarySearch(packages, name); !found {
				t.Errorf("stale dependency package directory: %s", name)
			}
		}
		t.Fatal("update dependency_packages.txt after qualifying template/dependency changes")
	}
}

func dependencyPackageDirectories(modulePath string, source fs.FS) ([]string, error) {
	directories := map[string]bool{}
	err := fs.WalkDir(source, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name != "." {
				if entry.Name() == "vendor" || entry.Name() == ".git" {
					return fs.SkipDir
				}
				if info, err := fs.Stat(source, path.Join(name, "go.mod")); err == nil && !info.IsDir() {
					return fs.SkipDir
				} else if err != nil && !os.IsNotExist(err) {
					return err
				}
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		info, err := fs.Stat(source, name)
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			directories[path.Join(modulePath, path.Dir(name))] = true
		}
		return nil
	})
	var result []string
	for directory := range directories {
		result = append(result, directory)
	}
	slices.Sort(result)
	return result, err
}

func TestDependencyPackageDirectories(t *testing.T) {
	source := fstest.MapFS{
		"go.mod":                           {Data: []byte("module example.com/dependency")},
		"root.go":                          {},
		"platform/source_windows.go":       {},
		"onlytests/only_test.go":           {},
		"ignored/source.go":                {Data: []byte("//go:build ignore\n\npackage ignored\n")},
		"testdata/example/main.go":         {},
		"nested/go.mod":                    {Data: []byte("module example.com/nested")},
		"nested/root.go":                   {},
		"nested/child/source.go":           {},
		"notnested/go.mod/source.go":       {},
		"notnested/source.go":              {},
		"vendor/example.com/lib/source.go": {},
		".git/ignored.go":                  {},
		"assets/not_go.txt":                {},
	}
	found, err := dependencyPackageDirectories("example.com/dependency", source)
	want := []string{"example.com/dependency", "example.com/dependency/ignored",
		"example.com/dependency/notnested", "example.com/dependency/notnested/go.mod",
		"example.com/dependency/onlytests", "example.com/dependency/platform",
		"example.com/dependency/testdata/example"}
	if err != nil || !slices.Equal(found, want) {
		t.Fatal("source directory selection differs", found, err)
	}
}

func TestGeneratedSubpackageIdentities(t *testing.T) {
	original := dependencyPackages
	t.Cleanup(func() { dependencyPackages = original })
	for _, suffix := range []string{"", "/internal/bootstrap", "/internal/setting"} {
		dependencyPackages = "\nexample.com/app" + suffix + "\n"
		if !shadowsDependency("example.com/app") {
			t.Fatal("generated package collision was missed", suffix)
		}
		for _, modulePath := range []string{"example.com/app-sibling", "example.com/app/child"} {
			if shadowsDependency(modulePath) {
				t.Fatal("shared prefix was refused", modulePath, suffix)
			}
		}
	}
}
