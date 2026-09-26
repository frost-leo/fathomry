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

package project

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"golang.org/x/mod/modfile"
)

func traversalFixture(t *testing.T) (root, logical, physical string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("native symlink traversal acceptance is initially Linux")
	}
	root = t.TempDir()
	logical, physical = filepath.Join(root, "logical"), filepath.Join(root, "physical")
	mustMkdir(t, logical)
	mustMkdir(t, physical)
	mustMkdir(t, filepath.Join(physical, "nested"))
	mustLink(t, filepath.Join(physical, "nested"), filepath.Join(logical, "link"))
	return root, logical, physical
}

func assertProjectSource(t *testing.T, target, source string) {
	t.Helper()
	files := readFiles(t, target)
	if len(files) != 4 {
		t.Fatalf("expected four generated files, got %v", files)
	}
	metadata, err := modfile.Parse("go.mod", []byte(files["go.mod"]), nil)
	if err != nil || len(metadata.Replace) != 1 {
		t.Fatalf("invalid generated module: %v", err)
	}
	binding, err := filepath.EvalSymlinks(filepath.Join(target, filepath.FromSlash(metadata.Replace[0].New.Path)))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := filepath.EvalSymlinks(source)
	if err != nil || binding != expected {
		t.Fatalf("source binding=%q, expected=%q: %v", binding, expected, err)
	}
}

func sourceProfile(t *testing.T, directory string) {
	t.Helper()
	mustWrite(t, filepath.Join(directory, "go.mod"), "module "+frameworkModule+"\ngo 1.27.0\n")
	mustMkdir(t, filepath.Join(directory, "cli"))
}

func TestNativeDestinationTraversal(t *testing.T) {
	for _, spelling := range []string{"absolute", "relative", "trailing-separator"} {
		t.Run(spelling, func(t *testing.T) {
			source := repository(t)
			root, logical, physical := traversalFixture(t)
			target := logical + "/link/../app"
			if spelling == "relative" {
				t.Chdir(root)
				target = "logical/link/../app"
			} else if spelling == "trailing-separator" {
				target += "/"
			}
			effect, err := create(context.Background(), request{target, "example.org/path", source})
			if effect != complete || err != nil {
				t.Fatalf("create: %v %v", effect, err)
			}
			assertProjectSource(t, filepath.Join(physical, "app"), source)
			assertAbsent(t, filepath.Join(logical, "app"))
		})
	}
}

func TestNativeExistingTargetTraversal(t *testing.T) {
	for _, kind := range []string{"file", "empty", "nonempty", "dangling"} {
		t.Run(kind, func(t *testing.T) {
			source := repository(t)
			_, logical, physical := traversalFixture(t)
			actual := filepath.Join(physical, "existing")
			switch kind {
			case "file":
				mustWrite(t, actual, "preserve")
			case "empty", "nonempty":
				mustMkdir(t, actual)
				if kind == "nonempty" {
					mustWrite(t, filepath.Join(actual, "keep"), "preserve")
				}
			case "dangling":
				mustLink(t, filepath.Join(physical, "missing"), actual)
			}
			before, err := os.Lstat(actual)
			if err != nil {
				t.Fatal(err)
			}
			effect, err := create(context.Background(), request{logical + "/link/../existing", "example.org/path", source})
			if effect != untouched || !errors.Is(err, os.ErrExist) {
				t.Fatalf("existing native target accepted: %v %v", effect, err)
			}
			after, err := os.Lstat(actual)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("existing entry replaced: %v", err)
			}
			if kind == "file" {
				data, err := os.ReadFile(actual)
				if err != nil || string(data) != "preserve" {
					t.Fatalf("existing file changed: %q %v", data, err)
				}
			} else if kind == "empty" || kind == "nonempty" {
				files := readFiles(t, actual)
				if kind == "empty" && len(files) != 0 || kind == "nonempty" && (len(files) != 1 || files["keep"] != "preserve") {
					t.Fatalf("existing directory changed: %v", files)
				}
			}
			assertAbsent(t, filepath.Join(logical, "existing"))
		})
	}
}

func TestNativeSourceTraversal(t *testing.T) {
	for _, profile := range []string{"valid", "invalid", "missing"} {
		t.Run(profile, func(t *testing.T) {
			root, logical, physical := traversalFixture(t)
			sourceProfile(t, logical)
			if profile == "valid" {
				sourceProfile(t, physical)
				mustWrite(t, filepath.Join(logical, "go.mod"), "module example.org/not-selected\ngo 1.27.0\n")
			} else if profile == "invalid" {
				mustWrite(t, filepath.Join(physical, "go.mod"), "module example.org/wrong-source\ngo 1.27.0\n")
			}
			target := filepath.Join(root, "app")
			effect, err := create(context.Background(), request{target, "example.org/path", logical + "/link/.."})
			if profile == "valid" {
				if effect != complete || err != nil {
					t.Fatalf("valid native source refused: %v %v", effect, err)
				}
				assertProjectSource(t, target, physical)
			} else {
				if effect != untouched || err == nil {
					t.Fatalf("wrong native source accepted: %v %v", effect, err)
				}
				assertAbsent(t, target)
			}
		})
	}
}

func TestNativeSourceOverlapTraversal(t *testing.T) {
	_, logical, physical := traversalFixture(t)
	sourceProfile(t, physical)
	effect, err := create(context.Background(), request{logical + "/link/../app", "example.org/path", physical})
	if effect != untouched || !errors.Is(err, errOverlap) {
		t.Fatalf("native source overlap accepted: %v %v", effect, err)
	}
	assertAbsent(t, filepath.Join(physical, "app"))
	assertAbsent(t, filepath.Join(logical, "app"))
}

func TestNativeTraversalRequiresExistingDirectories(t *testing.T) {
	for _, operand := range []string{"source", "destination"} {
		for _, kind := range []string{"missing", "file", "dangling"} {
			t.Run(operand+"/"+kind, func(t *testing.T) {
				root, logical, _ := traversalFixture(t)
				sourceProfile(t, logical)
				component := filepath.Join(logical, "component")
				if kind == "file" {
					mustWrite(t, component, "not a directory")
				} else if kind == "dangling" {
					mustLink(t, filepath.Join(logical, "missing"), component)
				}
				input := request{filepath.Join(root, "app"), "example.org/path", logical}
				wrongDestination := input.directory
				if operand == "source" {
					input.source = component + "/.."
				} else {
					input.source = repository(t)
					input.directory = component + "/../app"
					wrongDestination = filepath.Join(logical, "app")
				}
				effect, err := create(context.Background(), input)
				if effect != untouched || err == nil {
					t.Fatalf("invalid intermediate component accepted: %v %v", effect, err)
				}
				assertAbsent(t, wrongDestination)
			})
		}
	}
}

func TestPhysicalInvocationDirectory(t *testing.T) {
	for _, sourceSpelling := range []string{"absolute", "relative"} {
		t.Run(sourceSpelling, func(t *testing.T) {
			source := repository(t)
			_, logical, physical := traversalFixture(t)
			mustLink(t, source, filepath.Join(physical, "framework"))
			t.Chdir(filepath.Join(logical, "link"))
			t.Setenv("PWD", filepath.Join(logical, "link"))
			input := request{"../app", "example.org/path", source}
			if sourceSpelling == "relative" {
				input.source = "../framework"
			}
			effect, err := create(context.Background(), input)
			if effect != complete || err != nil {
				t.Fatalf("physical CWD not respected: %v %v", effect, err)
			}
			assertProjectSource(t, filepath.Join(physical, "app"), source)
			assertAbsent(t, filepath.Join(logical, "app"))
		})
	}
}
