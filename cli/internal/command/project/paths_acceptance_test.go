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
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"golang.org/x/mod/modfile"
)

func TestActualNativePathJourney(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("native pathname consumption acceptance currently targets Linux/amd64")
	}
	root, scratch := sourceRoot(t), t.TempDir()
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	env := []string{"GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS="}
	generator := filepath.Join(scratch, "fathomry")
	runTool(t, root, goBinary, env, "build", "-o", generator, "./cmd/fathomry")
	logical, physical := filepath.Join(scratch, "logical"), filepath.Join(scratch, "physical")
	for _, directory := range []string{logical, physical, filepath.Join(physical, "nested")} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range map[string]string{
		"link":            filepath.Join(physical, "nested"),
		"framework-child": filepath.Join(root, "cli"),
	} {
		if err := os.Symlink(target, filepath.Join(logical, name)); err != nil {
			t.Fatal(err)
		}
	}
	expectedSource, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, cwd, directory, source string
	}{
		{"operands", scratch, "logical/link/../operands", "logical/framework-child/.."},
		{"cwd", filepath.Join(logical, "link"), "../cwd", "../../logical/framework-child/.."},
	} {
		t.Run(test.name, func(t *testing.T) {
			noTools := []string{"PATH=" + filepath.Join(scratch, "no-tools"), "GOPROXY=off", "PWD=" + test.cwd}
			runTool(t, test.cwd, generator, noTools, arguments(test.directory, test.source, "example.org/"+test.name)...)
			target := filepath.Join(physical, test.name)
			files := generated(t, target)
			absent(t, filepath.Join(logical, test.name))
			metadata, err := modfile.Parse("go.mod", []byte(files["go.mod"]), nil)
			if err != nil || len(metadata.Replace) != 1 || len(metadata.Require) != 1 {
				t.Fatalf("bad initial module: %v", err)
			}
			binding, err := filepath.EvalSymlinks(filepath.Join(target, metadata.Replace[0].New.Path))
			if err != nil || binding != expectedSource {
				t.Fatalf("selected source=%q, want=%q: %v", binding, expectedSource, err)
			}
			t.Logf("native target=%s; replacement=%s", target, metadata.Replace[0].New.Path)
			runTool(t, scratch, goBinary, env, "-C", target, "mod", "tidy")
			if output := runTool(t, scratch, goBinary, env, "-C", target, "mod", "tidy", "-diff"); len(output) != 0 {
				t.Fatalf("unstable tidy: %s", output)
			}
			runTool(t, scratch, goBinary, env, "-C", target, "build", "-mod=readonly", "-o", "./bin/app", ".")
			for _, output := range [][]byte{
				runTool(t, scratch, filepath.Join(target, "bin", "app"), noTools, "--help"),
				runTool(t, scratch, goBinary, env, "-C", target, "run", "-mod=readonly", ".", "--help"),
			} {
				if !bytes.Contains(output, []byte("Usage: fathomry")) {
					t.Fatalf("generated CLI failed: %s", output)
				}
			}
			data, err := os.ReadFile(filepath.Join(target, "go.mod"))
			if err != nil {
				t.Fatal(err)
			}
			hydrated, err := modfile.Parse("go.mod", data, nil)
			if err != nil || len(hydrated.Replace) != 1 || hydrated.Replace[0].Old != metadata.Replace[0].Old ||
				hydrated.Replace[0].New != metadata.Replace[0].New {
				t.Fatalf("hydration changed the selected source: %s %v", data, err)
			}
		})
	}
	t.Run("existing-native-target", func(t *testing.T) {
		existing := filepath.Join(physical, "existing")
		if err := os.WriteFile(existing, []byte("preserve"), 0o600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, generator, arguments(logical+"/link/../existing", root, "example.org/existing")...)
		output, err := command.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 {
			t.Fatalf("existing native target accepted: %v %s", err, output)
		}
		data, err := os.ReadFile(existing)
		if err != nil || string(data) != "preserve" {
			t.Fatalf("existing target changed: %q %v", data, err)
		}
		absent(t, filepath.Join(logical, "existing"))
	})
}
