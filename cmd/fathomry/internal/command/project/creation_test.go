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

package project

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

func optionsFor(t *testing.T) Options {
	t.Helper()
	return Options{Directory: filepath.Join(t.TempDir(), "sample"), Name: "sample",
		Module: "example.org/business", FrameworkVersion: "v0.0.0-gh83", Provider: "viper"}
}
func put(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCreationProducesOnlyImplementedFunctionalContent(t *testing.T) {
	for _, provider := range []string{"viper", "nacos"} {
		t.Run(provider, func(t *testing.T) {
			options := optionsFor(t)
			options.Provider = provider
			if provider == "nacos" {
				options.Configuration = "remote"
			}
			if _, err := renderProject(options); err != nil {
				t.Fatalf("template rendering: %v", err)
			}
			result, err := Create(context.Background(), options)
			if err != nil || !result.Created || !result.Complete {
				t.Fatalf("creation: %v", err)
			}
			count := 0
			err = filepath.WalkDir(options.Directory, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				count++
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if strings.HasSuffix(path, ".go") {
					if _, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors); err != nil {
						return err
					}
					if strings.Contains(string(data), FrameworkModule+"/internal/") ||
						strings.Contains(string(data), FrameworkModule+"/cmd/") ||
						strings.Contains(string(data), FrameworkModule+"/framework/project") {
						t.Error("generated source depends on private/tooling packages")
					}
				}
				return nil
			})
			if err != nil || count != result.FilesWritten {
				t.Fatal("file evidence mismatch", err, count, result.FilesWritten)
			}
			for _, path := range []string{"internal/project", "workflows", "nodes", "runs", "go.work", "go.sum", "third_party"} {
				if _, err := os.Stat(filepath.Join(options.Directory, path)); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("unexpected generated path %s", path)
				}
			}
			data, err := os.ReadFile(filepath.Join(options.Directory, "go.mod"))
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := modfile.Parse("go.mod", data, nil)
			if err != nil || parsed.Module.Mod.Path != options.Module || len(parsed.Require) != 1 ||
				parsed.Require[0].Mod.Path != FrameworkModule || parsed.Require[0].Mod.Version != options.FrameworkVersion ||
				len(parsed.Replace) != 0 {
				t.Fatal("wrong independent module", err)
			}
			if _, err := os.Stat(filepath.Join(options.Directory, "internal/configuration/configuration.go")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInvalidOptionsAcquireNothing(t *testing.T) {
	mutations := []func(*Options){
		func(o *Options) { o.Name = "../escape" }, func(o *Options) { o.Name = "CON" }, func(o *Options) { o.Name = "con" },
		func(o *Options) { o.Name = "" }, func(o *Options) { o.Name = "bad.name" }, func(o *Options) { o.Module = "bad\npath" },
		func(o *Options) { o.Module = FrameworkModule }, func(o *Options) { o.Module = FrameworkModule + "/child" },
		func(o *Options) { o.Module = "" }, func(o *Options) { o.FrameworkVersion = "latest" },
		func(o *Options) { o.FrameworkVersion = "v2.0.0" }, func(o *Options) { o.FrameworkVersion = "v1" },
		func(o *Options) { o.FrameworkVersion = "v1.0.0+private" }, func(o *Options) { o.Provider = "other" },
		func(o *Options) { o.Directory = "relative" }, func(o *Options) { o.Directory += "/." },
		func(o *Options) { o.Bundle = "relative" }, func(o *Options) { o.Name = strings.Repeat("a", 65) },
	}
	for index, mutate := range mutations {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			options := optionsFor(t)
			target := options.Directory
			mutate(&options)
			result, err := Create(context.Background(), options)
			if !errors.Is(err, InvalidInput) || result.Created {
				t.Fatalf("invalid declaration accepted: %v", err)
			}
			if _, err := os.Stat(target); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("target acquired on invalid input")
			}
		})
	}
	if _, err := Create(nil, optionsFor(t)); !errors.Is(err, InvalidInput) {
		t.Fatal("nil context accepted")
	}
}

func TestExistingTargetAndSymlinkAreNotOverwritten(t *testing.T) {
	for _, kind := range []string{"directory", "file", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			options := optionsFor(t)
			original := filepath.Join(t.TempDir(), "canary")
			put(t, original, []byte("preserve"))
			switch kind {
			case "directory":
				if err := os.Mkdir(options.Directory, 0700); err != nil {
					t.Fatal(err)
				}
			case "file":
				put(t, options.Directory, []byte("preserve"))
			case "symlink":
				if err := os.Symlink(filepath.Dir(original), options.Directory); err != nil {
					t.Skip("symlink fixture unavailable")
				}
			}
			result, err := Create(context.Background(), options)
			if !errors.Is(err, Exists) || result.Created {
				t.Fatal("existing destination accepted", err)
			}
			data, err := os.ReadFile(original)
			if err != nil || string(data) != "preserve" {
				t.Fatal("outside canary changed")
			}
		})
	}
}

type cancelOnFile struct {
	context.Context
	target    string
	cancel    context.CancelCauseFunc
	cause     error
	triggered bool
}

func (ctx *cancelOnFile) Err() error {
	if !ctx.triggered {
		if _, err := os.Stat(filepath.Join(ctx.target, ".gitignore")); err == nil {
			ctx.triggered = true
			if err := os.WriteFile(filepath.Join(ctx.target, "unrelated.txt"), []byte("preserve"), 0600); err != nil {
				panic(err)
			}
			ctx.cancel(ctx.cause)
		}
	}
	return ctx.Context.Err()
}
func TestCancellationRetainsPartialResponsibilityAndForeignFile(t *testing.T) {
	options := optionsFor(t)
	parent, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("caller stop")
	ctx := &cancelOnFile{Context: parent, target: options.Directory, cancel: cancel, cause: cause}
	result, err := Create(ctx, options)
	if !errors.Is(err, Incomplete) || !errors.Is(err, Cancelled) || !errors.Is(err, cause) ||
		!result.Created || result.Complete || result.FilesWritten < 1 || !ctx.triggered {
		t.Fatal("partial evidence lost", err, result.FilesWritten)
	}
	written := 0
	if err := filepath.WalkDir(options.Directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && entry.Name() != "unrelated.txt" {
			written++
		}
		return nil
	}); err != nil || written != result.FilesWritten {
		t.Fatal("partial file count is not observed", written, result.FilesWritten, err)
	}
	data, err := os.ReadFile(filepath.Join(options.Directory, "unrelated.txt"))
	if err != nil || string(data) != "preserve" {
		t.Fatal("unrelated file deleted")
	}
	parent, cancel = context.WithCancelCause(context.Background())
	cancel(cause)
	result, err = Create(parent, optionsFor(t))
	if !errors.Is(err, cause) || result.Created {
		t.Fatal("pre-cancelled creation acquired a target")
	}
}

func TestConcurrentCreationHasOneOwner(t *testing.T) {
	options := optionsFor(t)
	var group sync.WaitGroup
	results := make(chan bool, 8)
	for range 8 {
		group.Go(func() {
			result, err := Create(context.Background(), options)
			if err == nil {
				results <- result.Complete
				return
			}
			if !errors.Is(err, Exists) {
				t.Error("unexpected competing creation error", err)
			}
			results <- false
		})
	}
	group.Wait()
	close(results)
	successes := 0
	for result := range results {
		if result {
			successes++
		}
	}
	if successes != 1 {
		t.Fatal("creation did not establish one owner", successes)
	}
}

func TestRootedWritesPreserveConflictingAndExternalFiles(t *testing.T) {
	directory := t.TempDir()
	put(t, filepath.Join(directory, "existing"), []byte("preserve"))
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := writeFile(context.Background(), root, "existing", []byte("new")); !errors.Is(err, Unavailable) {
		t.Fatal("file overwritten", err)
	}
	outside := t.TempDir()
	put(t, filepath.Join(outside, "canary"), []byte("preserve"))
	if err := os.Symlink(outside, filepath.Join(directory, "escape")); err != nil {
		t.Skip("symlink fixture unavailable")
	}
	if err := writeFile(context.Background(), root, "escape/canary", []byte("new")); err == nil {
		t.Fatal("root escape accepted")
	}
	for _, path := range []string{filepath.Join(directory, "existing"), filepath.Join(outside, "canary")} {
		data, err := os.ReadFile(path)
		if err != nil || string(data) != "preserve" {
			t.Fatal("canary changed")
		}
	}
}

func TestRuntimeDeclarationDiagnosticsAreRestricted(t *testing.T) {
	options := optionsFor(t)
	options.Bundle = "private-canary"
	for _, value := range []any{options, &options, Result{Directory: "private-canary"}} {
		if strings.Contains(fmt.Sprintf("%#v", value), "private-canary") {
			t.Fatal("private input disclosed")
		}
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime JSON accepted")
		}
	}
}

func FuzzDeclarations(f *testing.F) {
	f.Add("sample", "example.org/business", "v1.0.0")
	f.Fuzz(func(t *testing.T, projectName, modulePath, version string) {
		if len(projectName)+len(modulePath)+len(version) > 4096 {
			t.Skip()
		}
		if !name(projectName) || !frameworkVersion(version) || module.CheckPath(modulePath) != nil {
			return
		}
		options := Options{Name: projectName, Module: modulePath, FrameworkVersion: version, Provider: "viper"}
		if len(modulePath) > 256 {
			return
		}
		if _, err := renderProject(options); err != nil {
			t.Fatal("valid declarations broke embedded templates", err)
		}
	})
}
