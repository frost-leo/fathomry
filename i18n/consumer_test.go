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

package i18n_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestIndependentModuleArtifact(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	proxy := filepath.Join(directory, "proxy")
	cache := filepath.Join(directory, "modules")
	const module = "github.com/frost-leo/fathomry"
	const version = "v0.0.0-gh73"
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	read := func(path string) []byte {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	command := exec.CommandContext(ctx, "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	paths := strings.Split(strings.TrimSuffix(string(output), "\x00"), "\x00")
	slices.Sort(paths)
	var nested []string
	for _, path := range paths {
		if path != "go.mod" && strings.HasSuffix(path, "/go.mod") {
			nested = append(nested, strings.TrimSuffix(path, "go.mod"))
		}
	}
	var artifact bytes.Buffer
	archive := zip.NewWriter(&artifact)
	for _, path := range paths {
		if slices.ContainsFunc(nested, func(prefix string) bool { return strings.HasPrefix(path, prefix) }) {
			continue
		}
		info, err := os.Lstat(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		entry, err := archive.Create(module + "@" + version + "/" + filepath.ToSlash(path))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(read(filepath.Join(root, path))); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	versions := filepath.Join(proxy, module, "@v")
	write(filepath.Join(versions, version+".mod"), read(filepath.Join(root, "go.mod")))
	write(filepath.Join(versions, version+".zip"), artifact.Bytes())
	write(filepath.Join(versions, version+".info"), []byte(`{"Version":"`+version+`","Time":"2026-09-18T00:00:00Z"}`))
	write(filepath.Join(versions, "list"), []byte(version+"\n"))
	parent := exec.CommandContext(ctx, goBinary, "env", "GOMODCACHE")
	parent.Dir = root
	output, err = parent.Output()
	if err != nil {
		t.Fatal(err)
	}
	parentCache := strings.TrimSpace(string(output))
	parent = exec.CommandContext(ctx, goBinary, "list", "-m", "-json", "github.com/nicksnyder/go-i18n/v2", "golang.org/x/text")
	parent.Dir = root
	output, err = parent.Output()
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	for decoder.More() {
		var dependency struct {
			Path, Version string
			Replace       any
		}
		if err := decoder.Decode(&dependency); err != nil {
			t.Fatal(err)
		}
		if dependency.Replace != nil {
			t.Fatal("unexpected localization dependency replacement")
		}
		source := filepath.Join(parentCache, "cache", "download", dependency.Path, "@v")
		target := filepath.Join(proxy, dependency.Path, "@v")
		for _, extension := range []string{".mod", ".info", ".zip"} {
			write(filepath.Join(target, dependency.Version+extension), read(filepath.Join(source, dependency.Version+extension)))
		}
		write(filepath.Join(target, "list"), []byte(dependency.Version+"\n"))
	}
	consumer := filepath.Join(directory, "consumer")
	write(filepath.Join(consumer, "go.mod"), []byte("module example.org/business\n\ngo 1.27.0\n\nrequire "+module+" "+version+"\n"))
	write(filepath.Join(consumer, "go.sum"), read(filepath.Join(root, "go.sum")))
	entries, err := os.ReadDir(filepath.Join(root, "i18n/testdata/consumer"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			write(filepath.Join(consumer, entry.Name()), read(filepath.Join(root, "i18n/testdata/consumer", entry.Name())))
		}
	}
	run := func(executable string, args ...string) string {
		t.Helper()
		command := exec.CommandContext(ctx, executable, args...)
		command.Dir = consumer
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=-modcacherw",
			"GOPROXY=file://"+filepath.ToSlash(proxy), "GOSUMDB=off", "GOPRIVATE=", "GONOPROXY=none", "GOMODCACHE="+cache)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("independent module %v failed: %v\n%s", args, err, output)
		}
		return string(output)
	}
	t.Log(run(goBinary, "test", "-mod=mod", "-race", "-count=1", "-timeout=1m", "./..."))
	binary := filepath.Join(consumer, "consumer")
	run(goBinary, "build", "-race", "-mod=readonly", "-o", binary, ".")
	t.Log(run(binary))
	graph := run(goBinary, "list", "-mod=readonly", "-deps", "-test", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./...")
	found := false
	for _, path := range strings.Split(graph, "\n") {
		if path == module+"/i18n" {
			found = true
		}
		if path == "" || path == module+"/i18n" || path == module+"/failure" ||
			strings.HasPrefix(path, "example.org/business") ||
			strings.HasPrefix(path, "github.com/nicksnyder/go-i18n/v2/") ||
			strings.HasPrefix(path, "golang.org/x/text/") {
			continue
		}
		t.Fatalf("unexpected consumer dependency: %s", path)
	}
	if !found {
		t.Fatal("consumer never imported public i18n")
	}
	selected := run(goBinary, "list", "-m", "-mod=readonly", "-f", "{{.Version}} {{if .Replace}}replaced{{end}}", module)
	if strings.TrimSpace(selected) != version || bytes.Contains(read(filepath.Join(consumer, "go.mod")), []byte("replace ")) {
		t.Fatal("consumer did not use an unreplaced module artifact")
	}
	failureGraph := run(goBinary, "list", "-mod=readonly", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", module+"/failure")
	if strings.TrimSpace(failureGraph) != module+"/failure" {
		t.Fatal("failure gained a localization dependency")
	}
}
