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

package configuration_test

import (
	"archive/zip"
	"bytes"
	"context"
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
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	proxy := filepath.Join(directory, "proxy")
	cache := filepath.Join(directory, "modules")
	const module = "github.com/frost-leo/fathomry"
	const version = "v0.0.0-gh79"
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
	write(filepath.Join(versions, version+".info"), []byte(`{"Version":"`+version+`","Time":"2026-09-19T00:00:00Z"}`))
	write(filepath.Join(versions, "list"), []byte(version+"\n"))
	parent := exec.CommandContext(ctx, goBinary, "env", "GOMODCACHE")
	parent.Dir = root
	output, err = parent.Output()
	if err != nil {
		t.Fatal(err)
	}
	parentCache := strings.TrimSpace(string(output))
	consumer := filepath.Join(directory, "consumer")
	write(filepath.Join(consumer, "go.mod"), []byte("module example.org/business\n\ngo 1.27.0\n\nrequire "+module+" "+version+"\n"))
	write(filepath.Join(consumer, "go.sum"), read(filepath.Join(root, "go.sum")))
	entries, err := os.ReadDir(filepath.Join(root, "framework/configuration/testdata/project"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			write(filepath.Join(consumer, entry.Name()), read(filepath.Join(root, "framework/configuration/testdata/project", entry.Name())))
		}
	}
	run := func(executable string, args ...string) string {
		t.Helper()
		command := exec.CommandContext(ctx, executable, args...)
		command.Dir = consumer
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=-modcacherw",
			"GOPROXY=file://"+filepath.ToSlash(proxy)+",file://"+filepath.ToSlash(filepath.Join(parentCache, "cache", "download")), "GOSUMDB=off", "GOPRIVATE=", "GONOPROXY=none", "GOMODCACHE="+cache)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("independent module %v failed: %v\n%s", args, err, output)
		}
		return string(output)
	}
	t.Log(run(goBinary, "test", "-mod=mod", "-race", "-count=1", "-timeout=1m", "./..."))
	binary := filepath.Join(consumer, "consumer")
	run(goBinary, "build", "-race", "-mod=readonly", "-o", binary, ".")
	configurationRoot := filepath.Join(directory, "settings")
	write(filepath.Join(configurationRoot, "settings.yaml"), []byte("label: example\n"))
	if output := run(binary, configurationRoot); output != "configuration loaded: provider=local schema=1\n" {
		t.Fatalf("unexpected standalone result: %q", output)
	}
	graph := run(goBinary, "list", "-mod=readonly", "-deps", "-f", "{{.ImportPath}}", "./...")
	for _, required := range []string{module + "/framework/configuration", module + "/adapters/configuration/local", "github.com/spf13/viper"} {
		if !slices.Contains(strings.Fields(graph), required) {
			t.Fatalf("independent consumer did not exercise %s", required)
		}
	}
	for _, name := range strings.Fields(graph) {
		for _, banned := range []string{module + "/internal/database/", module + "/internal/orchestration/", module + "/internal/configsource/nacos/", module + "/cmd/", "go.temporal.io/"} {
			if strings.HasPrefix(name, banned) {
				t.Fatalf("unrelated capability linked: %s", name)
			}
		}
	}
	contractGraph := run(goBinary, "list", "-deps", "-f", "{{.ImportPath}}", module+"/framework/configuration")
	for _, name := range strings.Fields(contractGraph) {
		if strings.HasPrefix(name, module+"/adapters/") || strings.HasPrefix(name, module+"/internal/configsource/") || name == "github.com/spf13/viper" {
			t.Fatalf("contract imports its Provider: %s", name)
		}
	}
	selected := run(goBinary, "list", "-m", "-mod=readonly", "-f", "{{.Version}} {{if .Replace}}replaced{{end}}", module)
	if strings.TrimSpace(selected) != version || bytes.Contains(read(filepath.Join(consumer, "go.mod")), []byte("replace ")) {
		t.Fatal("consumer did not use an unreplaced module artifact")
	}
	write(filepath.Join(consumer, "forbidden.go"), []byte("package main\nimport _ \""+module+"/internal/resource\"\n"))
	command = exec.CommandContext(ctx, goBinary, "build", "-mod=readonly", ".")
	command.Dir = consumer
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off", "GOMODCACHE="+cache)
	refused, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(refused), "use of internal package "+module+"/internal/resource not allowed") {
		t.Fatalf("wrong private-boundary rejection: %v\n%s", err, refused)
	}
	t.Log("Independent consumer: unreplaced module artifact, empty private module cache, file-only dependency proxies, direct internal import refused")
}
