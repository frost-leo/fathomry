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

package failure_test

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// This is an actual file-proxy module artifact, not a workspace or module replace.
// Nested SDK modules are excluded as they are from ordinary Go module archives.
func TestIndependentModuleArtifact(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	proxy := filepath.Join(directory, "proxy")
	version := "v0.0.0-gh71"
	module := "github.com/frost-leo/fathomry"
	cache := filepath.Join(directory, "modules")
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	files := exec.CommandContext(ctx, "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	files.Dir = root
	tracked, err := files.Output()
	if err != nil {
		t.Fatal("module artifact file enumeration failed", err)
	}
	paths := strings.Split(strings.TrimSuffix(string(tracked), "\x00"), "\x00")
	slices.Sort(paths)
	var submodules []string
	for _, path := range paths {
		if path != "go.mod" && strings.HasSuffix(path, "/go.mod") {
			submodules = append(submodules, strings.TrimSuffix(path, "go.mod"))
		}
	}
	var artifact bytes.Buffer
	archive := zip.NewWriter(&artifact)
	for _, path := range paths {
		if slices.ContainsFunc(submodules, func(prefix string) bool { return strings.HasPrefix(path, prefix) }) {
			continue
		}
		info, err := os.Lstat(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		content, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		entry, err := archive.Create(module + "@" + version + "/" + filepath.ToSlash(path))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	versions := filepath.Join(proxy, module, "@v")
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(versions, version+".mod"), mod)
	write(filepath.Join(versions, version+".zip"), artifact.Bytes())
	write(filepath.Join(versions, version+".info"), []byte(`{"Version":"`+version+`","Time":"2026-09-18T00:00:00Z"}`))
	write(filepath.Join(versions, "list"), []byte(version+"\n"))

	consumer := filepath.Join(directory, "consumer")
	write(filepath.Join(consumer, "go.mod"), []byte("module example.org/consumer\n\ngo 1.27.0\nrequire "+module+" "+version+"\n"))
	for _, pair := range [][2]string{
		{"failure/extension_test.go", "consumer_test.go"},
		{"failure/testdata/configcheck/errors.go", "configcheck/errors.go"},
		{"failure/testdata/orders/errors.go", "orders/errors.go"},
	} {
		content, err := os.ReadFile(filepath.Join(root, pair[0]))
		if err != nil {
			t.Fatal(err)
		}
		content = bytes.ReplaceAll(content, []byte(module+"/failure/testdata/"), []byte("example.org/consumer/"))
		write(filepath.Join(consumer, pair[1]), content)
	}
	run := func(args ...string) string {
		t.Helper()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = consumer
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=-modcacherw",
			"GOPROXY=file://"+filepath.ToSlash(proxy), "GOSUMDB=off", "GOPRIVATE=", "GONOPROXY=none",
			"GOMODCACHE="+cache)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("independent module %v failed: %v\n%s", args, err, output)
		}
		return string(output)
	}
	t.Log(run("test", "-mod=mod", "-race", "-count=1", "-timeout=1m", "./..."))
	run("build", "-mod=readonly", "./...")
	graph := run("list", "-mod=readonly", "-deps", "-test", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./...")
	found := false
	for _, path := range strings.Split(graph, "\n") {
		if path == module+"/failure" {
			found = true
			continue
		}
		if path == "" || strings.HasPrefix(path, "example.org/consumer") {
			continue
		}
		t.Fatalf("unexpected non-standard dependency in independent consumer: %s", path)
	}
	if !found {
		t.Fatal("consumer never imported the public contract")
	}
	selected := run("list", "-m", "-mod=readonly", "-f", "{{.Version}} {{if .Replace}}replaced{{end}}", module)
	if strings.TrimSpace(selected) != version {
		t.Fatal("consumer did not use the module artifact directly")
	}
	consumerMod, err := os.ReadFile(filepath.Join(consumer, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(consumerMod, []byte("replace ")) {
		t.Fatal("consumer required SDK replacements")
	}
	t.Logf("GOWORK=off; artifact %s; only standard-library + failure dependencies", version)
}
