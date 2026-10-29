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

package configuration

import (
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

func TestIndependentConsumer(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	fixture := newPeer(t)
	fixture.mu.Lock()
	fixture.value = "application:\n workers: 2\n"
	fixture.mu.Unlock()
	directory := t.TempDir()
	source := os.DirFS("testdata/consumer")
	if err := fs.WalkDir(source, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		target := filepath.Join(directory, path)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		content, err := fs.ReadFile(source, path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, content, 0600)
	}); err != nil {
		t.Fatal(err)
	}
	module := fmt.Sprintf("module example.org/configuration-consumer\n\ngo 1.27.0\n\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n\nrequire (\n github.com/BurntSushi/toml v1.6.0\n gopkg.in/yaml.v2 v2.4.0\n)\n", filepath.ToSlash(root))
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	run := func(offline bool, args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin/go"), args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off",
			"FATHOMRY_TEST_NACOS_HTTP="+fixture.http.URL+"/nacos", "FATHOMRY_TEST_NACOS_GRPC="+fixture.address)
		if offline {
			command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
		}
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("independent consumer %v: %v\n%s", args, err, output)
		}
		return output
	}
	run(false, "mod", "tidy")
	run(true, "mod", "tidy", "-diff")
	run(true, "vet", "-mod=readonly", "./...")
	run(true, "test", "-mod=readonly", "-race", "-count=1", "./...")
	output := run(true, "list", "-f", "{{join .Imports \"\\n\"}}", "github.com/frost-leo/fathomry/framework/v1", "github.com/frost-leo/fathomry/framework/configuration/v1")
	if strings.Contains(string(output), "fathomry/internal/") || strings.Contains(string(output), "nacos-group/") {
		t.Fatal("public implementation bypassed public Adapters")
	}
	core := run(true, "list", "-deps", "github.com/frost-leo/fathomry/framework/v1")
	if strings.Contains(string(core), "fathomry/adapters/") || strings.Contains(string(core), "fathomry/framework/configuration/") {
		t.Fatal("shared metadata pulled in providers")
	}
}
