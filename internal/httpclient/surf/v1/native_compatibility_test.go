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

package surf

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type selectedModule struct {
	Path, Version string
	Main          bool
}

func nativeModules(t *testing.T, root string) []selectedModule {
	t.Helper()
	command := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "list", "-mod=readonly", "-deps", "-test",
		"-json=Module", "./internal/httpclient/surf/v1")
	command.Dir = root
	command.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("consuming dependency graph unavailable: %v\n%s", err, output)
	}
	var result []selectedModule
	seen := make(map[string]bool)
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var value struct{ Module *selectedModule }
		if err := decoder.Decode(&value); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		if value.Module != nil && !seen[value.Module.Path] {
			seen[value.Module.Path] = true
			result = append(result, *value.Module)
		}
	}
	return result
}
func nativeModuleFile(t *testing.T, root, nativeRoot, module string, selection []selectedModule) (string, []string) {
	t.Helper()
	directory := t.TempDir()
	modfile := filepath.Join(directory, "native.mod")
	for _, extension := range []string{"mod", "sum"} {
		data, err := os.ReadFile(filepath.Join(root, "go."+extension))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "native."+extension), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	arguments := []string{"-C", nativeRoot, "mod", "edit", "-modfile=" + modfile, "-module=" + module, "-droprequire=" + module, "-dropreplace=" + module}
	for _, dependency := range selection {
		if dependency.Main || dependency.Path == module {
			continue
		}
		arguments = append(arguments, "-require="+dependency.Path+"@"+dependency.Version)
	}
	environment := append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=")
	inspect := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "mod", "edit", "-json")
	inspect.Dir, inspect.Env = root, environment
	configuration, err := inspect.Output()
	if err != nil {
		t.Fatal("inspect consuming replacements", err)
	}
	var manifest struct {
		Replace []struct {
			Old, New struct{ Path, Version string }
		}
	}
	if err := json.Unmarshal(configuration, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, replacement := range manifest.Replace {
		if replacement.Old.Path == module || replacement.New.Version != "" {
			continue
		}
		from, target := replacement.Old.Path, replacement.New.Path
		if replacement.Old.Version != "" {
			from += "@" + replacement.Old.Version
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(root, target)
		}
		arguments = append(arguments, "-replace="+from+"="+target)
	}
	command := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), arguments...)
	command.Env = environment
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("prepare consuming native graph: %v\n%s", err, output)
	}
	return modfile, environment
}

func TestNativeCompatibilityUsesConsumingSelectionsOffline(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source absent")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../../../.."))
	selection := nativeModules(t, root)
	modules := []struct{ path, dir string }{
		{"github.com/enetx/surf", "third_party/surf"},
		{"github.com/enetx/http2", "third_party/surf-http2"},
		{"github.com/enetx/http3", "third_party/surf-http3"},
	}
	for _, module := range modules {
		t.Run(module.path, func(t *testing.T) {
			nativeRoot := filepath.Join(root, module.dir)
			modfile, environment := nativeModuleFile(t, root, nativeRoot, module.path, selection)
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()
			arguments := []string{"-C", nativeRoot, "test"}
			configuration := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "env", "CGO_ENABLED")
			configuration.Env = environment
			cgo, err := configuration.Output()
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(string(cgo)) == "1" {
				arguments = append(arguments, "-race")
			}
			arguments = append(arguments, "-modfile="+modfile, "-mod=readonly", "-count=1", "-timeout=45s", "-run=^TestFathomry", "-v", ".")
			if module.path == "github.com/enetx/surf" {
				arguments = append(arguments, "./pkg/socks4")
			}
			command := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), arguments...)
			command.Env = environment
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("native controls failed: %v\n%s", err, output)
			}
			if !strings.Contains(string(output), "--- PASS: TestFathomry") {
				t.Fatal("native subtest executed no compatibility controls", module.path)
			}
		})
	}
}
