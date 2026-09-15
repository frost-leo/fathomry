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

package httpcloak

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sardanioss/httpcloak/transport"
	nethttp2 "github.com/sardanioss/net/http2"
	nativeh3 "github.com/sardanioss/quic-go/http3"
	"github.com/sardanioss/udpbara"
)

// Native submodules use the versions actually compiled by this consuming test
// package. Unrelated framework SDKs are not dependencies of these SDK tests.
func TestNativeCompatibilityUnitControls(t *testing.T) {
	if transport.FathomryCompatibilityRevision == "" || nativeh3.FathomryCompatibilityRevision == "" || nethttp2.FathomryCompatibilityRevision == "" || udpbara.FathomryCompatibilityRevision == "" {
		t.Fatal("local compatibility marker unavailable")
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("source path unavailable")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../../../.."))
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	env := append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=")
	run := func(args ...string) []byte {
		command := exec.CommandContext(ctx, goBinary, args...)
		command.Env = env
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("native module preparation: %v\n%s", err, output)
		}
		return output
	}
	selected := make(map[string]string)
	for _, line := range strings.Split(string(run("list", "-mod=readonly", "-deps", "-test", "-f", "{{if .Module}}{{.Module.Path}} {{.Module.Version}}{{end}}", ".")), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			selected[fields[0]] = fields[1]
		}
	}
	goVersion := strings.TrimSpace(string(run("list", "-m", "-f", "{{.GoVersion}}")))
	cgo := strings.TrimSpace(string(run("env", "CGO_ENABLED")))
	paths := make([]string, 0, len(selected))
	for path := range selected {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	replacements := map[string]string{
		"github.com/sardanioss/httpcloak": "httpcloak",
		"github.com/sardanioss/quic-go":   "httpcloak-quic-go",
		"github.com/sardanioss/net":       "httpcloak-net",
		"github.com/sardanioss/udpbara":   "udpbara",
	}
	rootSums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for module, directory := range replacements {
		t.Run(directory, func(t *testing.T) {
			moduleFile := filepath.Join(t.TempDir(), "native.mod")
			var description strings.Builder
			description.WriteString("module " + module + "\n\ngo " + goVersion + "\n\nrequire (\n")
			for _, path := range paths {
				if path != module {
					description.WriteString(path + " " + selected[path] + "\n")
				}
			}
			description.WriteString(")\n")
			for dependency, path := range replacements {
				if dependency != module {
					description.WriteString("replace " + dependency + " => " + filepath.ToSlash(filepath.Join(root, "third_party", path)) + "\n")
				}
			}
			if err := os.WriteFile(moduleFile, []byte(description.String()), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(strings.TrimSuffix(moduleFile, ".mod")+".sum", rootSums, 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"-C", filepath.Join(root, "third_party", directory), "test", "-modfile=" + moduleFile, "-mod=readonly", "-count=1", "-timeout=60s", "-run=^TestFathomry"}
			if cgo == "1" {
				args = append(args, "-race")
			}
			args = append(args, "./...")
			command := exec.CommandContext(ctx, goBinary, args...)
			command.Env = env
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("native selected-version regressions: %v\n%s", err, output)
			}
		})
	}
}
