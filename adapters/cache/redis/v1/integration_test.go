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

package redis

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestIndependentPublicConsumers(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"direct", "framework"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			fixture, err := os.ReadFile("testdata/" + mode + "/main.go")
			if err != nil {
				t.Fatal(err)
			}
			syntax, err := parser.ParseFile(token.NewFileSet(), "main.go", fixture, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, imported := range syntax.Imports {
				name, err := strconv.Unquote(imported.Path.Value)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(name, "/internal/") || strings.Contains(name, "go-redis") || mode == "direct" && strings.Contains(name, "/framework/") {
					t.Fatal("consumer bypassed public boundary")
				}
			}
			sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
			if err != nil {
				t.Fatal(err)
			}
			for name, content := range map[string][]byte{
				"main.go": fixture, "go.sum": sums, "go.mod": []byte(fmt.Sprintf("module example.org/redis-%s-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", mode, filepath.ToSlash(root))),
			} {
				if err := os.WriteFile(filepath.Join(directory, name), content, 0600); err != nil {
					t.Fatal(err)
				}
			}
			run := func(offline bool, args ...string) []byte {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				command := exec.CommandContext(ctx, "go", args...)
				command.Dir = directory
				command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
				if offline {
					command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
				}
				output, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("external consumer %v: %v\n%s", args, err, output)
				}
				return output
			}
			run(false, "mod", "tidy")
			run(true, "mod", "tidy", "-diff")
			binary := filepath.Join(directory, "consumer")
			build := []string{"build", "-mod=readonly", "-o", binary}
			if strings.TrimSpace(string(run(false, "env", "CGO_ENABLED"))) == "1" {
				build = append(build, "-race")
			}
			run(true, append(build, ".")...)
			dependencies := run(true, "list", "-mod=readonly", "-deps", ".")
			for _, name := range strings.Fields(string(dependencies)) {
				if strings.Contains(name, "/fathomry/adapters/database/") || strings.Contains(name, "/fathomry/internal/broker/") || mode == "direct" && strings.Contains(name, "/fathomry/framework/") {
					t.Fatal("unrelated provider dependency", name)
				}
			}
			info, err := buildinfo.ReadFile(binary)
			if err != nil {
				t.Fatal(err)
			}
			pinned := false
			for _, module := range info.Deps {
				if module.Path == "github.com/redis/go-redis/v9" {
					pinned = module.Version == "v9.22.0" && module.Replace == nil && module.Sum != ""
				}
			}
			if !pinned {
				t.Fatal("native SDK dependency drift")
			}
			address := peer(t, func(_ net.Conn, args []string) string {
				switch strings.ToUpper(args[0]) {
				case "GET":
					return "$-1\r\n"
				case "XADD":
					return "-WRONGTYPE synthetic\r\n"
				default:
					return "+OK\r\n"
				}
			})
			input, err := json.Marshal(testSettings(address))
			if err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(testContext(t), binary)
			command.Stdin = bytes.NewReader(input)
			output, err := command.CombinedOutput()
			expected := "redis public consumer passed\n"
			if mode == "framework" {
				expected = "redis framework consumer passed\n"
			}
			if err != nil || string(output) != expected {
				t.Fatalf("external execution: %v\n%s", err, output)
			}
		})
	}
}
