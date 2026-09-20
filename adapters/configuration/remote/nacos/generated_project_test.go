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

package nacos_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestGeneratedProjectAuthenticatedConfiguration(t *testing.T) {
	f := newFixture(t, true)
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	environment := append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=-modcacherw", "GOPROXY=off", "GOSUMDB=off", "GOPRIVATE=", "GONOPROXY=none")
	run := func(work, executable string, args ...string) (string, error) {
		t.Helper()
		command := exec.CommandContext(ctx, executable, args...)
		command.Dir = work
		command.Env = environment
		data, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("generated project check exceeded its bound")
		}
		return string(data), err
	}
	must := func(work, executable string, args ...string) string {
		t.Helper()
		output, err := run(work, executable, args...)
		if err != nil {
			t.Fatalf("generated project %v: %v\n%s", args, err, output)
		}
		return output
	}
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	const module = "github.com/frost-leo/fathomry"
	const version = "v0.0.0-gh83-nacos"
	files := strings.Split(strings.TrimSuffix(must(root, "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"), "\x00"), "\x00")
	var nested []string
	for _, file := range files {
		if file != "go.mod" && strings.HasSuffix(file, "/go.mod") {
			nested = append(nested, strings.TrimSuffix(file, "go.mod"))
		}
	}
	var data bytes.Buffer
	archive := zip.NewWriter(&data)
	for _, file := range files {
		if slices.ContainsFunc(nested, func(prefix string) bool { return strings.HasPrefix(file, prefix) }) {
			continue
		}
		info, err := os.Lstat(filepath.Join(root, file))
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		content, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Fatal(err)
		}
		output, err := archive.Create(module + "@" + version + "/" + filepath.ToSlash(file))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := output.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	proxy := filepath.Join(directory, "proxy")
	versions := filepath.Join(proxy, module, "@v")
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(versions, version+".zip"), data.Bytes())
	write(filepath.Join(versions, version+".mod"), mod)
	write(filepath.Join(versions, version+".info"), []byte(`{"Version":"`+version+`","Time":"2026-09-20T00:00:00Z"}`))
	write(filepath.Join(versions, "list"), []byte(version+"\n"))
	cli := filepath.Join(directory, "fathomry")
	must(root, goBinary, "build", "-o", cli, "./cmd/fathomry")
	must(directory, cli, "new", "remote", "--directory", directory, "--module", "example.org/remote", "--framework-version", version, "--environment-source", "production=remote/nacos")
	parentCache := strings.TrimSpace(must(root, goBinary, "env", "GOMODCACHE"))
	environment = append(environment, "GOMODCACHE="+filepath.Join(directory, "modules"),
		"GOPROXY=file://"+filepath.ToSlash(proxy)+",file://"+filepath.ToSlash(filepath.Join(parentCache, "cache/download")))
	project := filepath.Join(directory, "remote")
	must(project, goBinary, "test", "-mod=mod", "-race", "-count=1", "./...")
	binary := filepath.Join(project, "remote")
	must(project, goBinary, "build", "-mod=readonly", "-o", binary, "./cmd/remote")
	options := f.options()
	prefix := "REMOTE_PRODUCTION_NACOS_"
	var bootstrap strings.Builder
	for name, value := range map[string]string{
		"HTTP_URL": options.Servers[0].HTTPURL, "GRPC_ADDRESS": options.Servers[0].GRPCAddress,
		"NAMESPACE": options.Namespace, "USERNAME": options.Username, "PASSWORD": options.Password,
		"ROOT_CA_PEM": options.RootCAPEM, "ALLOW_INSECURE": "false",
	} {
		fmt.Fprintf(&bootstrap, "%s%s=%q\n", prefix, name, value)
	}
	bootstrap.WriteString("UNUSED_PRIVATE=private-settings-canary\n")
	write(filepath.Join(project, ".env.production"), []byte(bootstrap.String()))
	development := must(directory, binary, "--root", project, "--environment", "development")
	if !strings.Contains(development, "\"Provider\":\"viper\"") || f.queries.Load() != 0 || f.logins.Load() != 0 {
		t.Fatal("development acquired remote resources", development)
	}

	f.mu.Lock()
	f.content["remote.base.yaml"] = "i18n: {default_locale: en}\n"
	f.content["remote.production.yaml"] = "i18n: {default_locale: zh-Hans}\n"
	f.mu.Unlock()
	output := must(directory, binary, "--root", project, "--environment", "production", "--env-file", ".env.production")
	var result struct {
		Environment   string `json:"environment"`
		Configuration struct {
			Provider string
			Sources  []struct {
				Name    string
				Present bool
			}
		} `json:"configuration"`
	}
	if json.Unmarshal([]byte(output), &result) != nil || result.Environment != "production" || result.Configuration.Provider != "nacos" ||
		len(result.Configuration.Sources) != 2 || f.queries.Load() != 2 || f.logins.Load() != 1 || f.watches.Load() != 0 {
		t.Fatal("generated remote project did not load its declared sources", output)
	}
	if strings.Contains(output, "private-settings-canary") || strings.Contains(output, "private-reader") ||
		strings.Contains(output, options.Servers[0].HTTPURL) {
		t.Fatal("generated metadata exposed private input")
	}
	for _, mode := range []string{"missing", "denied", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			f.mu.Lock()
			f.faultID = "remote.production.yaml"
			f.fault = 0
			f.content["remote.production.yaml"] = "i18n: {default_locale: zh-Hans}\n"
			switch mode {
			case "missing":
				delete(f.content, "remote.production.yaml")
			case "denied":
				f.fault = 403
			case "invalid":
				f.content["remote.production.yaml"] = "unknown: private-native-message\n"
			}
			f.mu.Unlock()
			output, err := run(directory, binary, "--root", project, "--environment", "production", "--env-file", ".env.production")
			if err == nil || !strings.Contains(output, "fathomry.configuration.") || strings.Contains(output, "private-native-message") {
				t.Fatal("generated remote refusal failed", output, err)
			}
		})
	}
}
