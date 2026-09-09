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

package compatibility_test

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
	"strings"
	"testing"
	"time"
)

type moduleProjection struct {
	Path, Version, Sum, Replacement, ReplacementPath, ReplacementVersion, ReplacementSum string
	VersionKind, Revision, Modified                                                      string
	Present, Main                                                                        bool
}
type probeOutput struct {
	Go                   string
	Main, Framework      moduleProjection
	SDKs                 []moduleProjection
	Attempts             int
	SerializationRefused bool
}

func writeFixture(t testing.TB, directory, name, content string) {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func fixtureCommand(t testing.TB, directory string, env []string, executable string, args ...string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir, command.Env = directory, append(os.Environ(), env...)
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatal("consumer command exceeded its bound")
	}
	return output, err
}

func addProxyModule(t testing.TB, proxy, path, version, module, code string) {
	t.Helper()
	directory := filepath.Join(proxy, path, "@v")
	writeFixture(t, directory, version+".mod", module)
	writeFixture(t, directory, version+".info", fmt.Sprintf("{\"Version\":%q,\"Time\":\"2026-09-09T00:00:00Z\"}", version))
	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	for name, contents := range map[string]string{"go.mod": module, "fixture.go": code} {
		file, err := archive.Create(path + "@" + version + "/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(contents)); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, directory, version+".zip", buffer.String())
}

func TestIndependentConsumerBuildSelectionAndBehavior(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	proxy, app := filepath.Join(directory, "proxy"), filepath.Join(directory, "application")
	headerBytes, err := os.ReadFile(filepath.Join(root, ".github", "LICENSE_HEADER"))
	if err != nil {
		t.Fatal(err)
	}
	header := "/**\n * " + strings.ReplaceAll(strings.TrimSpace(string(headerBytes)), "\n", "\n * ") + "\n */\n"
	cacheOutput, err := fixtureCommand(t, root, nil, "go", "env", "GOMODCACHE")
	if err != nil {
		t.Fatal("could not locate already selected native dependency")
	}
	cache := strings.TrimSpace(string(cacheOutput))
	for _, extension := range []string{".mod", ".info", ".zip"} {
		data, err := os.ReadFile(filepath.Join(cache, "cache/download/go.yaml.in/yaml/v3/@v/v3.0.5"+extension))
		if err != nil {
			t.Fatal("the repository's selected YAML dependency must already be cached")
		}
		writeFixture(t, proxy, "go.yaml.in/yaml/v3/@v/v3.0.5"+extension, string(data))
	}
	sdkModule := "module " + sdkPath + "\n\ngo 1.26.0\n"
	sdkCode := func(attempts int) string {
		return header + fmt.Sprintf(`package fixturesdk
import "context"
func Send(ctx context.Context, send func(context.Context) error) (int, error) {
	var err error
	for count := 0; count < %d; count++ {
		if ctx.Err() != nil { return count, ctx.Err() }
		err = send(ctx)
		if err == nil { return count+1, nil }
	}
	return %d, err
}
`, attempts, attempts)
	}
	addProxyModule(t, proxy, sdkPath, "v1.0.0", sdkModule, sdkCode(1))
	addProxyModule(t, proxy, sdkPath, "v1.1.0", sdkModule, sdkCode(2))
	addProxyModule(t, proxy, "example.org/provider-fixture", "v1.0.0",
		"module example.org/provider-fixture\n\ngo 1.26.0\nrequire "+sdkPath+" v1.0.0\n",
		header+"package providerfixture\nimport sdk "+fmt.Sprintf("%q", sdkPath)+"\nvar Send = sdk.Send\n")
	addProxyModule(t, proxy, "example.org/unused", "v1.0.0", "module example.org/unused\n\ngo 1.26.0\n", header+"package unused\n")
	program, err := os.ReadFile(filepath.Join(root, "compatibility/testdata/buildprobe/main.go"))
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, app, "main.go", string(program))
	writeFixture(t, app, "sdk.go", header+`package main
import (
	"context"
	"io"
	provider "example.org/provider-fixture"
)
func init() {
	nativeAttempts, _ = provider.Send(context.Background(), func(context.Context) error { return io.ErrUnexpectedEOF })
}
`)
	writeFixture(t, app, "consumer_test.go", header+`package main
import (
	"context"
	"io"
	"testing"
	provider "example.org/provider-fixture"
)
func TestAcceptedDefaultRetryContract(t *testing.T) {
	entries := 0
	count, err := provider.Send(context.Background(), func(context.Context) error { entries++; return io.ErrUnexpectedEOF })
	if entries != 1 || count != 1 || err != io.ErrUnexpectedEOF {
		t.Error("conformance: default retry or error identity changed")
	}
}
`)
	local := filepath.Join(directory, "private-canary-sdk")
	writeFixture(t, local, "go.mod", sdkModule)
	writeFixture(t, local, "fixture.go", sdkCode(1))
	env := []string{"GOWORK=off", "GOPROXY=file://" + filepath.ToSlash(proxy), "GOSUMDB=off",
		"GONOPROXY=none", "GOPRIVATE=", "GONOSUMDB=", "GOTOOLCHAIN=local", "GOFLAGS=-modcacherw", "GOMODCACHE=" + filepath.Join(directory, "cache")}
	module := func(extra string) string {
		return "module example.org/consumer\n\ngo 1.26.0\nrequire (\n github.com/frost-leo/fathomry v0.0.0\n example.org/provider-fixture v1.0.0\n example.org/unused v1.0.0\n)\nreplace github.com/frost-leo/fathomry => " + fmt.Sprintf("%q", filepath.ToSlash(root)) + "\n" + extra
	}
	probe := func(t *testing.T, extra string, environment []string) probeOutput {
		t.Helper()
		writeFixture(t, app, "go.mod", module(extra))
		binary := filepath.Join(directory, "consumer-probe")
		if output, err := fixtureCommand(t, app, environment, "go", "build", "-mod=mod", "-buildvcs=false", "-o", binary, "."); err != nil {
			t.Fatalf("consumer build failed: %v\n%s", err, output)
		}
		output, err := fixtureCommand(t, app, environment, binary, sdkPath, "go.yaml.in/yaml/v3", "example.org/unused")
		if err != nil {
			t.Fatal("consumer binary failed")
		}
		if bytes.Contains(output, []byte("private-canary")) || bytes.Contains(output, []byte(root)) {
			t.Fatal("consumer diagnostic disclosed a local path")
		}
		var actual probeOutput
		if err := json.Unmarshal(output, &actual); err != nil {
			t.Fatal(err)
		}
		if actual.Go != runtime.Version() || !actual.SerializationRefused || actual.Framework.Main ||
			actual.Framework.Replacement != "local" || actual.Framework.ReplacementPath != "" || actual.Framework.Revision != "" {
			t.Fatal("consumer confused framework dependency with main application/build facts")
		}
		for _, dependency := range actual.SDKs {
			if dependency.Path == "example.org/unused" && dependency.Present {
				t.Fatal("unused requirement was reported as contributing build content")
			}
			if dependency.Path == "go.yaml.in/yaml/v3" && (!dependency.Present || dependency.Version != "v3.0.5" || dependency.Sum != moduleSum) {
				t.Fatal("actual pinned native module facts unavailable")
			}
		}
		return actual
	}
	findSDK := func(actual probeOutput) moduleProjection {
		for _, module := range actual.SDKs {
			if module.Path == sdkPath {
				return module
			}
		}
		t.Fatal("selected SDK query disappeared")
		return moduleProjection{}
	}
	t.Run("provider-minimum", func(t *testing.T) {
		actual := probe(t, "", env)
		if module := findSDK(actual); module.Version != "v1.0.0" || module.Sum == "" || actual.Attempts != 1 {
			t.Fatal("provider's selected minimum not observed")
		}
		if output, err := fixtureCommand(t, app, env, "go", "test", "-mod=mod", "-count=1", "."); err != nil {
			t.Fatalf("baseline contract: %v\n%s", err, output)
		}
	})
	t.Run("consumer-upgrade-changes-behavior", func(t *testing.T) {
		actual := probe(t, "require "+sdkPath+" v1.1.0\n", env)
		if module := findSDK(actual); module.Version != "v1.1.0" || module.Replacement != "" || actual.Attempts != 2 {
			t.Fatal("MVS selection or changed retry default hidden")
		}
		output, err := fixtureCommand(t, app, env, "go", "test", "-mod=mod", "-count=1", ".")
		if err == nil || !bytes.Contains(output, []byte("conformance: default retry")) {
			t.Fatal("compiling SDK upgrade did not fail the accepted behavioral contract")
		}
	})
	t.Run("versioned-replacement", func(t *testing.T) {
		actual := probe(t, "require "+sdkPath+" v1.1.0\nreplace "+sdkPath+" => "+sdkPath+" v1.0.0\n", env)
		module := findSDK(actual)
		if module.Version != "v1.1.0" || module.Replacement != "module" || module.ReplacementVersion != "v1.0.0" ||
			module.ReplacementPath != sdkPath || module.ReplacementSum == "" || actual.Attempts != 1 {
			t.Fatal("selected version confused with replacement content")
		}
	})
	t.Run("local-replacement-and-modification", func(t *testing.T) {
		extra := "require " + sdkPath + " v1.1.0\nreplace " + sdkPath + " => " + fmt.Sprintf("%q", filepath.ToSlash(local)) + "\n"
		actual := probe(t, extra, env)
		module := findSDK(actual)
		if module.Version != "v1.1.0" || module.Replacement != "local" || module.ReplacementPath != "" || module.Revision != "" || actual.Attempts != 1 {
			t.Fatal("local replacement facts incorrect")
		}
		writeFixture(t, local, "fixture.go", sdkCode(3))
		changed := probe(t, extra, env)
		if findSDK(changed) != module || changed.Attempts != 3 {
			t.Fatal("local changed content test did not establish metadata's limit")
		}
	})
	t.Run("workspace-selection", func(t *testing.T) {
		workspace := filepath.Join(directory, "go.work")
		writeFixture(t, directory, "go.work", "go 1.26.0\nuse (\n"+fmt.Sprintf("%q\n%q\n", filepath.ToSlash(app), filepath.ToSlash(local))+")\n")
		environment := append(append([]string(nil), env...), "GOWORK="+workspace)
		// In workspace mode go refuses -mod=mod; use its default readonly mode.
		writeFixture(t, app, "go.mod", module("require "+sdkPath+" v1.1.0\n"))
		binary := filepath.Join(directory, "workspace-probe")
		if output, err := fixtureCommand(t, app, environment, "go", "build", "-buildvcs=false", "-o", binary, "."); err != nil {
			t.Fatalf("workspace build: %v\n%s", err, output)
		}
		output, err := fixtureCommand(t, app, environment, binary, sdkPath)
		if err != nil {
			t.Fatal(err)
		}
		var actual probeOutput
		if err := json.Unmarshal(output, &actual); err != nil {
			t.Fatal(err)
		}
		module := findSDK(actual)
		if module.VersionKind != "development" || module.Version != "" || module.Revision != "" || actual.Attempts != 3 {
			t.Fatal("workspace source falsely reported a selected tagged dependency")
		}
	})
	for _, vcs := range []string{"true", "false"} {
		t.Run("framework-as-main-vcs-"+vcs, func(t *testing.T) {
			binary := filepath.Join(directory, "framework-probe")
			if output, err := fixtureCommand(t, root, env, "go", "build", "-buildvcs="+vcs, "-o", binary, "./compatibility/testdata/buildprobe"); err != nil {
				t.Fatalf("framework main build: %v\n%s", err, output)
			}
			output, err := fixtureCommand(t, root, env, binary, "go.yaml.in/yaml/v3")
			if err != nil {
				t.Fatal(err)
			}
			var actual probeOutput
			if err := json.Unmarshal(output, &actual); err != nil {
				t.Fatal(err)
			}
			if !actual.Framework.Main || actual.Framework.Revision != actual.Main.Revision || actual.SDKs[0].Revision != "" ||
				vcs == "true" && actual.Framework.Revision == "" || vcs == "false" && actual.Framework.Revision != "" {
				t.Fatal("main VCS attribution or explicit unavailable metadata incorrect")
			}
		})
	}
}
