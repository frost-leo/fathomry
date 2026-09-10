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
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
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
	root, err := filepath.Abs("../..")
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
	program, err := os.ReadFile(filepath.Join(root, "internal/compatibility/testdata/consumerprobe/main.go"))
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
		return "module example.org/consumer\n\ngo 1.26.0\nrequire (\n github.com/frost-leo/fathomry v0.0.0\n go.yaml.in/yaml/v3 v3.0.5\n example.org/provider-fixture v1.0.0\n example.org/unused v1.0.0\n)\nreplace github.com/frost-leo/fathomry => " + fmt.Sprintf("%q", filepath.ToSlash(root)) + "\n" + extra
	}
	inspectConsumer := func(t *testing.T, binary string, output []byte, paths ...string) probeOutput {
		t.Helper()
		var executed struct {
			Go       string
			Attempts int
		}
		if err := json.Unmarshal(output, &executed); err != nil {
			t.Fatal("independent consumer output is invalid")
		}
		info, err := buildinfo.ReadFile(binary)
		if err != nil {
			t.Fatal("could not inspect the executed consumer binary")
		}
		build, err := compatibility.FromBuildInfo(info, compatibility.BuildRequest{
			SDKModules: paths, DisclosePaths: []string{"example.org/consumer"},
		})
		if err != nil || build.Go.Value != executed.Go || executed.Go != runtime.Version() {
			t.Fatal("executed and embedded consumer toolchain facts differ")
		}
		project := func(module compatibility.Module) moduleProjection {
			return moduleProjection{Path: module.Path.Value, Version: module.Version.Value, Sum: module.Sum.Value,
				Replacement: string(module.Replacement.Kind), ReplacementPath: module.Replacement.Path.Value,
				ReplacementVersion: module.Replacement.Version.Value, ReplacementSum: module.Replacement.Sum.Value,
				VersionKind: string(module.Version.Kind), Revision: module.VCS.Revision.Value,
				Modified: module.VCS.Modified.Value, Present: module.Present, Main: module.Main}
		}
		_, encodingError := json.Marshal(build)
		actual := probeOutput{Go: build.Go.Value, Main: project(build.Main), Framework: project(build.Framework),
			Attempts: executed.Attempts, SerializationRefused: encodingError != nil}
		for _, module := range build.SDKs {
			actual.SDKs = append(actual.SDKs, project(module))
		}
		if !actual.SerializationRefused ||
			actual.Main.Path != "example.org/consumer" || !actual.Main.Main || !actual.Main.Present ||
			actual.Framework != (moduleProjection{Path: compatibility.FrameworkModule}) {
			t.Fatal("consumer confused its actual build with an unlinked framework requirement")
		}
		diagnostic, err := json.Marshal(actual)
		if err != nil || bytes.Contains(diagnostic, []byte("private-canary")) || bytes.Contains(diagnostic, []byte(root)) {
			t.Fatal("consumer diagnostic disclosed a local path")
		}
		return actual
	}
	probe := func(t *testing.T, extra string, environment []string) probeOutput {
		t.Helper()
		writeFixture(t, app, "go.mod", module(extra))
		binary := filepath.Join(directory, "consumer-probe")
		if output, err := fixtureCommand(t, app, environment, "go", "build", "-mod=mod", "-buildvcs=false", "-o", binary, "."); err != nil {
			t.Fatalf("consumer build failed: %v\n%s", err, output)
		}
		output, err := fixtureCommand(t, app, environment, binary)
		if err != nil {
			t.Fatal("consumer binary failed")
		}
		actual := inspectConsumer(t, binary, output, sdkPath, "go.yaml.in/yaml/v3", "example.org/unused")
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
		writeFixture(t, local, "fixture.go", sdkCode(3))
		workspace := filepath.Join(directory, "go.work")
		writeFixture(t, directory, "go.work", "go 1.26.0\nuse (\n"+fmt.Sprintf("%q\n%q\n", filepath.ToSlash(app), filepath.ToSlash(local))+")\n")
		environment := append(append([]string(nil), env...), "GOWORK="+workspace)
		// In workspace mode go refuses -mod=mod; use its default readonly mode.
		writeFixture(t, app, "go.mod", module("require "+sdkPath+" v1.1.0\n"))
		binary := filepath.Join(directory, "workspace-probe")
		if output, err := fixtureCommand(t, app, environment, "go", "build", "-buildvcs=false", "-o", binary, "."); err != nil {
			t.Fatalf("workspace build: %v\n%s", err, output)
		}
		output, err := fixtureCommand(t, app, environment, binary)
		if err != nil {
			t.Fatal(err)
		}
		actual := inspectConsumer(t, binary, output, sdkPath)
		module := findSDK(actual)
		if module.VersionKind != "development" || module.Version != "" || module.Revision != "" || actual.Attempts != 3 {
			t.Fatal("workspace source falsely reported a selected tagged dependency")
		}
	})
	for _, vcs := range []string{"true", "false"} {
		t.Run("framework-as-main-vcs-"+vcs, func(t *testing.T) {
			binary := filepath.Join(directory, "framework-probe")
			if output, err := fixtureCommand(t, root, env, "go", "build", "-buildvcs="+vcs, "-o", binary, "./internal/compatibility/testdata/buildprobe"); err != nil {
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
