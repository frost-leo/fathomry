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

package version_test

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
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/version"
)

type consumerComponent struct {
	Path, Version, Replacement, ReplacementPath, ReplacementVersion string
	Present, Main                                                   bool
}

type consumerReport struct {
	Code, CheckCode, Go, GOOS, GOARCH                     string
	Main, Framework                                       consumerComponent
	Dependencies                                          []consumerComponent
	NativeRevision, NativeTree, CommitTime                string
	Release, Revision, Tree, BuildTime, DeclarationOrigin string
	English, Chinese, Fallback                            string
	Behavior                                              int
}

func TestIndependentConsumerArtifacts(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	proxy, cache, app := filepath.Join(directory, "proxy"), filepath.Join(directory, "modules"), filepath.Join(directory, "app")
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	read := func(path string) []byte {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
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
	run := func(dir string, env []string, executable string, args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(ctx, executable, args...)
		command.Dir, command.Env = dir, append(os.Environ(), env...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("consumer command %v: %v\n%s", args, err, output)
		}
		return output
	}
	const moduleVersion = "v0.0.0-gh75"
	paths := strings.Split(strings.TrimSuffix(string(run(root, nil, "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")), "\x00"), "\x00")
	slices.Sort(paths)
	var nested []string
	for _, path := range paths {
		if path != "go.mod" && strings.HasSuffix(path, "/go.mod") {
			nested = append(nested, strings.TrimSuffix(path, "go.mod"))
		}
	}
	artifact := make(map[string][]byte)
	for _, path := range paths {
		if slices.ContainsFunc(nested, func(prefix string) bool { return strings.HasPrefix(path, prefix) }) {
			continue
		}
		info, err := os.Lstat(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().IsRegular() {
			artifact[filepath.ToSlash(path)] = read(filepath.Join(root, path))
		}
	}
	addModule := func(path, selected string, files map[string][]byte) {
		t.Helper()
		location := filepath.Join(proxy, path, "@v")
		write(filepath.Join(location, selected+".mod"), files["go.mod"])
		write(filepath.Join(location, selected+".info"), []byte(fmt.Sprintf("{\"Version\":%q,\"Time\":\"2026-09-19T00:00:00Z\"}", selected)))
		var buffer bytes.Buffer
		archive := zip.NewWriter(&buffer)
		names := make([]string, 0, len(files))
		for name := range files {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			entry, err := archive.Create(path + "@" + selected + "/" + name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := entry.Write(files[name]); err != nil {
				t.Fatal(err)
			}
		}
		if err := archive.Close(); err != nil {
			t.Fatal(err)
		}
		write(filepath.Join(location, selected+".zip"), buffer.Bytes())
		write(filepath.Join(location, "list"), []byte(selected+"\n"))
	}
	addModule(version.FrameworkModule, moduleVersion, artifact)
	parentCache := strings.TrimSpace(string(run(root, nil, goBinary, "env", "GOMODCACHE")))
	output := run(root, nil, goBinary, "list", "-m", "-json", "github.com/nicksnyder/go-i18n/v2", "golang.org/x/text", "golang.org/x/mod", "go.yaml.in/yaml/v3")
	decoder := json.NewDecoder(bytes.NewReader(output))
	for decoder.More() {
		var module struct {
			Path, Version string
			Replace       any
		}
		if err := decoder.Decode(&module); err != nil {
			t.Fatal(err)
		}
		if module.Replace != nil {
			t.Fatal("unexpected supporting module replacement")
		}
		for _, extension := range []string{".mod", ".info", ".zip"} {
			name := module.Version + extension
			write(filepath.Join(proxy, module.Path, "@v", name), read(filepath.Join(parentCache, "cache/download", module.Path, "@v", name)))
		}
		write(filepath.Join(proxy, module.Path, "@v", "list"), []byte(module.Version+"\n"))
	}
	header := "/**\n"
	for _, line := range strings.Split(strings.TrimSpace(string(read(filepath.Join(root, ".github/LICENSE_HEADER")))), "\n") {
		header += " *"
		if line != "" {
			header += " " + line
		}
		header += "\n"
	}
	header += " */\n\n"
	for _, item := range []struct {
		path, selected string
		behavior       int
	}{
		{"example.org/selected", "v1.0.0", 1}, {"example.org/fork", "v1.1.0", 2}, {"example.org/hidden", "v1.0.0", 0},
	} {
		addModule(item.path, item.selected, map[string][]byte{
			"go.mod":   []byte("module " + item.path + "\n\ngo 1.27.0\n"),
			"value.go": []byte(header + fmt.Sprintf("package selected\nfunc Value() int { return %d }\n", item.behavior)),
		})
	}
	baseModule := "module example.org/version-consumer\n\ngo 1.27.0\n\nrequire (\n" + version.FrameworkModule + " " + moduleVersion + "\nexample.org/selected v1.0.0\nexample.org/hidden v1.0.0\n)\n"
	write(filepath.Join(app, "go.mod"), []byte(baseModule))
	write(filepath.Join(app, "go.sum"), read(filepath.Join(root, "go.sum")))
	write(filepath.Join(app, "main.go"), read(filepath.Join(root, "version/testdata/consumer/main.go")))
	env := []string{
		"GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=-modcacherw", "GOPROXY=file://" + filepath.ToSlash(proxy),
		"GOSUMDB=off", "GOPRIVATE=", "GONOPROXY=none", "GONOSUMDB=none", "GOMODCACHE=" + cache,
		"CGO_ENABLED=0", "GOOS=" + runtime.GOOS, "GOARCH=" + runtime.GOARCH,
		"GIT_CONFIG_GLOBAL=" + os.DevNull, "GIT_CONFIG_SYSTEM=" + os.DevNull,
		"SOURCE_DATE_EPOCH=1", "GITHUB_SHA=synthetic-environment-canary", "LANG=zh_CN.UTF-8",
	}
	want := declaration()
	stamp := func(input version.Declaration) string {
		return "-X " + version.FrameworkModule + "/version.release=" + input.Release +
			" -X " + version.FrameworkModule + "/version.gitRevision=" + input.GitRevision +
			" -X " + version.FrameworkModule + "/version.tree=" + string(input.Tree) +
			" -X " + version.FrameworkModule + "/version.buildTime=" + input.BuildTime
	}
	build := func(name, flags, vcs string, extraEnv ...string) string {
		t.Helper()
		binary := filepath.Join(directory, name)
		if runtime.GOOS == "windows" {
			binary += ".exe"
		}
		args := []string{"build", "-mod=mod", "-trimpath", "-buildvcs=" + vcs, "-o", binary}
		if flags != "" {
			args = append(args, "-ldflags", flags)
		}
		args = append(args, ".")
		run(app, append(slices.Clone(env), extraEnv...), goBinary, args...)
		return binary
	}
	execute := func(binary string, expected version.Declaration) consumerReport {
		t.Helper()
		output := run(app, env, binary, expected.Release, expected.GitRevision, string(expected.Tree), expected.BuildTime)
		if bytes.Contains(output, []byte(root)) || bytes.Contains(output, []byte(directory)) ||
			bytes.Contains(output, []byte("example.org/hidden")) {
			t.Fatal("consumer disclosed a private path or unrequested inventory")
		}
		var report consumerReport
		if err := json.Unmarshal(output, &report); err != nil {
			t.Fatal(err)
		}
		return report
	}
	inspect := func(binary string) version.Snapshot {
		t.Helper()
		info, err := buildinfo.ReadFile(binary)
		if err != nil {
			t.Fatal(err)
		}
		result, err := version.FromBuildInfo(info, version.Request{
			Dependencies:  []string{"example.org/selected", "example.org/absent"},
			DisclosePaths: []string{"example.org/version-consumer", "example.org/fork"},
		})
		if err != nil {
			t.Fatal(err)
		}
		snapshot := result.Snapshot()
		if snapshot.Declaration.Origin != version.NoDeclaration {
			t.Fatal("file metadata invented a decoded linker declaration")
		}
		if snapshot.Go.Value != info.GoVersion {
			t.Fatal("normalized toolchain differs from raw artifact metadata")
		}
		for _, raw := range info.Settings {
			if raw.Key != "GOOS" && raw.Key != "GOARCH" {
				continue
			}
			if !slices.ContainsFunc(snapshot.Settings, func(setting version.Setting) bool {
				return setting.Name == raw.Key && setting.Value == raw.Value
			}) {
				t.Fatal("normalized target differs from raw artifact metadata")
			}
		}
		return snapshot
	}
	t.Run("unreplaced-archive-and-unstamped", func(t *testing.T) {
		binary := build("unstamped", "", "false")
		report := execute(binary, want)
		if report.Code != "" || report.CheckCode != string(version.MissingDeclaration) ||
			report.Main.Path != "example.org/version-consumer" || !report.Main.Main ||
			report.Framework.Path != version.FrameworkModule || report.Framework.Version != moduleVersion ||
			report.Framework.Main || report.Framework.Replacement != "" || report.Behavior != 1 ||
			report.NativeRevision != "" || report.NativeTree != "" || report.Release != "" {
			t.Fatal("independent module identities or unknown states wrong", report)
		}
		embedded := inspect(binary)
		if embedded.Go.Value != report.Go || embedded.Framework.Version.Value != report.Framework.Version ||
			report.GOOS != runtime.GOOS || report.GOARCH != runtime.GOARCH || report.English == report.Chinese || report.Fallback == "" {
			t.Fatal("artifact/runtime/presentation disagreement")
		}
		if bytes.Contains(read(filepath.Join(app, "go.mod")), []byte("replace ")) {
			t.Fatal("archive fixture unexpectedly replaced framework")
		}
	})
	t.Run("forced-vcs-without-checkout", func(t *testing.T) {
		report := execute(build("archive", "", "true"), want)
		if report.Code != "" || report.NativeTree != "" || report.NativeRevision != "" || report.CheckCode != string(version.MissingDeclaration) {
			t.Fatal("forced VCS invented absent source data", report)
		}
	})
	t.Run("valid-stripped-stamp-and-wrong-expectation", func(t *testing.T) {
		binary := build("stamped", "-s -w "+stamp(want), "false")
		report := execute(binary, want)
		if report.Code != "" || report.CheckCode != "" || report.Release != want.Release || report.Revision != want.GitRevision ||
			report.BuildTime != want.BuildTime || report.DeclarationOrigin != string(version.Linker) ||
			report.Main.Version == report.Release || report.Framework.Version == report.Release {
			t.Fatal("actual injection failed or claimed module identity", report)
		}
		inspect(binary)
		wrong := want
		wrong.Release = "v0.0.1"
		if execute(binary, wrong).CheckCode != string(version.Conflict) {
			t.Fatal("wrong expected stamp accepted")
		}
	})
	for _, item := range []struct{ name, flags, code, check string }{
		{"all-targets-misspelled", strings.ReplaceAll(stamp(want), "/version.", "/missing."), "", string(version.MissingDeclaration)},
		{"one-target-misspelled", strings.Replace(stamp(want), ".release=", ".releaze=", 1), string(version.InvalidDeclaration), ""},
		{"missing-time", strings.Split(stamp(want), " -X "+version.FrameworkModule+"/version.buildTime=")[0], string(version.InvalidDeclaration), ""},
		{"invalid-version", strings.Replace(stamp(want), want.Release, "invalid-private-canary", 1), string(version.InvalidDeclaration), ""},
		{"invalid-time", strings.Replace(stamp(want), want.BuildTime, "yesterday", 1), string(version.InvalidDeclaration), ""},
		{"invalid-tree", strings.Replace(stamp(want), ".tree=clean", ".tree=unknown", 1), string(version.InvalidDeclaration), ""},
		{"short-revision", strings.Replace(stamp(want), want.GitRevision, "1234", 1), string(version.InvalidDeclaration), ""},
	} {
		t.Run(item.name, func(t *testing.T) {
			report := execute(build(item.name, item.flags, "false"), want)
			if report.Code != item.code || report.CheckCode != item.check {
				t.Fatal("injection failure not observed", report)
			}
		})
	}
	t.Run("cross-target-file-read-only", func(t *testing.T) {
		targetOS := "windows"
		if runtime.GOOS == "windows" {
			targetOS = "linux"
		}
		binary := build("cross", stamp(want), "false", "GOOS="+targetOS, "GOARCH=arm64")
		result := inspect(binary)
		settings := make(map[string]string)
		for _, setting := range result.Settings {
			settings[setting.Name] = setting.Value
		}
		if settings["GOOS"] != targetOS || settings["GOARCH"] != "arm64" || result.Origin != version.Supplied || result.Go.Value == "" {
			t.Fatal("inspector facts replaced target facts")
		}
	})
	t.Run("local-framework-and-versioned-dependency-replacement", func(t *testing.T) {
		write(filepath.Join(app, "go.mod"), []byte(baseModule+
			"replace "+version.FrameworkModule+" => "+strconv.Quote(filepath.ToSlash(root))+"\n"+
			"replace example.org/selected => example.org/fork v1.1.0\n"))
		report := execute(build("replaced", stamp(want), "false"), want)
		if report.Code != "" || report.Framework.Replacement != "local" || report.Framework.ReplacementPath != "" ||
			report.Framework.Version != moduleVersion || report.Behavior != 2 || len(report.Dependencies) != 2 {
			t.Fatal("local framework replacement was misattributed", report)
		}
		dependency := report.Dependencies[1]
		if dependency.Path != "example.org/selected" || dependency.Version != "v1.0.0" || dependency.Replacement != "module" ||
			dependency.ReplacementPath != "example.org/fork" || dependency.ReplacementVersion != "v1.1.0" {
			t.Fatal("selected dependency conflated with its replacement", dependency)
		}
	})
	t.Run("actual-source-conflicts", func(t *testing.T) {
		write(filepath.Join(app, "go.mod"), []byte(baseModule))
		build("pre-git", "", "false")
		run(app, env, "git", "init", "-q")
		run(app, env, "git", "add", "go.mod", "go.sum", "main.go")
		run(app, env, "git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.org", "-c", "commit.gpgsign=false", "-c", "core.hooksPath="+os.DevNull, "commit", "-qm", "fixture")
		actual := want
		actual.GitRevision = strings.TrimSpace(string(run(app, env, "git", "rev-parse", "HEAD")))
		binary := build("clean-source", stamp(actual), "true")
		report := execute(binary, actual)
		if report.Code != "" || report.CheckCode != "" || report.NativeRevision != actual.GitRevision ||
			report.NativeTree != string(version.Clean) || report.CommitTime == "" {
			t.Fatal("clean source read-back failed", report)
		}
		if inspect(binary).Source.Revision.Value != actual.GitRevision {
			t.Fatal("file source differs from executed source")
		}
		wrong := actual
		wrong.GitRevision = strings.Repeat("a", 40)
		if execute(build("conflicting-revision", stamp(wrong), "true"), wrong).Code != string(version.Conflict) {
			t.Fatal("native revision conflict was silently overwritten")
		}
		path := filepath.Join(app, "main.go")
		write(path, append(read(path), '\n'))
		if execute(build("conflicting-tree", stamp(actual), "true"), actual).Code != string(version.Conflict) {
			t.Fatal("native dirty tree accepted a clean declaration")
		}
		actual.Tree = version.Dirty
		report = execute(build("dirty-source", stamp(actual), "true"), actual)
		if report.Code != "" || report.CheckCode != "" || report.NativeTree != string(version.Dirty) {
			t.Fatal("explicit dirty source declaration failed", report)
		}
	})
}
