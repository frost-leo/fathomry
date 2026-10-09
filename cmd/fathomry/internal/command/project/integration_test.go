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

package project

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/mod/module"
	modzip "golang.org/x/mod/zip"
)

func goTool() string {
	name := "go"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(runtime.GOROOT(), "bin", name)
}
func consumerEnvironment(extra ...string) []string {
	var result []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "FATHOMRY_") || name == "GOWORK" || name == "GOTOOLCHAIN" || name == "GOFLAGS" {
			continue
		}
		replace := false
		for _, value := range extra {
			key, _, _ := strings.Cut(value, "=")
			if name == key {
				replace = true
			}
		}
		if !replace {
			result = append(result, entry)
		}
	}
	return append(append(result, "GOWORK=off", "GOTOOLCHAIN=local"), extra...)
}
func runConsumer(t testing.TB, directory string, environment []string, command string, args ...string) []byte {
	t.Helper()
	return runConsumerBudget(t, 90*time.Second, directory, environment, command, args...)
}

func runConsumerBudget(t testing.TB, budget time.Duration, directory string, environment []string, command string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	started := time.Now()
	process := exec.CommandContext(ctx, command, args...)
	process.Dir = directory
	process.Env = environment
	output, err := process.CombinedOutput()
	t.Logf("consumer %s %v: elapsed=%s budget=%s", filepath.Base(command), args, time.Since(started), budget)
	if err != nil || ctx.Err() != nil {
		t.Fatalf("consumer command %s %v: %v; context=%v cause=%v\n%s", filepath.Base(command), args, err, ctx.Err(), context.Cause(ctx), output)
	}
	return output
}
func qualifyProject(t *testing.T, directory string, environment []string, mode string) {
	t.Helper()
	for _, args := range [][]string{{"mod", "tidy"}, {"mod", "tidy", "-diff"}, {"vet", "./..."}, {"test", "-race", "-count=1", "./..."}} {
		runConsumer(t, directory, environment, goTool(), args...)
	}
	binary := filepath.Join(t.TempDir(), "application")
	runConsumer(t, directory, environment, goTool(), "build", "-mod=readonly", "-o", binary, "./cmd/demo")
	for _, name := range []string{"development", "testing", "production"} {
		if mode == "remote" {
			break
		}
		output := runConsumer(t, directory, environment, binary, "--env", name, "--config-dir", filepath.Join(directory, "configs"))
		var report struct {
			Status   string `json:"status"`
			Accepted bool   `json:"configuration_accepted"`
			Worker   bool   `json:"worker_started"`
		}
		if json.Unmarshal(output, &report) != nil || !report.Accepted || report.Worker || report.Status != "configuration_accepted" {
			t.Fatal("generated executable misreported configuration")
		}
	}
	boot, err := os.ReadFile(filepath.Join(directory, "internal/bootstrap/boot.go"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(boot, []byte("configuration.Bootstrap")) || !bytes.Contains(boot, []byte("configuration.Load(")) || !bytes.Contains(boot, []byte("configuration.Watch(")) {
		t.Fatal("generated boot does not consume the Framework scenario")
	}
	packages := runConsumer(t, directory, environment, goTool(), "list", "-f", "{{.ImportPath}} {{join .Imports \" \"}}", "./...")
	if bytes.Contains(packages, []byte(frameworkModule+"/adapters/")) || bytes.Contains(packages, []byte(frameworkModule+"/internal/")) {
		t.Fatal("project assembly imports Adapter or Internal APIs")
	}

}

func executePublicConsumer(t testing.TB, directory string, environment []string, binary, expected string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	process := exec.CommandContext(ctx, binary)
	process.Dir, process.Env = directory, environment
	var diagnostics bytes.Buffer
	process.Stderr = &diagnostics
	output, err := process.Output()
	if err != nil || ctx.Err() != nil || string(output) != expected {
		t.Fatalf("versioned public consumer failed: %v\n%s\n%s", err, output, diagnostics.String())
	}
	if diagnostics.Len() != 0 {
		t.Logf("native protocol diagnostics: %s", diagnostics.String())
	}
}

func TestIndependentProjects(t *testing.T) {
	if testing.Short() {
		t.Skip("independent generated-module qualification")
	}
	for _, mode := range []string{"local", "remote"} {
		for _, encoding := range []string{"yaml", "toml"} {
			t.Run(mode+"/"+encoding, func(t *testing.T) {
				tree := testPlan(t, mode, encoding)
				if err := create(context.Background(), tree, writeProjectFile); err != nil {
					t.Fatal(err)
				}
				qualifyProject(t, tree.destination, consumerEnvironment(), mode)
			})
		}
	}
}

func TestVersionedProjects(t *testing.T) {
	if testing.Short() {
		t.Skip("cold-cache versioned-module qualification")
	}
	if runtime.GOOS != "linux" {
		t.Skip("the cold module-proxy runtime fixture is Linux-qualified")
	}
	job := t.TempDir()
	proxy := filepath.Join(job, "proxy")
	cache := filepath.Join(job, "module-cache")
	const fixtureVersion = "v0.0.0-configuration-test"
	versionDirectory := filepath.Join(proxy, filepath.FromSlash(frameworkModule), "@v")
	if err := os.MkdirAll(versionDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	moduleFile, err := os.ReadFile(filepath.Join(repository(t), "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		fixtureVersion + ".mod":  moduleFile,
		fixtureVersion + ".info": []byte("{\"Version\":\"" + fixtureVersion + "\",\"Time\":\"2026-10-02T00:00:00Z\"}"),
		"list":                   []byte(fixtureVersion + "\n"),
	} {
		if err := os.WriteFile(filepath.Join(versionDirectory, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	archive, err := os.Create(filepath.Join(versionDirectory, fixtureVersion+".zip"))
	if err != nil {
		t.Fatal(err)
	}
	err = modzip.CreateFromDir(archive, module.Version{Path: frameworkModule, Version: fixtureVersion}, repository(t))
	closeErr := archive.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	toolDirectory := filepath.Join(job, "tool")
	if err := os.Mkdir(toolDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	manifest := "module example.org/configuration-tool\n\ngo 1.27.0\nrequire " + frameworkModule + " " + fixtureVersion + "\n"
	for _, pin := range sdkPins() {
		manifest += "replace " + pin.original + " => " + frameworkModule + "/" + pin.directory + " " + pin.version + "\n"
	}
	if err := os.WriteFile(filepath.Join(toolDirectory, "go.mod"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	proxyURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(proxy)}).String()
	environment := consumerEnvironment("GOMODCACHE="+cache, "GOPROXY="+proxyURL+",https://proxy.golang.org", "GONOSUMDB="+frameworkModule, "GOFLAGS=-modcacherw")
	cli := filepath.Join(job, "fathomry")
	runConsumerBudget(t, 3*time.Minute, toolDirectory, environment, goTool(), "list", "-mod=mod", "-deps", frameworkModule+"/cmd/fathomry")
	offline := append(append([]string(nil), environment...), "GOPROXY=off", "GONOPROXY=none", "GOSUMDB=off")
	runConsumerBudget(t, 3*time.Minute, toolDirectory, offline, goTool(), "build", "-mod=readonly", "-o", cli, frameworkModule+"/cmd/fathomry")
	verifyHTTPReplacement(t, toolDirectory, offline)
	verifyTelemetryReplacement(t, toolDirectory, offline)
	for _, mode := range []string{"local", "remote"} {
		for _, encoding := range []string{"yaml", "toml"} {
			t.Run(mode+"/"+encoding, func(t *testing.T) {
				destination := filepath.Join(job, mode+"-"+encoding)
				provider := "viper"
				if mode == "remote" {
					provider = "nacos"
				}
				runConsumer(t, job, environment, cli, "new", destination, "--name=demo", "--module=example.org/demo", "--config-source", mode, "--config-provider", provider, "--config-format", encoding, "--output=json")
				qualifyProject(t, destination, environment, mode)
				moduleJSON := runConsumer(t, destination, environment, goTool(), "list", "-m", "-json", frameworkModule)
				var selected struct {
					Version, Dir string
					Replace      any
				}
				if json.Unmarshal(moduleJSON, &selected) != nil || selected.Version != fixtureVersion || selected.Replace != nil || !strings.HasPrefix(selected.Dir, cache+string(filepath.Separator)) {
					t.Fatal("versioned project retained a developer checkout")
				}
				raw, err := os.ReadFile(filepath.Join(destination, "go.mod"))
				if err != nil {
					t.Fatal(err)
				}
				for _, pin := range sdkPins() {
					expected := frameworkModule + "/" + pin.directory + " " + pin.version
					if !bytes.Contains(raw, []byte(expected)) {
						t.Fatal("versioned SDK policy missing", pin.original)
					}
				}
				t.Logf("%s/%s: generated, tidied, vetted, race-tested and built from the isolated module cache; local programs executed, remote programs reject absent bootstrap", mode, encoding)
				if mode == "local" && encoding == "yaml" {
					for _, provider := range []string{"nethttp", "tlsclient", "surf", "httpcloak", "nuki"} {
						for _, variant := range []string{"direct", "framework"} {
							fixture, err := os.ReadFile(filepath.Join(repository(t), "adapters/httpclient", provider, "v1/testdata", variant, "main.go"))
							if err != nil {
								t.Fatal(err)
							}
							name := "http-" + provider + "-" + variant
							directory := filepath.Join(destination, "cmd", name)
							if err := os.MkdirAll(directory, 0700); err != nil {
								t.Fatal(err)
							}
							if err := os.WriteFile(filepath.Join(directory, "main.go"), fixture, 0600); err != nil {
								t.Fatal(err)
							}
							runConsumer(t, destination, environment, goTool(), "mod", "tidy")
							runConsumer(t, destination, environment, goTool(), "mod", "tidy", "-diff")
							binary := filepath.Join(job, name)
							runConsumerBudget(t, 3*time.Minute, destination, environment, goTool(), "build", "-mod=readonly", "-race", "-o", binary, "./cmd/"+name)
							executePublicConsumer(t, destination, environment, binary, provider+" "+variant+" public consumer passed\n")
						}
					}
					for _, variant := range []string{"direct", "framework"} {
						fixture, err := os.ReadFile(filepath.Join(repository(t), "adapters/telemetry/otel/v1/testdata", variant, "main.go"))
						if err != nil {
							t.Fatal(err)
						}
						name := "otel-" + variant
						directory := filepath.Join(destination, "cmd", name)
						if err := os.MkdirAll(directory, 0700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(directory, "main.go"), fixture, 0600); err != nil {
							t.Fatal(err)
						}
						runConsumer(t, destination, environment, goTool(), "mod", "tidy")
						runConsumer(t, destination, environment, goTool(), "mod", "tidy", "-diff")
						binary := filepath.Join(job, name)
						runConsumerBudget(t, 3*time.Minute, destination, environment, goTool(), "build", "-mod=readonly", "-race", "-o", binary, "./cmd/"+name)
						executePublicConsumer(t, destination, environment, binary, "otel "+variant+" public consumer passed\n")
					}
					for _, variant := range []string{"direct", "framework"} {
						fixture, err := os.ReadFile(filepath.Join(repository(t), "adapters/logging/zap/v1/testdata", variant, "main.go"))
						if err != nil {
							t.Fatal(err)
						}
						name := "zap-" + variant
						directory := filepath.Join(destination, "cmd", name)
						if err := os.MkdirAll(directory, 0700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(directory, "main.go"), fixture, 0600); err != nil {
							t.Fatal(err)
						}
						runConsumer(t, destination, environment, goTool(), "mod", "tidy")
						runConsumer(t, destination, environment, goTool(), "mod", "tidy", "-diff")
						binary := filepath.Join(job, name)
						runConsumerBudget(t, 3*time.Minute, destination, environment, goTool(), "build", "-mod=readonly", "-race", "-o", binary, "./cmd/"+name)
						executePublicConsumer(t, destination, append(append([]string(nil), environment...), "TMPDIR="+job), binary, "zap "+variant+" public consumer passed\n")
						packages := runConsumer(t, destination, environment, goTool(), "list", "-mod=readonly", "-deps", "./cmd/"+name)
						for _, dependency := range strings.Fields(string(packages)) {
							if strings.HasPrefix(dependency, "go.opentelemetry.io/") || strings.HasPrefix(dependency, frameworkModule+"/adapters/telemetry/") ||
								strings.HasPrefix(dependency, frameworkModule+"/internal/telemetry/") || strings.HasPrefix(dependency, "github.com/rs/zerolog") ||
								variant == "direct" && strings.HasPrefix(dependency, frameworkModule+"/framework/") {
								t.Fatal("versioned local-only Zap consumer acquired an unnecessary provider", dependency)
							}
						}
					}
					selected := runConsumer(t, destination, environment, goTool(), "list", "-m", "-json", frameworkModule)
					if bytes.Contains(selected, []byte("\"Replace\"")) {
						t.Fatal("public consumer acquired checkout replacement")
					}
					verifyHTTPReplacement(t, destination, environment)
					verifyTelemetryReplacement(t, destination, environment)
					verifyZapSelection(t, destination, environment)
				}
			})
		}
	}
	if _, err := os.Stat(filepath.Join(repository(t), "scripts", "install-cli.sh")); err == nil {
		t.Fatal("unrequested installer appeared")
	}
}

func verifyZapSelection(t testing.TB, directory string, environment []string) {
	t.Helper()
	raw := runConsumer(t, directory, environment, goTool(), "list", "-mod=readonly", "-m", "-json", "go.uber.org/zap")
	var selected struct {
		Version, Sum string
		Replace      any
	}
	if json.Unmarshal(raw, &selected) != nil || selected.Version != "v1.28.0" || selected.Replace != nil || selected.Sum == "" {
		t.Fatal("versioned Zap consumer changed the selected original SDK")
	}
}

func verifyHTTPReplacement(t testing.TB, directory string, environment []string) {
	t.Helper()
	verified := 0
	for _, pin := range sdkPins() {
		switch pin.original {
		case "github.com/bogdanfinn/tls-client", "github.com/enetx/surf", "github.com/enetx/http2", "github.com/enetx/http3",
			"github.com/sardanioss/httpcloak", "github.com/sardanioss/net", "github.com/sardanioss/quic-go", "github.com/sardanioss/udpbara",
			"github.com/nukilabs/http", "github.com/nukilabs/qpack", "github.com/nukilabs/quic-go", "github.com/nukilabs/socks", "github.com/nukilabs/tlsclient":
		default:
			continue
		}
		raw := runConsumer(t, directory, environment, goTool(), "list", "-mod=readonly", "-m", "-json", pin.original)
		var selected struct {
			Replace *struct{ Path, Version, Sum string }
		}
		if json.Unmarshal(raw, &selected) != nil || selected.Replace == nil || selected.Replace.Path != frameworkModule+"/"+pin.directory || selected.Replace.Version != pin.version || selected.Replace.Sum != pin.sum {
			t.Fatal("actual downloaded HTTP replacement differs from qualified immutable bytes")
		}
		verified++
	}
	if verified != 13 {
		t.Fatal("HTTP replacement policy missing")
	}
}

func verifyTelemetryReplacement(t testing.TB, directory string, environment []string) {
	t.Helper()
	verified := 0
	for _, pin := range sdkPins() {
		if pin.original != "go.opentelemetry.io/otel/sdk/metric" && pin.original != "go.opentelemetry.io/otel/exporters/otlp/otlptrace" {
			continue
		}
		raw := runConsumer(t, directory, environment, goTool(), "list", "-mod=readonly", "-m", "-json", pin.original)
		var selected struct {
			Version string
			Replace *struct{ Path, Version, Sum string }
		}
		if json.Unmarshal(raw, &selected) != nil || selected.Version != "v1.47.0" || selected.Replace == nil ||
			selected.Replace.Path != frameworkModule+"/"+pin.directory || selected.Replace.Version != pin.version || selected.Replace.Sum != pin.sum {
			t.Fatal("actual downloaded telemetry replacement differs from qualified immutable bytes", pin.original)
		}
		verified++
	}
	if verified != 2 {
		t.Fatal("telemetry SDK replacement policy missing")
	}
	for _, path := range []string{"go.opentelemetry.io/otel", "go.opentelemetry.io/otel/trace", "go.opentelemetry.io/otel/metric", "go.opentelemetry.io/otel/sdk", "go.opentelemetry.io/otel/log", "go.opentelemetry.io/otel/sdk/log", "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp", "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp", "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"} {
		var original struct {
			Version string
			Replace any
		}
		raw := runConsumer(t, directory, environment, goTool(), "list", "-mod=readonly", "-m", "-json", path)
		want := "v1.47.0"
		if strings.HasSuffix(path, "/otlploghttp") {
			want = "v0.23.0"
		}
		if json.Unmarshal(raw, &original) != nil || original.Version != want || original.Replace != nil {
			t.Fatalf("selected coordinated telemetry module changed: %s", path)
		}
	}
}
