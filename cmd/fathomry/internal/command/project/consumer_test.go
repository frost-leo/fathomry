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

package project

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/mod/module"
	modulezip "golang.org/x/mod/zip"
)

type consumerHarness struct {
	root        string
	directory   string
	cli         string
	goBinary    string
	environment []string
}

const artifactVersion = "v0.0.0-gh83"

func newConsumerHarness(t *testing.T) *consumerHarness {
	t.Helper()
	root, err := filepath.Abs("../../../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	harness := &consumerHarness{root: root, directory: directory, goBinary: filepath.Join(runtime.GOROOT(), "bin", "go")}
	harness.environment = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=-modcacherw", "GOPROXY=off", "GOSUMDB=off", "GOPRIVATE=", "GONOPROXY=none")
	harness.cli = filepath.Join(directory, "fathomry")
	harness.run(t, root, harness.goBinary, "build", "-o", harness.cli, "./cmd/fathomry")
	archive := frameworkArchive(t, root)
	proxy := filepath.Join(directory, "proxy")
	versions := filepath.Join(proxy, FrameworkModule, "@v")
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(versions, artifactVersion+".mod"), mod)
	put(t, filepath.Join(versions, artifactVersion+".zip"), archive)
	put(t, filepath.Join(versions, artifactVersion+".info"), []byte(`{"Version":"`+artifactVersion+`","Time":"2026-09-20T00:00:00Z"}`))
	put(t, filepath.Join(versions, "list"), []byte(artifactVersion+"\n"))
	parentCache := strings.TrimSpace(harness.run(t, root, harness.goBinary, "env", "GOMODCACHE"))
	harness.environment = append(harness.environment,
		"GOPROXY=file://"+filepath.ToSlash(proxy)+",file://"+filepath.ToSlash(filepath.Join(parentCache, "cache/download")),
		"GOMODCACHE="+filepath.Join(directory, "modules"))
	return harness
}

func frameworkArchive(t *testing.T, root string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	files := strings.Split(strings.TrimSuffix(string(output), "\x00"), "\x00")
	slices.Sort(files)
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
		entry, err := archive.Create(FrameworkModule + "@" + artifactVersion + "/" + filepath.ToSlash(file))
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
	return data.Bytes()
}
func (h *consumerHarness) command(t *testing.T, directory, executable string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = directory
	command.Env = h.environment
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatal("consumer command exceeded its bound", args)
	}
	return string(output), err
}
func (h *consumerHarness) run(t *testing.T, directory, executable string, args ...string) string {
	t.Helper()
	output, err := h.command(t, directory, executable, args...)
	if err != nil {
		t.Fatalf("consumer %v failed: %v\n%s", args, err, output)
	}
	return output
}
func (h *consumerHarness) create(t *testing.T, name, provider, bundle string, extra ...string) string {
	t.Helper()
	kind, implementation := "local", "viper"
	if provider == "nacos" {
		kind, implementation = "remote", "nacos"
	}
	args := []string{"new", name, "--directory", h.directory, "--module", "example.org/" + name,
		"--framework-version", artifactVersion, "--configuration", kind, "--provider", implementation}
	if bundle != "" {
		args = append(args, "--sdk-bundle", bundle)
	}
	args = append(args, extra...)
	h.run(t, h.directory, h.cli, args...)
	return filepath.Join(h.directory, name)
}

func TestGeneratedProjectsConsumeUnreplacedFrameworkArtifact(t *testing.T) {
	h := newConsumerHarness(t)
	for _, provider := range []string{"local", "nacos"} {
		t.Run(provider, func(t *testing.T) {
			project := h.create(t, "sample-"+provider, provider, "")
			if provider == "local" {
				installLocalAcceptance(t, project)
			}
			h.run(t, project, h.goBinary, "test", "-mod=mod", "-race", "-count=1", "-timeout=1m", "./...")
			declarations := strings.Fields(h.run(t, project, h.goBinary, "list", "-mod=readonly", "-deps", "-f", "{{.ImportPath}}", "./internal/resource"))
			for _, imported := range declarations {
				if imported == FrameworkModule+"/framework/configuration" || strings.HasPrefix(imported, FrameworkModule+"/adapters/") || strings.HasPrefix(imported, FrameworkModule+"/internal/configsource/") || strings.HasSuffix(imported, "/internal/configuration") {
					t.Fatal("project declarations depend on loading", imported)
				}
			}
			binary := filepath.Join(project, "project")
			h.run(t, project, h.goBinary, "build", "-race", "-mod=readonly", "-o", binary, "./cmd/sample-"+provider)
			help := h.run(t, h.directory, binary, "--help", "--lang", "zh-CN")
			if !strings.Contains(help, "检查项目配置") {
				t.Fatal("generated help is not localized")
			}
			invalid, err := h.command(t, h.directory, binary, "--root", project, "--environment", "unknown")
			if err == nil || !strings.Contains(invalid, "fathomry.configuration.invalid_input") {
				t.Fatal("unknown environment silently accepted", invalid, err)
			}
			graph := strings.Fields(h.run(t, project, h.goBinary, "list", "-mod=readonly", "-deps", "-f", "{{.ImportPath}}", "./..."))
			chosen := FrameworkModule + "/adapters/configuration/local/viper"
			if provider == "nacos" {
				chosen = FrameworkModule + "/adapters/configuration/remote/nacos"
			}
			found := false
			for _, imported := range graph {
				found = found || imported == chosen
				if strings.HasPrefix(imported, FrameworkModule+"/cmd/") || strings.HasPrefix(imported, FrameworkModule+"/framework/project") ||
					strings.HasPrefix(imported, FrameworkModule+"/internal/orchestration/") ||
					strings.HasPrefix(imported, FrameworkModule+"/internal/database/") {
					t.Fatal("unrelated/private tooling dependency", imported)
				}
				if provider == "nacos" && imported == FrameworkModule+"/adapters/configuration/local/viper" {
					t.Fatal("remote project imported local adapter")
				}
				if provider == "local" && imported == FrameworkModule+"/adapters/configuration/remote/nacos" {
					t.Fatal("local project imported remote adapter")
				}
			}
			if !found {
				t.Fatal("selected adapter missing")
			}
			selected := h.run(t, project, h.goBinary, "list", "-m", "-mod=readonly", "-f", "{{.Version}} {{if .Replace}}replaced{{end}}", FrameworkModule)
			if strings.TrimSpace(selected) != artifactVersion {
				t.Fatal("framework artifact was replaced", selected)
			}
			if provider == "local" {
				for _, environment := range []string{"development", "production"} {
					output := h.run(t, h.directory, binary, "--root", project, "--environment", environment)
					var description struct {
						Environment   string `json:"environment"`
						Configuration struct {
							Provider string `json:"Provider"`
						} `json:"configuration"`
					}
					if json.Unmarshal([]byte(output), &description) != nil || description.Environment != environment || description.Configuration.Provider != "viper" {
						t.Fatal("generated entry did not report chosen environment", output)
					}
				}
				verifyGeneratedDotenvAndLocale(t, h, project, binary)
				h.environment = append(h.environment, "SAMPLE_LOCAL_PROJECT_LABEL=private-canary")
				output := h.run(t, h.directory, binary, "--root", project, "--environment", "development")
				if strings.Contains(output, "private-canary") || strings.Contains(output, "SAMPLE_LOCAL_PROJECT_LABEL") {
					t.Fatal("settings leaked")
				}
			}
			put(t, filepath.Join(project, "forbidden.go"), []byte(goNotice+"package forbidden\nimport _ \""+FrameworkModule+"/cmd/fathomry/internal/command/project\"\n"))
			output, err := h.command(t, project, h.goBinary, "build", "-mod=readonly", "./...")
			if err == nil || !strings.Contains(output, "use of internal package") {
				t.Fatal("consumer imported creation tooling", output, err)
			}
		})
	}
}

func installLocalAcceptance(t *testing.T, project string) {
	t.Helper()
	schemaPath := filepath.Join(project, "internal/resource/configuration.go")
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	extension := "type Config struct {\nItems []string `json:\"items\"`\nOptional *string `json:\"optional\"`\nLabels map[string]string `json:\"labels\"`\nEnabled bool `json:\"enabled\"`\n"
	if !strings.Contains(string(schema), "type Config struct {") {
		t.Fatal("generated schema boundary changed")
	}
	put(t, schemaPath, []byte(strings.Replace(string(schema), "type Config struct {", extension, 1)))
	put(t, filepath.Join(project, "configs/base.yaml"), []byte("i18n: {default_locale: en}\nitems: [base]\noptional: base\nlabels: {keep: base, replace: base}\nenabled: true\n"))
	put(t, filepath.Join(project, "configs/environments/development.yaml"), []byte("i18n: {default_locale: zh-Hans}\nitems: []\noptional: null\nlabels: {replace: development}\nenabled: false\n"))
	source := goNotice + `package configuration
import("context";"errors";"os";"path/filepath";"testing"; "example.org/sample-local/internal/resource"; framework "github.com/frost-leo/fathomry/framework/configuration")
func TestGeneratedLocalEnvironmentValues(t *testing.T){
 root,err:=filepath.Abs("../..");if err!=nil{t.Fatal(err)}
 load:=func(environment string,local bool)(framework.Configuration[resource.Config],error){return Load(context.Background(),Options{Root:root,Environment:environment,Local:local})}
 for _,environment:=range []string{"development","test","production"}{
  loaded,err:=load(environment,false);if err!=nil{t.Fatal(err)}
  value,err:=loaded.Value();if err!=nil{t.Fatal(err)}
  expected:="en";if environment=="development"{expected="zh-Hans"}
  if value.I18n.DefaultLocale!=expected{t.Fatal("wrong environment")}
  if environment=="development" && (value.Items==nil || len(value.Items)!=0 || value.Optional!=nil || value.Enabled || value.Labels["keep"]!="base" || value.Labels["replace"]!="development"){t.Fatal("extended project schema lost overlay semantics")}
 }
 local:=filepath.Join(root,"configs/local/development.yaml")
 if err:=os.WriteFile(local,[]byte("i18n: {default_locale: en-US}\n"),0600);err!=nil{t.Fatal(err)}
 defer os.Remove(local)
 loaded,err:=load("development",false);if err!=nil{t.Fatal(err)}
 value,_:=loaded.Value();if value.I18n.DefaultLocale!="zh-Hans"{t.Fatal("local override was implicit")}
 loaded,err=load("development",true);if err!=nil{t.Fatal(err)}
 value,_=loaded.Value();if value.I18n.DefaultLocale!="en-US"{t.Fatal("local override not selected")}
 loaded,err=load("production",true);if err!=nil{t.Fatal(err)}
 value,_=loaded.Value();if value.I18n.DefaultLocale!="en"{t.Fatal("development override leaked")}
 t.Setenv("SAMPLE_LOCAL_DEVELOPMENT_I18N_DEFAULT_LOCALE","")
 if _,err=load("development",true);!errors.Is(err,framework.ValidationFailed){t.Fatal("empty explicit variable incorrectly fell back")}
 if err:=os.WriteFile(local,[]byte("unknown: private-canary\n"),0600);err!=nil{t.Fatal(err)}
 failed,err:=load("development",true)
 if !errors.Is(err,framework.Invalid){t.Fatal("invalid local input accepted",err)}
 if _,err:=failed.Value();!errors.Is(err,framework.InvalidInput){t.Fatal("failed load retained settings")}
 environmentPath:=filepath.Join(root,"configs/environments/test.yaml")
 original,err:=os.ReadFile(environmentPath);if err!=nil{t.Fatal(err)}
 defer os.WriteFile(environmentPath,original,0600)
 if err:=os.Remove(environmentPath);err!=nil{t.Fatal(err)}
 if _,err:=load("test",false);!errors.Is(err,framework.Unavailable){t.Fatal("missing environment silently inherited")}
}
`
	put(t, filepath.Join(project, "internal/configuration/acceptance_test.go"), []byte(source))
}

func verifyGeneratedDotenvAndLocale(t *testing.T, h *consumerHarness, project, binary string) {
	t.Helper()
	put(t, filepath.Join(project, ".env"), []byte("invalid automatic dotenv"))
	put(t, filepath.Join(project, ".env.selected"), []byte("SAMPLE_LOCAL_DEVELOPMENT_I18N_DEFAULT_LOCALE=en\nUNUSED_PRIVATE=private-canary\n"))
	check := func(environment, locale string, args ...string) {
		t.Helper()
		flags := append([]string{"--root", project, "--environment", environment}, args...)
		output := h.run(t, h.directory, binary, flags...)
		var result struct {
			Locale        string
			Message       string
			Configuration struct{ Variables []struct{ Source string } }
		}
		if json.Unmarshal([]byte(output), &result) != nil || result.Locale != locale || result.Message == "" || strings.Contains(output, "private-canary") {
			t.Fatal("effective localization or privacy failed", output)
		}
		if locale == "zh-Hans" && result.Message != "配置检查通过。" {
			t.Fatal("configuration did not select actual translation")
		}
	}
	check("development", "zh-Hans")
	check("development", "en", "--env-file", ".env.selected")
	check("production", "en", "--env-file", ".env.selected")
	check("development", "zh-Hans", "--env-file", ".env.selected", "--lang", "zh-Hans")
	original := h.environment
	defer func() { h.environment = original }()
	h.environment = append(slices.Clone(original), "SAMPLE_LOCAL_DEVELOPMENT_I18N_DEFAULT_LOCALE=zh-Hans")
	check("development", "zh-Hans", "--env-file", ".env.selected")
	h.environment = append(slices.Clone(original), "SAMPLE_LOCAL_DEVELOPMENT_I18N_DEFAULT_LOCALE=")
	if output, err := h.command(t, h.directory, binary, "--root", project, "--environment", "development", "--env-file", ".env.selected"); err == nil || !strings.Contains(output, "validation_failed") {
		t.Fatal("empty process variable hid behind dotenv fallback", output, err)
	}
	h.environment = original
	for _, file := range []string{".env", "missing-private.env"} {
		if output, err := h.command(t, h.directory, binary, "--root", project, "--environment", "development", "--env-file", file); err == nil || strings.Contains(output, file) {
			t.Fatal("explicit invalid/missing dotenv accepted or disclosed", output, err)
		}
	}
	if output, err := h.command(t, h.directory, binary, "--help", "--env-file", "missing-private.env"); err != nil || strings.Contains(output, "missing-private.env") {
		t.Fatal("help acquired bootstrap inputs", output, err)
	}
}

func TestGeneratedProjectConsumesActualTemporalPatch(t *testing.T) {
	h := newConsumerHarness(t)
	bundle := filepath.Join(h.directory, "bundle")
	if err := os.Mkdir(bundle, 0700); err != nil {
		t.Fatal(err)
	}
	native := filepath.Join(h.root, "third_party/temporal-sdk")
	var archive bytes.Buffer
	if err := modulezip.CreateFromDir(&archive, module.Version{Path: "go.temporal.io/sdk", Version: "v1.49.0"}, native); err != nil {
		t.Fatal(err)
	}
	reader, err := zip.NewReader(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	if err != nil {
		t.Fatal(err)
	}
	entry := BundleModule{Path: "go.temporal.io/sdk", Version: "v1.49.0", Revision: "gh61", Directory: "temporal-sdk", Files: map[string]string{}}
	prefix := "go.temporal.io/sdk@v1.49.0/"
	for _, file := range reader.File {
		relative := strings.TrimPrefix(file.Name, prefix)
		if relative == file.Name || !fs.ValidPath(relative) {
			t.Fatal("bad native archive entry")
		}
		opened, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(opened)
		closeErr := opened.Close()
		if err != nil || closeErr != nil {
			t.Fatal("native archive read failed")
		}
		put(t, filepath.Join(bundle, entry.Directory, relative), data)
		entry.Files[relative] = digest(data)
	}
	manifest := BundleManifest{Format: BundleFormat, FrameworkVersion: artifactVersion, Modules: []BundleModule{entry}}
	saveManifest(t, bundle, manifest)
	project := h.create(t, "native-patch", "local", bundle)
	test := goNotice + `package configuration
import("context";"testing";"go.temporal.io/sdk/worker")
func TestNativePatchABI(t *testing.T){
 var wait func(context.Context,worker.Worker)error=worker.FathomryWaitStoppedV1
 if wait==nil{t.Fatal("patch missing")}
}
`
	put(t, filepath.Join(project, "internal/configuration/native_patch_test.go"), []byte(test))
	h.run(t, project, h.goBinary, "test", "-mod=mod", "-race", "-count=1", "-timeout=2m", "./...")
	h.run(t, project, h.goBinary, "build", "-mod=readonly", "./...")
	selected := h.run(t, project, h.goBinary, "list", "-m", "-f", "{{.Version}} {{.Replace.Path}}", "go.temporal.io/sdk")
	if strings.TrimSpace(selected) != "v1.49.0 ./third_party/fathomry/temporal-sdk" {
		t.Fatal("native patch not selected", selected)
	}
	patch := filepath.Join(project, "third_party/fathomry/temporal-sdk/worker/fathomry_lifecycle.go")
	if err := os.Remove(patch); err != nil {
		t.Fatal(err)
	}
	output, err := h.command(t, project, h.goBinary, "test", "-run", "TestNativePatchABI", "./internal/configuration")
	if err == nil || !strings.Contains(output, "undefined: worker.FathomryWaitStoppedV1") {
		t.Fatal("patch-removal control failed for the wrong reason", output, err)
	}
}
