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

package create_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func runTool(t *testing.T, directory string, environment []string, executable string, args ...string) ([]byte, error) {
	t.Helper()
	budget := 3 * time.Minute
	if len(args) >= 2 && args[0] == "mod" && args[1] == "tidy" && !slices.Contains(args, "-diff") {
		budget = 5 * time.Minute
	}
	deadline := time.Now().Add(budget)
	if outer, ok := t.Deadline(); ok && outer.Add(-10*time.Second).Before(deadline) {
		deadline = outer.Add(-10 * time.Second)
	}
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = directory
	command.Env = environment
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = 5 * time.Second
	started := time.Now()
	output, err := command.CombinedOutput()
	t.Logf("phase %s %v: %s", filepath.Base(executable), args, time.Since(started).Round(time.Millisecond))
	if ctx.Err() != nil {
		t.Fatalf("bounded command timed out: %s %v: %v\n%s", executable, args, ctx.Err(), output)
	}
	return output, err
}

func requireTool(t *testing.T, directory string, environment []string, executable string, args ...string) []byte {
	t.Helper()
	output, err := runTool(t, directory, environment, executable, args...)
	if err != nil {
		t.Fatalf("command %s %v failed: %v\n%s", executable, args, err, output)
	}
	return output
}

func TestActualGeneratedPublishedConsumer(t *testing.T) {
	testActualGeneratedConsumer(t, false)
}

func TestActualGeneratedCandidateConsumer(t *testing.T) {
	testActualGeneratedConsumer(t, true)
}

func testActualGeneratedConsumer(t *testing.T, candidate bool) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Fatal("this native acceptance profile requires Linux")
	}
	goTool := filepath.Join(runtime.GOROOT(), "bin", "go")
	var environment []string
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "FATHOMRY_") {
			environment = append(environment, value)
		}
	}
	environment = append(environment, "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=")
	moduleFile := strings.TrimSpace(string(requireTool(t, ".", environment, goTool, "env", "GOMOD")))
	product := filepath.Dir(moduleFile)
	job := t.TempDir()
	tool := filepath.Join(job, "fathomry")
	requireTool(t, product, environment, goTool, "build", "-mod=readonly", "-o", tool, "./cmd/fathomry")
	candidateDeps := string(requireTool(t, product, environment, goTool, "list", "-mod=readonly", "-deps", "./cmd/fathomry/v1"))
	if strings.Contains(candidateDeps, "/internal/command/project") {
		t.Fatal("public CLI imported developer generation")
	}
	parent := filepath.Join(job, "generation")
	noTools := filepath.Join(job, "no-tools")
	for _, path := range []string{parent, noTools} {
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatal(err)
		}
	}
	noToolEnvironment := []string{"PATH=" + noTools, "HOME=" + job, "FATHOMRY_LANG=zh-CN"}
	pin := "v0.0.0-20261031064010-94997fff92da"
	raw := requireTool(t, parent, noToolEnvironment, tool, "--output=json", "project", "new",
		"--name=ordersync", "--module=example.com/generated/app", "--framework-version="+pin)
	var result struct {
		Data struct {
			Target      string   `json:"target"`
			State       string   `json:"state"`
			Directories []string `json:"directories"`
			Files       []struct {
				Path         string
				Complete     bool
				BytesWritten int64 `json:"bytes_written"`
			} `json:"files"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &result) != nil || result.Data.Target != "ordersync" || result.Data.State != "complete" || len(result.Data.Files) != 15 || len(result.Data.Directories) != 6 {
		t.Fatal("creation report invalid", string(raw))
	}
	consumer := filepath.Join(parent, "ordersync")
	var paths []string
	err := filepath.WalkDir(consumer, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			relative, err := filepath.Rel(consumer, path)
			if err != nil {
				return err
			}
			paths = append(paths, filepath.ToSlash(relative))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		".env", ".env.example", ".gitignore", "README.md", "config/base.yaml",
		"config/environment/development.yaml", "config/environment/production.yaml", "go.mod",
		"internal/bootstrap/bootstrap.go", "internal/bootstrap/bootstrap_test.go", "internal/setting/application.go",
		"internal/setting/config.go", "internal/setting/config_test.go", "main.go", "main_test.go",
	}
	if !slices.Equal(paths, want) {
		t.Fatal("unexpected initial tree", paths)
	}
	for _, file := range result.Data.Files {
		data, err := os.ReadFile(filepath.Join(consumer, filepath.FromSlash(file.Path)))
		if err != nil || !file.Complete || file.BytesWritten != int64(len(data)) {
			t.Fatal("reported bytes/completion differ", file.Path, err)
		}
	}
	initial, err := os.ReadFile(filepath.Join(consumer, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(initial, []byte("module example.com/generated/app\n")) ||
		!bytes.Contains(initial, []byte("go 1.27.0\n")) ||
		!bytes.Contains(initial, []byte("require github.com/frost-leo/fathomry "+pin+"\n")) ||
		bytes.Contains(initial, []byte("replace")) {
		t.Fatal("generated module differs", string(initial))
	}
	consumerEnvironment := append(slices.Clone(environment), "GOFLAGS=-modcacherw")
	cache := filepath.Join(job, "modules")
	if !candidate {
		if err := os.Mkdir(cache, 0755); err != nil {
			t.Fatal(err)
		}
		consumerEnvironment = append(consumerEnvironment, "GOMODCACHE="+cache)
	}
	offline := append(slices.Clone(consumerEnvironment), "GOPROXY=off")
	rejected, err := runTool(t, consumer, offline, goTool, "test", "-mod=readonly", "./...")
	if err == nil || !bytes.Contains(rejected, []byte("missing go.sum entry")) ||
		!bytes.Contains(rejected, []byte("github.com/frost-leo/fathomry/cmd/fathomry/v1")) {
		t.Fatal("pre-resolution control failed for wrong reason", err, string(rejected))
	}
	if candidate {
		requireTool(t, consumer, environment, goTool, "mod", "edit", "-replace=github.com/frost-leo/fathomry="+product)
	}
	requireTool(t, consumer, consumerEnvironment, goTool, "mod", "tidy")
	if !candidate {
		// Tidy need not fetch .info metadata required by offline module inspection.
		requireTool(t, consumer, consumerEnvironment, goTool, "mod", "download", "-json", "github.com/frost-leo/fathomry@"+pin)
	}
	selected := strings.TrimSpace(string(requireTool(t, consumer, offline, goTool, "list", "-mod=readonly", "-m", "-f={{.Version}}", "github.com/frost-leo/fathomry")))
	if selected != pin {
		t.Fatal("dependency resolution changed the explicit pin", selected)
	}
	requireTool(t, consumer, offline, goTool, "mod", "tidy", "-diff")
	requireTool(t, consumer, offline, goTool, "vet", "-mod=readonly", "./...")
	dotenv := filepath.Join(consumer, ".env")
	if err := os.Rename(dotenv, dotenv+"-private"); err != nil {
		t.Fatal(err)
	}
	requireTool(t, consumer, offline, goTool, "test", "-mod=readonly", "-race", "-count=1", "./...")
	if err := os.Rename(dotenv+"-private", dotenv); err != nil {
		t.Fatal(err)
	}
	applicationBin := filepath.Join(job, "application-bin")
	if err := os.Mkdir(applicationBin, 0755); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(applicationBin, "fathomry")
	requireTool(t, consumer, offline, goTool, "build", "-mod=readonly", "-o", binary, ".")
	sourceHelp := requireTool(t, consumer, offline, goTool, "run", "-mod=readonly", ".", "--lang=zh-CN", "--output=json", "help")
	sourceResult := requireTool(t, consumer, offline, goTool, "run", "-mod=readonly", ".", "config", "check", "--lang=en", "--output=json")
	assertConfiguration(t, sourceResult, "development", false)
	provenance := string(requireTool(t, job, environment, goTool, "version", "-m", binary))
	if !strings.Contains(provenance, "github.com/frost-leo/fathomry\t"+pin) || strings.Contains(provenance, "\t=>") != candidate {
		t.Fatal("unqualified dependency", provenance)
	}
	deps := string(requireTool(t, consumer, offline, goTool, "list", "-mod=readonly", "-deps", "."))
	for _, path := range strings.Fields(deps) {
		for _, forbidden := range []string{
			"github.com/frost-leo/fathomry/schema/", "github.com/frost-leo/fathomry/adapters/workflow/",
			"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project",
		} {
			if strings.HasPrefix(path, forbidden) {
				t.Fatal("unexpected consumer dependency", path)
			}
		}
	}
	for _, required := range []string{"github.com/frost-leo/fathomry/framework/configuration/v1", "example.com/generated/app/internal/setting", "example.com/generated/app/internal/bootstrap"} {
		if !slices.Contains(strings.Fields(deps), required) {
			t.Fatal("project configuration composition absent", required)
		}
	}
	schemaPath := filepath.Join(consumer, "internal/setting/config.go")
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Replace(schema, []byte("return errors.Join(value.Application.Validate(), ctx.Err())"), []byte(`return errors.New("consumer-validation-control")`), 1)
	if bytes.Equal(schema, changed) {
		t.Fatal("validator rejecting control was not applied")
	}
	if err := os.WriteFile(schemaPath, changed, 0600); err != nil {
		t.Fatal(err)
	}
	rejected, err = runTool(t, consumer, offline, goTool, "run", "-mod=readonly", ".", "config", "check", "--output=json")
	if err == nil || !bytes.Contains(rejected, []byte("identifier")) || bytes.Contains(rejected, []byte(`"accepted":true`)) {
		t.Fatal("changed project validator did not reject", err, string(rejected))
	}
	if err := os.WriteFile(schemaPath, schema, 0600); err != nil {
		t.Fatal(err)
	}
	required := bytes.Replace(schema, []byte("return errors.Join(value.Application.Validate(), ctx.Err())"),
		[]byte("if value.Application.Namespace != \"commerce\" { return errors.New(\"required namespace missing\") }; return errors.Join(value.Application.Validate(), ctx.Err())"), 1)
	if bytes.Equal(required, schema) {
		t.Fatal("required-field control was not applied")
	}
	basePath := filepath.Join(consumer, "config/base.yaml")
	base, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(schemaPath, required, 0600); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "commerce", "different"} {
		updated := bytes.Replace(base, []byte("namespace: \"\""), []byte("namespace: \""+value+"\""), 1)
		if value != "" && bytes.Equal(updated, base) {
			t.Fatal("source control was not applied")
		}
		if err := os.WriteFile(basePath, updated, 0600); err != nil {
			t.Fatal(err)
		}
		output, err := runTool(t, consumer, offline, goTool, "run", "-mod=readonly", ".", "config", "check", "--output=json")
		if value == "commerce" {
			if err != nil {
				t.Fatal("valid source-provided requirement rejected before merge", err, string(output))
			}
			assertConfiguration(t, output, "development", false)
		} else if err == nil || bytes.Contains(output, []byte(`"accepted":true`)) {
			t.Fatal("missing or invalid required value admitted", err, string(output))
		}
	}
	if err := os.WriteFile(schemaPath, schema, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(basePath, base, 0600); err != nil {
		t.Fatal(err)
	}
	runtimeDir := filepath.Join(job, "runtime")
	if err := os.Mkdir(runtimeDir, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".env", "config"} {
		if err := os.Rename(filepath.Join(consumer, name), filepath.Join(runtimeDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	moved := filepath.Join(job, "moved-source")
	if err := os.Rename(consumer, moved); err != nil {
		t.Fatal(err)
	}
	if !candidate {
		if err := os.Rename(cache, cache+"-moved"); err != nil {
			t.Fatal(err)
		}
	}
	isolated := filepath.Join(job, "standalone")
	if err := os.Mkdir(isolated, 0755); err != nil {
		t.Fatal(err)
	}
	binaryHelp := requireTool(t, isolated, noToolEnvironment, binary, "--lang=zh-CN", "--output=json", "help")
	if !bytes.Equal(sourceHelp, binaryHelp) {
		t.Fatal("successful source/binary help differs")
	}
	arguments := []string{"config", "check", "--bootstrap", filepath.Join(runtimeDir, ".env"), "--lang=en", "--output=json"}
	binaryResult := requireTool(t, isolated, noToolEnvironment, binary, arguments...)
	if !bytes.Equal(sourceResult, binaryResult) {
		t.Fatal("successful source/binary configuration differs", string(sourceResult), string(binaryResult))
	}
	watchEnvironment := append(slices.Clone(noToolEnvironment), "FATHOMRY_CONFIG_WATCH=true")
	assertConfiguration(t, requireTool(t, isolated, watchEnvironment, binary, arguments...), "development", true)
	production := append(slices.Clone(arguments), "--env=production")
	assertConfiguration(t, requireTool(t, isolated, noToolEnvironment, binary, production...), "production", false)
	boundaryEnvironment := strings.Repeat("e", 64)
	if err := os.WriteFile(filepath.Join(runtimeDir, "config/environment", boundaryEnvironment+".yaml"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	boundary := append(slices.Clone(arguments), "--env="+boundaryEnvironment)
	assertConfiguration(t, requireTool(t, isolated, noToolEnvironment, binary, boundary...), boundaryEnvironment, false)
	assertStartupSignal(t, isolated, noToolEnvironment, binary, arguments)
	assertDeploymentInputs(t, product, isolated, noToolEnvironment, binary)
	if err := os.Remove(filepath.Join(runtimeDir, "config/environment/development.yaml")); err != nil {
		t.Fatal(err)
	}
	missing, err := runTool(t, isolated, noToolEnvironment, binary, arguments...)
	var missingExit *exec.ExitError
	if !errors.As(err, &missingExit) || missingExit.ExitCode() == 0 || !json.Valid(missing) || bytes.Contains(missing, []byte(runtimeDir)) {
		t.Fatal("missing configuration did not fail safely", err, string(missing))
	}
	refused, err := runTool(t, isolated, noToolEnvironment, binary, "--output=json", "project", "new")
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 2 || !json.Valid(refused) {
		t.Fatal("project shell admitted generator", err, string(refused))
	}
	if candidate {
		t.Log("actual root-entry generation and candidate-source checks passed; test-only replacement is NOT published dependency qualification")
	} else {
		t.Log("actual root-entry generation, cold published resolution, offline checks and standalone configuration passed")
	}
}

func assertConfiguration(t *testing.T, raw []byte, environment string, watch bool) {
	t.Helper()
	var result struct {
		Data struct {
			Accepted    bool   `json:"accepted"`
			Application string `json:"application"`
			Environment string `json:"environment"`
			Watching    bool   `json:"watching"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &result) != nil || !result.Data.Accepted || result.Data.Application != "ordersync" || result.Data.Environment != environment || result.Data.Watching != watch {
		t.Fatal("unexpected configuration report", string(raw))
	}
}

func assertStartupSignal(t *testing.T, directory string, environment []string, binary string, arguments []string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	address := listener.Addr().String()
	settings := `{"name":"signal-test","servers":[{"http_url":"http://` + address + `/nacos","grpc_address":"` + address + `"}],"keys":[{"data_id":"main"}],"allow_insecure":true}`
	environment = append(slices.Clone(environment), "FATHOMRY_CONFIG_MODE=remote", "FATHOMRY_CONFIG_PROVIDER=nacos",
		`FATHOMRY_CONFIG_DOCUMENTS_JSON=[{"kind":"base","encoding":"yaml"}]`, "FATHOMRY_NACOS_SETTINGS_JSON="+settings)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	process := exec.CommandContext(ctx, binary, arguments...)
	process.Dir, process.Env = directory, environment
	process.WaitDelay = time.Second
	var output, diagnostics bytes.Buffer
	process.Stdout, process.Stderr = &output, &diagnostics
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if process.ProcessState == nil {
			_ = process.Process.Kill()
			_ = process.Wait()
		}
	}()
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal("generated process never reached admitted remote startup", err)
	}
	defer connection.Close()
	if err := process.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err = process.Wait()
	var exited *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exited) || output.Len() != 0 || !json.Valid(diagnostics.Bytes()) {
		t.Fatal("generated SIGTERM startup failed", err, diagnostics.String())
	}
	var diagnostic struct {
		Identifier string `json:"identifier"`
	}
	if err := json.Unmarshal(diagnostics.Bytes(), &diagnostic); err != nil {
		t.Fatal(err)
	}
	if exited.ExitCode() == 1 {
		if diagnostic.Identifier != "fathomry.command_line.cleanup_failed" {
			t.Fatal("unexpected failure precedence", diagnostics.String())
		}
	} else if exited.ExitCode() != 130 || diagnostic.Identifier != "fathomry.command_line.canceled" {
		t.Fatal("signal bypassed the shared exit policy", exited.ExitCode(), diagnostics.String())
	}
	for _, private := range []string{directory, address, "signal-test"} {
		if strings.Contains(diagnostics.String(), private) {
			t.Fatal("signal cleanup or privacy failed", diagnostics.String())
		}
	}
	t.Logf("generated SIGTERM during remote startup: exit=%d identifier=%s; retained source/cleanup errors take precedence over cancellation", exited.ExitCode(), diagnostic.Identifier)
}

func assertDeploymentInputs(t *testing.T, product, directory string, environment []string, binary string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(product, "docs/development/project-deployment/config-check-job.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var config struct {
		APIVersion string            `yaml:"apiVersion"`
		Kind       string            `yaml:"kind"`
		Immutable  bool              `yaml:"immutable"`
		Data       map[string]string `yaml:"data"`
	}
	if err := decoder.Decode(&config); err != nil {
		t.Fatal(err)
	}
	var job map[string]any
	if err := decoder.Decode(&job); err != nil {
		t.Fatal(err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatal("unexpected extra deployment document", err)
	}
	if config.APIVersion != "v1" || config.Kind != "ConfigMap" || !config.Immutable || job["apiVersion"] != "batch/v1" || job["kind"] != "Job" {
		t.Fatal("example is not an immutable configuration-check Job")
	}
	spec := job["spec"].(map[string]any)
	pod := spec["template"].(map[string]any)["spec"].(map[string]any)
	if spec["backoffLimit"] != 0 || spec["activeDeadlineSeconds"] != 120 || pod["restartPolicy"] != "Never" ||
		pod["automountServiceAccountToken"] != false || pod["terminationGracePeriodSeconds"].(int) <= 60 {
		t.Fatal("unbounded or privileged example lifecycle")
	}
	security := pod["securityContext"].(map[string]any)
	if security["runAsNonRoot"] != true || security["runAsUser"].(int) == 0 {
		t.Fatal("root Job example")
	}
	container := pod["containers"].([]any)[0].(map[string]any)
	security = container["securityContext"].(map[string]any)
	if security["readOnlyRootFilesystem"] != true || security["allowPrivilegeEscalation"] != false {
		t.Fatal("mutable/privileged container example")
	}
	arguments := []string{}
	for _, value := range container["args"].([]any) {
		arguments = append(arguments, value.(string))
	}
	if !slices.Equal(arguments, []string{"config", "check", "--bootstrap", "/run/fathomry/.env", "--output=json"}) {
		t.Fatal("Job bypasses the configuration command", arguments)
	}
	configuration := pod["volumes"].([]any)[0].(map[string]any)["configMap"].(map[string]any)
	if configuration["defaultMode"] != 0444 {
		t.Fatal("unexpected projected file permissions")
	}
	projected := filepath.Join(directory, "projected")
	for _, item := range configuration["items"].([]any) {
		entry := item.(map[string]any)
		data, present := config.Data[entry["key"].(string)]
		if !present {
			t.Fatal("projected key is missing")
		}
		path := entry["path"].(string)
		if !filepath.IsLocal(path) {
			t.Fatal("invalid projection path")
		}
		target := filepath.Join(projected, path)
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(data), 0400); err != nil {
			t.Fatal(err)
		}
	}
	arguments[3] = filepath.Join(projected, ".env")
	assertConfiguration(t, requireTool(t, directory, environment, binary, arguments...), "production", false)
	t.Log("Job structure and projected file inputs passed; no Kubernetes cluster was contacted")
}
