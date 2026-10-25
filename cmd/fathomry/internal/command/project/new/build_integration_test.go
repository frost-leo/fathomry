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

package newcmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestProjectBuildCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("generated project and actual CLI qualification")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("native build host not supported")
	}
	tree := testPlan(t, "local", "yaml")
	if err := create(context.Background(), tree, writeProjectFile); err != nil {
		t.Fatal(err)
	}
	project := tree.destination
	healthDir := filepath.Join(project, "internal", "health")
	if err := os.Mkdir(healthDir, 0700); err != nil {
		t.Fatal(err)
	}
	declaration, err := os.ReadFile(filepath.Join(repository(t), "framework/workflow/declaration/v1/testdata/health/declaration.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(healthDir, "declaration.go"), declaration, 0600); err != nil {
		t.Fatal(err)
	}
	mainPath := filepath.Join(project, "main.go")
	source, err := os.ReadFile(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	source = bytes.Replace(source, []byte("import ("), []byte("import (\n\"example.org/demo/internal/health\"\nwf \"github.com/frost-leo/fathomry/framework/workflow/declaration/v1\""), 1)
	old := []byte("cli.Project{Name: \"demo\", Control: bootstrap.Control}")
	if bytes.Count(source, old) != 1 {
		t.Fatal("project assembly fixture drifted")
	}
	source = bytes.Replace(source, old, []byte("cli.Project{Name: \"demo\", Control: bootstrap.Control, Declarations: []wf.Declaration{health.Declarations()}}"), 1)
	if err := os.WriteFile(mainPath, source, 0600); err != nil {
		t.Fatal(err)
	}
	environment := consumerEnvironment()
	runConsumer(t, project, environment, goTool(), "mod", "tidy")
	job := t.TempDir()
	cli := filepath.Join(job, "fathomry")
	runConsumerBudget(t, 3*time.Minute, repository(t), environment, goTool(), "build", "-mod=readonly", "-o", cli, "./cmd/fathomry")
	first, second := filepath.Join(job, "official"), filepath.Join(job, "local")
	if err := os.Rename(filepath.Join(project, "configs"), filepath.Join(project, "offline-configs")); err != nil {
		t.Fatal(err)
	}
	var expected [32]byte
	for index, invocation := range []struct {
		name     string
		args     []string
		artifact string
	}{
		{cli, []string{"build", project, "--out", first, "--output=json"}, first},
		{goTool(), []string{"run", ".", "build", "--out", second, "--output=json"}, second},
	} {
		raw := runConsumerBudget(t, 3*time.Minute, project, environment, invocation.name, invocation.args...)
		var response struct {
			Schema  string `json:"schema"`
			Command string `json:"command"`
			Data    struct {
				Published bool   `json:"published"`
				Workflows int    `json:"workflows"`
				Modes     int    `json:"modes"`
				SHA256    string `json:"sha256"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &response) != nil || response.Schema != "fathomry.cli/v1" || response.Command != "build" || !response.Data.Published || response.Data.Workflows != 1 || response.Data.Modes != 2 {
			t.Fatal("incorrect build result", string(raw))
		}
		binary, err := os.ReadFile(invocation.artifact)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(binary)
		if index == 0 {
			expected = digest
		} else if digest != expected {
			t.Fatal("two commands produced different executables")
		}
		runtimeReport := runConsumer(t, project, environment, invocation.artifact, "--config-dir", filepath.Join(project, "offline-configs"))
		if !bytes.Contains(runtimeReport, []byte(`"configuration_accepted":true`)) {
			t.Fatal("artifact did not run", string(runtimeReport))
		}
	}
	raw := runConsumer(t, project, environment, first, "--fathomry-project=v1")
	var exported struct {
		Bundles []json.RawMessage `json:"bundles"`
	}
	if json.Unmarshal(raw, &exported) != nil || len(exported.Bundles) != 1 {
		t.Fatal("missing declarations")
	}
	bundle := filepath.Join(job, "bundle.json")
	if err := os.WriteFile(bundle, exported.Bundles[0], 0600); err != nil {
		t.Fatal(err)
	}
	official := runConsumer(t, project, environment, cli, "workflow", "vet", bundle, "--output=json")
	local := runConsumer(t, project, environment, goTool(), "run", ".", "workflow", "vet", bundle, "--output=json")
	if !bytes.Equal(official, local) {
		t.Fatal("vet commands diverged")
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Minute)
	closedOutput := exec.CommandContext(ctx, first, "workflow", "vet", bundle, "--output=json")
	closedOutput.Dir, closedOutput.Env, closedOutput.Stdout = project, environment, writer
	var pipeDiagnostic bytes.Buffer
	closedOutput.Stderr = &pipeDiagnostic
	err = closedOutput.Run()
	stop()
	if problem := writer.Close(); problem != nil {
		t.Fatal(problem)
	}
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 1 || pipeDiagnostic.Len() == 0 {
		t.Fatal("generated CLI bypassed output-error cleanup through SIGPIPE", err)
	}
	invalid := bytes.Replace(declaration, []byte(`wf.Decimal("1000")`), []byte(`wf.Decimal("9999")`), 1)
	if bytes.Equal(invalid, declaration) {
		t.Fatal("invalid mode fixture not changed")
	}
	if err := os.WriteFile(filepath.Join(healthDir, "declaration.go"), invalid, 0600); err != nil {
		t.Fatal(err)
	}
	runConsumer(t, project, environment, goTool(), "build", "-mod=readonly", "-o", filepath.Join(job, "unchecked"), ".")
	for _, invocation := range []struct {
		name     string
		args     []string
		artifact string
	}{
		{cli, []string{"build", project, "--out", first, "--output=json"}, first},
		{goTool(), []string{"run", ".", "build", "--out", second, "--output=json"}, second},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		process := exec.CommandContext(ctx, invocation.name, invocation.args...)
		process.Dir, process.Env = project, environment
		var output, diagnostic bytes.Buffer
		process.Stdout, process.Stderr = &output, &diagnostic
		err := process.Run()
		cancel()
		if err == nil || output.Len() != 0 || diagnostic.Len() == 0 {
			t.Fatal("invalid declaration bypassed build", err, output.String())
		}
		binary, err := os.ReadFile(invocation.artifact)
		if err != nil || sha256.Sum256(binary) != expected {
			t.Fatal("failed build replaced prior artifact", err)
		}
		if strings.Contains(diagnostic.String(), "9999") {
			t.Fatal("raw declaration leaked")
		}
	}
}

func TestGeneratedProjectSelectiveVetAndMetadataGate(t *testing.T) {
	if testing.Short() {
		t.Skip("native generated-project validation")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("native project host not supported")
	}
	tree := testPlan(t, "local", "yaml")
	if err := create(context.Background(), tree, writeProjectFile); err != nil {
		t.Fatal(err)
	}
	directory := tree.destination
	healthDirectory := filepath.Join(directory, "workflows", "health_check")
	if err := os.MkdirAll(healthDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(repository(t), "framework/workflow/declaration/v1/testdata/health/declaration.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := bytes.Replace(raw, []byte("import wf "), []byte("import catalog \"github.com/frost-leo/fathomry/framework/workflow/catalog/v1\"\nimport wf "), 1)
	source = append(source, []byte(`
func Secondary() wf.Declaration {
 value:=Declarations()
 value.Workflows[0].Code="secondary"
 value.Workflows[0].Modes[0].Values=map[string]wf.Decimal{"attempt_timeout": "1000"}
 return value
}
func Metadata() *catalog.Metadata {
 return &catalog.Metadata{Resources:[]catalog.Resource{
  {Target:catalog.Target{Kind:catalog.Definition},Field:catalog.Name,Text:catalog.Text{English:"Health"}},
  {Target:catalog.Target{Kind:catalog.Mode,Version:"1",Mode:"standard"},Field:catalog.Name,Text:catalog.Text{English:"Standard"}},
  {Target:catalog.Target{Kind:catalog.Mode,Version:"1",Mode:"patient"},Field:catalog.Name,Text:catalog.Text{English:"Patient"}},
 }}
}
func SecondaryMetadata() *catalog.Metadata {
 value:=Metadata()
 value.Resources[0].Text.English="Secondary"
 return value
}
`)...)
	healthPath := filepath.Join(healthDirectory, "declaration.go")
	if err := os.WriteFile(healthPath, source, 0600); err != nil {
		t.Fatal(err)
	}
	mainPath := filepath.Join(directory, "main.go")
	main, err := os.ReadFile(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	main = bytes.Replace(main, []byte("import ("), []byte("import (\nhealth \"example.org/demo/workflows/health_check\""), 1)
	previous := []byte("cli.Project{Name: \"demo\", Control: bootstrap.Control}")
	if bytes.Count(main, previous) != 1 {
		t.Fatal("project registration fixture drift")
	}
	main = bytes.Replace(main, previous, []byte(`cli.Project{Name:"demo",Control:bootstrap.Control,Workflows:[]cli.Workflow{
  {Declaration:health.Declarations(),Metadata:health.Metadata()},
  {Declaration:health.Secondary(),Metadata:health.SecondaryMetadata()},
 }}`), 1)
	if err := os.WriteFile(mainPath, main, 0600); err != nil {
		t.Fatal(err)
	}
	environment := consumerEnvironment()
	runConsumer(t, directory, environment, goTool(), "mod", "tidy")
	runConsumer(t, directory, environment, goTool(), "test", "-race", "./...")
	if err := os.Rename(filepath.Join(directory, "configs"), filepath.Join(directory, "offline-configs")); err != nil {
		t.Fatal(err)
	}
	job := t.TempDir()
	official, artifact := filepath.Join(job, "fathomry"), filepath.Join(job, "application")
	runConsumerBudget(t, 3*time.Minute, repository(t), environment, goTool(), "build", "-mod=readonly", "-o", official, "./cmd/fathomry")
	runConsumerBudget(t, 3*time.Minute, directory, environment, official, "build", directory, "--out", artifact, "--output=json")
	initial, err := os.ReadFile(artifact)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(initial)

	invoke := func(expect bool, args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		process := exec.CommandContext(ctx, goTool(), append([]string{"run", "."}, args...)...)
		process.Dir, process.Env = directory, environment
		var output, diagnostics bytes.Buffer
		process.Stdout, process.Stderr = &output, &diagnostics
		err := process.Run()
		if ctx.Err() != nil || (err == nil) != expect {
			t.Fatalf("project command %v: %v\n%s", args, err, diagnostics.String())
		}
		return output.Bytes()
	}
	check := func(raw []byte, passed, full bool, selected int) {
		t.Helper()
		var report struct {
			Data struct {
				Format   string
				Passed   bool
				Full     bool `json:"full_project"`
				Selected int  `json:"selected_workflows"`
			}
		}
		if json.Unmarshal(raw, &report) != nil || report.Data.Format != "fathomry.project-workflow-check/v2" || report.Data.Passed != passed || report.Data.Full != full || report.Data.Selected != selected {
			t.Fatal("wrong project scope", string(raw))
		}
	}
	check(invoke(true, "workflow", "vet", "--output=json"), true, true, 2)
	check(invoke(true, "workflow", "vet", "--workflow=server-health", "--version=1", "--mode=patient", "--revision=1", "--output=json"), true, false, 1)
	help := invoke(true, "workflow", "vet", "--help", "--lang=zh-CN")
	if !bytes.Contains(help, []byte("--workflow")) || bytes.Contains(help, []byte("fathomry.workflow_vet.flag_")) {
		t.Fatal("selector help not localized")
	}
	for _, invalid := range [][]byte{
		bytes.Replace(source, []byte(`"attempt_timeout": "1000"`), []byte(`"attempt_timeout": "9999"`), 1),
		bytes.Replace(source, []byte(`value.Resources[0].Text.English="Secondary"`), []byte(`value.Resources[0].Text.English=""`), 1),
	} {
		if bytes.Equal(source, invalid) {
			t.Fatal("rejection fixture unchanged")
		}
		if err := os.WriteFile(healthPath, invalid, 0600); err != nil {
			t.Fatal(err)
		}
		check(invoke(true, "workflow", "vet", "--workflow=server-health", "--output=json"), true, false, 1)
		check(invoke(false, "workflow", "vet", "--output=json"), false, true, 2)
		invoke(false, "build", "--out", artifact, "--output=json")
		current, err := os.ReadFile(artifact)
		if err != nil || sha256.Sum256(current) != digest {
			t.Fatal("partial vet bypassed full artifact admission", err)
		}
	}
	if err := os.WriteFile(healthPath, source, 0600); err != nil {
		t.Fatal(err)
	}
	raw = runConsumer(t, directory, environment, artifact, "--fathomry-project=v1")
	var inspection struct{ Bundles []json.RawMessage }
	if json.Unmarshal(raw, &inspection) != nil || len(inspection.Bundles) != 2 {
		t.Fatal("full inspection lost the registered set")
	}
	bundlePath := filepath.Join(job, "bundle.json")
	if err := os.WriteFile(bundlePath, inspection.Bundles[0], 0600); err != nil {
		t.Fatal(err)
	}
	raw = runConsumer(t, directory, environment, official, "workflow", "vet", bundlePath, "--output=json")
	if bytes.Contains(raw, []byte("full_project")) || !bytes.Contains(raw, []byte(`"passed":true`)) {
		t.Fatal("explicit Bundle compatibility changed", string(raw))
	}
}
