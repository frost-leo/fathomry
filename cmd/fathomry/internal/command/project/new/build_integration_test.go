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
	old := []byte("cli.Project{Name: \"demo\"}")
	if bytes.Count(source, old) != 1 {
		t.Fatal("project assembly fixture drifted")
	}
	source = bytes.Replace(source, old, []byte("cli.Project{Name: \"demo\", Declarations: []wf.Declaration{health.Declarations()}}"), 1)
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
