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

package implementation_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func consumerEnvironment(offline bool) []string {
	result := []string{}
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if name == "GOWORK" || name == "GOTOOLCHAIN" || name == "GOFLAGS" || strings.HasPrefix(name, "FATHOMRY_") {
			continue
		}
		if offline && (name == "GOPROXY" || name == "GOSUMDB") {
			continue
		}
		result = append(result, entry)
	}
	result = append(result, "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=")
	if offline {
		result = append(result, "GOPROXY=off", "GOSUMDB=off")
	}
	return result
}
func TestIndependentProject(t *testing.T) {
	if testing.Short() {
		t.Skip("independent compiler and owned build")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("owned build host profile")
	}
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	official := filepath.Join(work, "fathomry")
	project := filepath.Join(work, "consumer")
	run := func(directory string, offline bool, program string, args ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, program, args...)
		command.Dir = directory
		command.Env = consumerEnvironment(offline)
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("command exceeded bound: %s", filepath.Base(program))
		}
		return output, err
	}
	require := func(directory string, offline bool, program string, args ...string) []byte {
		t.Helper()
		output, err := run(directory, offline, program, args...)
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", filepath.Base(program), args, err, output)
		}
		return output
	}
	goTool := filepath.Join(runtime.GOROOT(), "bin", "go")
	require(root, false, goTool, "build", "-o", official, "./cmd/fathomry")
	require(root, false, official, "new", project, "--module", "example.org/binding-consumer", "--name", "binding-consumer", "--framework-source", root)
	err = filepath.WalkDir("testdata/consumer", func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel("testdata/consumer", path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(raw, []byte("fathomry/internal/")) || bytes.Contains(raw, []byte("fathomry/cmd/fathomry/internal/")) {
			t.Fatal("consumer private import")
		}
		target := filepath.Join(project, relative)
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			return err
		}
		return os.WriteFile(target, raw, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
	require(project, false, goTool, "mod", "tidy")
	require(project, true, goTool, "mod", "tidy", "-diff")
	require(project, true, goTool, "test", "-mod=readonly", "-race", "./workflows/...")
	dependencies := require(project, true, goTool, "list", "-deps", "github.com/frost-leo/fathomry/framework/workflow/implementation/v1")
	for _, forbidden := range []string{"fathomry/internal/", "fathomry/cmd/", "fathomry/adapters/", "go.temporal.io/sdk"} {
		if bytes.Contains(dependencies, []byte(forbidden)) {
			t.Fatal("binding pulled runtime dependency", forbidden)
		}
	}
	require(project, true, goTool, "run", ".", "workflow", "vet")
	buildArtifact := func(path string) []byte {
		t.Helper()
		raw := require(project, true, official, "build", project, "--out", path, "--output=json")
		var receipt struct {
			Schema  string `json:"schema"`
			Command string `json:"command"`
			Data    struct {
				Published  bool   `json:"published"`
				Inspection string `json:"inspection"`
				SHA256     string `json:"sha256"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &receipt); err != nil || receipt.Schema != "fathomry.cli/v1" || receipt.Command != "build" || !receipt.Data.Published || receipt.Data.Inspection != "fathomry.project-inspection/v2" {
			t.Fatal("invalid build receipt", err)
		}
		artifact, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(artifact)
		if receipt.Data.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatal("receipt digest differs from actual executable bytes")
		}
		return artifact
	}
	artifactA := filepath.Join(work, "artifact-a")
	bytesA := buildArtifact(artifactA)
	first := require(project, true, artifactA, "--fathomry-project=v2")
	var inspection struct {
		Schema          string `json:"schema"`
		Implementations []struct {
			Status           string `json:"status"`
			SuppliedBindings int    `json:"supplied_bindings"`
		} `json:"implementations"`
	}
	if err := json.Unmarshal(first, &inspection); err != nil || inspection.Schema != "fathomry.project-inspection/v2" || len(inspection.Implementations) != 1 || inspection.Implementations[0].Status != "complete" || inspection.Implementations[0].SuppliedBindings != 3 {
		t.Fatalf("bad inspection: %v", err)
	}
	value := require(project, true, artifactA)
	if !bytes.Contains(value, []byte("42")) {
		t.Fatal("actual callable not included")
	}
	source := filepath.Join(project, "workflows/example/server_health/v1/nodes/sample/cpu.go")
	original, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Replace(original, []byte("Value: 42"), []byte("Value: 43"), 1)
	if bytes.Equal(original, changed) {
		t.Fatal("fixture replacement missing")
	}
	if err := os.WriteFile(source, changed, 0600); err != nil {
		t.Fatal(err)
	}
	artifactB := filepath.Join(work, "artifact-b")
	bytesB := buildArtifact(artifactB)
	second := require(project, true, artifactB, "--fathomry-project=v2")
	if !bytes.Equal(first, second) {
		t.Fatal("semantic code change altered static manifest")
	}
	if bytes.Equal(bytesA, bytesB) {
		t.Fatal("different implementation reused artifact bytes")
	}
	if !bytes.Contains(require(project, true, artifactB), []byte("43")) {
		t.Fatal("new callable not observed")
	}
	registryPath := filepath.Join(project, "workflows/example/server_health/v1/workflow.go")
	registry, _ := os.ReadFile(registryPath)
	invalid := bytes.Replace(registry, []byte("sample.CPUBinding()"), []byte(`implementation.Binding{Key: "read_cpu"}`), 1)
	if bytes.Equal(registry, invalid) {
		t.Fatal("invalid-binding control missing")
	}
	if err := os.WriteFile(registryPath, invalid, 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := run(project, true, official, "build", project, "--out", artifactB); err == nil {
		t.Fatalf("invalid binding published: %s", output)
	}
	retained, _ := os.ReadFile(artifactB)
	if !bytes.Equal(retained, bytesB) {
		t.Fatal("invalid new claim removed old artifact")
	}
	if _, err := run(project, true, official, "build", project, "--out", artifactB, "--declaration-only"); err == nil {
		t.Fatal("legacy compatibility bypassed supplied claim")
	}
}
