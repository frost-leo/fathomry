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

package nacos

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	viper "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
	"github.com/frost-leo/fathomry/internal/resource"
)

type applicationSettings struct {
	Host     string            `json:"host"`
	Labels   map[string]string `json:"labels"`
	Optional *string           `json:"optional"`
	Items    []string          `json:"items"`
	Large    uint64            `json:"large"`
}

func inspectPrepared(t testing.TB, prepared resource.Prepared[applicationSettings]) applicationSettings {
	t.Helper()
	var value applicationSettings
	refusal := errors.New("inspection-only factory")
	selected := resource.Select(prepared, func(_ context.Context, input applicationSettings) (resource.Resource[struct{}], error) {
		value = input
		return resource.Resource[struct{}]{}, refusal
	})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "inspection", selected)
	if !errors.Is(err, refusal) {
		t.Fatal("prepared configuration was not inspected")
	}
	if assembly != nil {
		_ = assembly.Close(context.Background())
	}
	return value
}
func TestNativeReadViperAndSingleFrozenPreparation(t *testing.T) {
	fixture := newFixture(t, false)
	raw := `{"host":"remote","labels":{"X-Case":"remote"},"optional":null,"items":[],"large":18446744073709551615}`
	fixture.mu.Lock()
	fixture.values[key{"DEFAULT_GROUP", "settings.yaml"}] = raw
	fixture.mu.Unlock()
	client := openClient(t, fixture.options())
	document, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	local, err := viper.Load(context.Background(), []viper.LoadInput{{Options: viper.OptionsV1{Encoding: "yaml"}, Reader: strings.NewReader("host: local\nlabels: {Keep: inherited}\noptional: inherited\nitems: [base]\n")}})
	if err != nil {
		t.Fatal(err)
	}
	validations := 0
	schema := resource.Schema[applicationSettings]{Format: 1, Validate: func(value applicationSettings) error {
		validations++
		if value.Large != ^uint64(0) || value.Optional != nil || value.Labels["X-Case"] != "remote" {
			return errors.New("lossy handoff")
		}
		return nil
	}}
	input := resource.Input{Identity: resource.Identity{Provider: "proof.settings", Name: "application"}, Format: 1,
		Layers: []resource.Layer{{Kind: resource.Environment, Content: document.RawCopy()}, {Kind: resource.Base, Content: local[0].RawCopy()}}}
	prepared, err := resource.Prepare(schema, input)
	if err != nil || validations != 1 {
		t.Fatal("preparation was not applied exactly once", err)
	}
	expected := applicationSettings{Host: "remote", Labels: map[string]string{"Keep": "inherited", "X-Case": "remote"}, Items: []string{}, Large: ^uint64(0)}
	if !reflect.DeepEqual(inspectPrepared(t, prepared), expected) {
		t.Fatal("effective source semantics changed")
	}
	revision := prepared.Description().Revision
	fixture.mu.Lock()
	fixture.values[key{"DEFAULT_GROUP", "settings.yaml"}] = "host: changed"
	fixture.mu.Unlock()
	changed, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"})
	if err != nil || string(changed.RawCopy()) == raw {
		t.Fatal("fresh acquisition reused an earlier value", err)
	}
	if string(document.RawCopy()) != raw || prepared.Description().Revision != revision || !reflect.DeepEqual(inspectPrepared(t, prepared), expected) {
		t.Fatal("new read mutated frozen preparation")
	}
	input.Layers[1].Content = []byte("large: invalid")
	if value, err := resource.Prepare(resource.Schema[applicationSettings]{Format: 1}, input); !errors.Is(err, resource.ErrConfiguration) || value.Description().Revision != "" {
		t.Fatal("remote source hid an invalid lower layer")
	}
}
func TestPreparationRejectingControl(t *testing.T) {
	if os.Getenv("FATHOMRY_NACOS_LOSSY_CONTROL") == "1" {
		layers := []resource.Layer{{Kind: resource.Base, Content: []byte("large: invalid")}, {Kind: resource.Environment, Content: []byte(`{"large":7}`)}}
		layers = layers[1:]
		prepared, err := resource.Prepare(resource.Schema[applicationSettings]{Format: 1}, resource.Input{Identity: resource.Identity{Provider: "proof.settings", Name: "broken"}, Format: 1, Layers: layers})
		if !errors.Is(err, resource.ErrConfiguration) || prepared.Description().Revision != "" {
			t.Error("preparation contract: invalid lower layer was discarded")
		}
		t.Log("handoff oracle returned")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPreparationRejectingControl$", "-test.v")
	command.Env = append(os.Environ(), "FATHOMRY_NACOS_LOSSY_CONTROL=1", "GORACE=atexit_sleep_ms=0")
	output, err := command.CombinedOutput()
	var rejected *exec.ExitError
	if !errors.As(err, &rejected) || rejected.ExitCode() != 1 || !strings.Contains(string(output), "preparation contract: invalid lower layer was discarded") || !strings.Contains(string(output), "handoff oracle returned") || strings.Contains(string(output), "panic:") {
		t.Fatal("wrong handoff did not fail the intended assertion")
	}
}
func TestActualConsumingExecutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "nacos-consumer")
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/consumer")
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build failed: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal("consumer execution failed", err)
	}
	var report struct {
		Go, SDK, SDKSum                  string
		PreparationVerified, RealService bool
	}
	if json.Unmarshal(output, &report) != nil || report.Go != runtime.Version() || report.SDK != "v2.3.5" || report.SDKSum == "" || !report.PreparationVerified || report.RealService {
		t.Fatal("consumer evidence mismatched")
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, module := range info.Deps {
		if module.Path == "github.com/nacos-group/nacos-sdk-go/v2" {
			found = module.Version == report.SDK && module.Sum == report.SDKSum && module.Replace == nil
		}
	}
	if !found {
		t.Fatal("consumer SDK facts differ from actual executable")
	}
}
