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

package viper_test

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	viper "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
)

type connection struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}
type settings struct {
	Connection connection        `json:"connection"`
	Headers    map[string]string `json:"headers"`
	Items      []string          `json:"items"`
	Optional   *string           `json:"optional"`
	Enabled    bool              `json:"enabled"`
	Limit      uint64            `json:"limit"`
}

func schema() resource.Schema[settings] {
	fallback := "fallback"
	return resource.Schema[settings]{Format: 1, Defaults: settings{
		Connection: connection{Host: "default", Port: 443},
		Headers:    map[string]string{"Default-Key": "inherited"},
		Items:      []string{"default"}, Optional: &fallback, Enabled: true, Limit: 19,
	}, Validate: func(value settings) error {
		if value.Connection.Port <= 0 || value.Connection.Port > 65535 {
			return errors.New("invalid port")
		}
		return nil
	}}
}

type selectedInput struct {
	kind           resource.LayerKind
	input          viper.LoadInput
	optional       bool
	environmentKey string
}

const errComposition fault.Kind = "fathomry.fixture.config.input"

func selected(kind resource.LayerKind, encoding, raw string) selectedInput {
	return selectedInput{kind: kind, input: viper.LoadInput{Options: viper.OptionsV1{Encoding: encoding}, Reader: strings.NewReader(raw)}}
}

// This test-owned composition chooses optional files and assigns layers. Neither
// policy is installed in the SDK package. Live environment strings are authorized
// document contents in this fixture, not a new public environment-value format.
func acquireLayers(ctx context.Context, selection []selectedInput) ([]resource.Layer, []*viper.Document, error) {
	var chosen []selectedInput
	var inputs []viper.LoadInput
	for _, source := range selection {
		if source.optional && source.input.File != "" {
			_, err := os.Stat(source.input.File)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, nil, errComposition.New(fault.Context{Operation: "select"}, err)
			}
		}
		chosen = append(chosen, source)
		inputs = append(inputs, source.input)
	}
	documents, err := viper.Load(ctx, inputs)
	if err != nil {
		return nil, nil, err
	}
	layers := make([]resource.Layer, 0, len(chosen))
	remaining := viper.MaxTotalBytes
	for index, document := range documents {
		var raw []byte
		if chosen[index].environmentKey != "" {
			value, err := document.ValueCopy(chosen[index].environmentKey)
			if err != nil {
				return nil, nil, err
			}
			text, ok := value.(string)
			if !ok {
				return nil, nil, errComposition.New(fault.Context{Operation: "variables"})
			}
			if len(text) > remaining {
				return nil, nil, errComposition.New(fault.Context{Operation: "variables"})
			}
			raw = []byte(text)
		} else {
			raw = document.RawCopy()
		}
		remaining -= len(raw)
		if remaining < 0 {
			return nil, nil, errComposition.New(fault.Context{Operation: "layers"})
		}
		layers = append(layers, resource.Layer{Kind: chosen[index].kind, Content: raw})
	}
	return layers, documents, nil
}

func prepareLayers(definition resource.Schema[settings], format uint32, layers []resource.Layer) (resource.Prepared[settings], error) {
	return resource.Prepare(definition, resource.Input{
		Identity: resource.Identity{Provider: "proof.settings", Name: "selected"},
		Format:   format, Layers: layers,
	})
}

// A refusing constructor observes the factory's fresh settings copy without
// acquiring any client, lease, capability or fictitious Viper shutdown obligation.
func inspectPrepared(t testing.TB, prepared resource.Prepared[settings]) settings {
	t.Helper()
	var result settings
	refusal := errors.New("inspection-only constructor")
	selection := resource.Select(prepared, func(_ context.Context, value settings) (resource.Resource[struct{}], error) {
		result = value
		return resource.Resource[struct{}]{}, refusal
	})
	ctx := context.Background()
	assembly, err := resource.Assemble(ctx, ctx, "inspection", selection)
	if !errors.Is(err, refusal) {
		t.Fatal("prepared inspection did not reach the refusing constructor")
	}
	if assembly != nil {
		if err := assembly.Close(ctx); err != nil {
			t.Fatal("inspection left cleanup responsibility")
		}
	}
	return result
}

func expectSettings(t testing.TB, got, want settings) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Error("preparation contract: effective values changed")
	}
}

func TestPreparationUsesRawLayersAndActualEnvironmentAcquisition(t *testing.T) {
	const variableName = "FATHOMRY_VIPER_PROOF_VARIABLES"
	t.Setenv(variableName, `{"connection":{"host":"variables"},"enabled":false,"limit":9007199254740993}`)
	path := filepath.Join(t.TempDir(), "literal-base.yaml")
	base := "connection: {host: base}\nheaders: {X-Tenant: base}\nitems: [base]"
	if err := os.WriteFile(path, []byte(base), 0600); err != nil {
		t.Fatal("fixture write failed")
	}
	original := []byte("items: []\noptional: null\nheaders: {Empty-Key: ''}")
	selection := []selectedInput{
		{kind: resource.Local, input: viper.LoadInput{Options: viper.OptionsV1{Encoding: "yaml"}, Reader: bytes.NewReader(original)}},
		{kind: resource.Variables, input: viper.LoadInput{Options: viper.OptionsV1{Encoding: "yaml",
			Environment: []viper.Binding{{Key: "document", Name: variableName}}, AllowEmptyEnv: true}, Reader: strings.NewReader("{}")}, environmentKey: "document"},
		{kind: resource.Base, input: viper.LoadInput{Options: viper.OptionsV1{Encoding: "yaml"}, File: path}},
		selected(resource.Environment, "json", `{"connection":{"host":"environment"},"headers":{"X-Tenant":"environment"}}`),
	}
	layers, documents, err := acquireLayers(context.Background(), selection)
	if err != nil {
		t.Fatal("acquisition failed", err)
	}
	definition := schema()
	prepared, err := prepareLayers(definition, 1, layers)
	if err != nil {
		t.Fatal("preparation failed", err)
	}
	want := settings{
		Connection: connection{Host: "variables", Port: 443},
		Headers:    map[string]string{"Default-Key": "inherited", "X-Tenant": "environment", "Empty-Key": ""},
		Items:      []string{}, Optional: nil, Enabled: false, Limit: 9007199254740993,
	}
	expectSettings(t, inspectPrepared(t, prepared), want)
	// Native lookup is useful and intentionally different from prepared lookup.
	nativeHeaders, err := documents[2].ValueCopy("headers")
	if err != nil || nativeHeaders.(map[string]any)["x-tenant"] != "base" {
		t.Fatal("actual SDK was not exercised")
	}
	revision := prepared.Description().Revision
	original[0] = '!'
	layers[0].Content[0] = '!'
	nativeHeaders.(map[string]any)["x-tenant"] = "changed"
	definition.Defaults.Headers["Default-Key"] = "changed"
	t.Setenv(variableName, `{"enabled":true}`)
	if err := os.WriteFile(path, []byte("enabled: true"), 0600); err != nil {
		t.Fatal("fixture rewrite failed")
	}
	if got, err := documents[1].ValueCopy("document"); err != nil || got != `{"enabled":true}` {
		t.Fatal("native environment was unexpectedly frozen")
	}
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() { expectSettings(t, inspectPrepared(t, prepared), want) })
	}
	group.Wait()
	if prepared.Description().Revision != revision {
		t.Fatal("frozen revision changed")
	}
	for index, layer := range prepared.Description().Provenance {
		if layer.Kind != resource.LayerKind(index) {
			t.Fatal("canonical precedence/provenance changed")
		}
	}
	conformance.Runtime(t, prepared, new(resource.Prepared[settings]), path, variableName, "X-Tenant", "Empty-Key")
}

func TestPreparationProofRejectsInvalidLayersAndFormats(t *testing.T) {
	for _, raw := range []string{
		"enabled: 1", "enabled: null", "Enabled: true", "unknown: credential-canary",
		"connection: {port: '443'}", "limit: -1", "limit: 1.5", "limit: 1e2",
		"limit: '1'", "limit: 18446744073709551616", "headers: {1: wrong}",
		"headers: {<<: {key: wrong}}", "enabled: !!bool true", "enabled: ! true",
		"connection: {host: 2026-09-10}", "items: &anchor [a]", "items: *anchor",
		`{"enabled":true,"enabled":false}`, "items: [a]\nitems: [b]",
		"[]", "null", "", "{}\n---\n{}", "{",
	} {
		for _, encoding := range []string{"yaml", "json"} {
			selection := []selectedInput{
				selected(resource.Base, encoding, raw),
				selected(resource.Variables, "yaml", "enabled: false\nlimit: 1\nconnection: {host: good, port: 443}\nheaders: {}\nitems: []"),
			}
			layers, _, err := acquireLayers(context.Background(), selection)
			var prepared resource.Prepared[settings]
			if err == nil {
				prepared, err = prepareLayers(schema(), 1, layers)
			}
			if err == nil || prepared.Description().Revision != "" {
				t.Fatal("invalid lower layer was accepted")
			}
			conformance.Private(t, err, "credential-canary")
		}
	}
	layers, _, err := acquireLayers(context.Background(), []selectedInput{selected(resource.Base, "yaml", "{}")})
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []uint32{0, 2} {
		if prepared, err := prepareLayers(schema(), format, layers); !errors.Is(err, resource.ErrConfiguration) || prepared.Description().Revision != "" {
			t.Fatal("unsupported preparation format accepted")
		}
	}
	layers = append(layers, resource.Layer{Kind: resource.Base, Content: []byte("{}")})
	if _, err := prepareLayers(schema(), 1, layers); !errors.Is(err, resource.ErrConfiguration) {
		t.Fatal("duplicate layer accepted")
	}
	for _, raw := range []string{"connection: {port: 0}", "connection: {port: 65536}"} {
		layers, _, err := acquireLayers(context.Background(), []selectedInput{selected(resource.Base, "yaml", raw)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := prepareLayers(schema(), 1, layers); !errors.Is(err, resource.ErrConfiguration) {
			t.Fatal("final semantic validation bypassed")
		}
	}
}

func TestOptionalSelectionIsNotMalformedFallback(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	selection := []selectedInput{
		selected(resource.Base, "yaml", "{}"),
		{kind: resource.Local, optional: true, input: viper.LoadInput{Options: viper.OptionsV1{Encoding: "yaml"}, File: missing}},
	}
	layers, _, err := acquireLayers(context.Background(), selection)
	if err != nil || len(layers) != 1 {
		t.Fatal("optional absent fixture selection failed")
	}
	if _, err := prepareLayers(schema(), 1, layers); err != nil {
		t.Fatal("optional missing positive control failed")
	}
	selection[1].optional = false
	if layers, _, err := acquireLayers(context.Background(), selection); layers != nil || !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("required missing source was silently ignored")
	}
	if err := os.WriteFile(missing, []byte("{"), 0600); err != nil {
		t.Fatal("fixture write failed")
	}
	selection[1].optional = true
	if layers, _, err := acquireLayers(context.Background(), selection); layers != nil || err == nil {
		t.Fatal("optional malformed source was ignored")
	}
	selection[1].input.File = ""
	selection[1].input.Reader = iotest.ErrReader(fs.ErrPermission)
	if layers, _, err := acquireLayers(context.Background(), selection); layers != nil || !errors.Is(err, fs.ErrPermission) {
		t.Fatal("optional unreadable source was ignored")
	}
}

func TestPreparationResolvedOutputBoundAndNumericControls(t *testing.T) {
	for _, raw := range []string{"limit: 0", "limit: 18446744073709551615"} {
		layers, _, err := acquireLayers(context.Background(), []selectedInput{selected(resource.Base, "yaml", raw)})
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := prepareLayers(schema(), 1, layers)
		if err != nil {
			t.Fatal("exact integer rejected")
		}
		want := uint64(0)
		if strings.Contains(raw, "18446744073709551615") {
			want = ^uint64(0)
		}
		if inspectPrepared(t, prepared).Limit != want {
			t.Fatal("exact integer changed")
		}
	}
	selection := []selectedInput{
		selected(resource.Base, "json", `{"headers":{"first":"`+strings.Repeat("x", 600<<10)+`"}}`),
		selected(resource.Local, "json", `{"headers":{"second":"`+strings.Repeat("y", 600<<10)+`"}}`),
	}
	layers, _, err := acquireLayers(context.Background(), selection)
	if err != nil {
		t.Fatal("bounded acquisition failed")
	}
	if prepared, err := prepareLayers(schema(), 1, layers); !errors.Is(err, resource.ErrConfiguration) || prepared.Description().Revision != "" {
		t.Fatal("resolved preparation size bound bypassed")
	}
}

func runHandoffOracle(t *testing.T, mode string) {
	selection := []selectedInput{selected(resource.Base, "json", `{"optional":null,"headers":{"X-Tenant":"value"},"limit":9007199254740993}`)}
	selection[0].input.Options.Defaults = []viper.Default{{Key: "optional", Value: "fallback"}}
	if mode == "lower-layer" || mode == "lower-layer-positive" {
		selection = []selectedInput{
			selected(resource.Base, "yaml", "enabled: 1"),
			selected(resource.Local, "yaml", "enabled: false"),
		}
	}
	layers, documents, err := acquireLayers(context.Background(), selection)
	if err != nil {
		t.Fatal("control acquisition failed")
	}
	switch mode {
	case "null", "case", "number":
		key := map[string]string{"null": "optional", "case": "headers", "number": "limit"}[mode]
		native, err := documents[0].ValueCopy(key)
		if err != nil {
			t.Fatal("control native query failed")
		}
		replacement := map[string]any{"optional": nil, "headers": map[string]string{"X-Tenant": "value"}, "limit": json.Number("9007199254740993")}
		replacement[key] = native
		layers[0].Content, err = json.Marshal(replacement)
		if err != nil {
			t.Fatal("broken control encoding failed")
		}
	case "lower-layer":
		layers = layers[1:]
	}
	prepared, err := prepareLayers(schema(), 1, layers)
	if strings.HasPrefix(mode, "lower-layer") {
		if !errors.Is(err, resource.ErrConfiguration) || prepared.Description().Revision != "" {
			t.Error("preparation contract: invalid lower layer accepted")
		}
		return
	}
	if err != nil {
		t.Fatal("control preparation failed")
	}
	want := schema().Defaults
	want.Optional = nil
	want.Headers["X-Tenant"] = "value"
	want.Limit = 9007199254740993
	expectSettings(t, inspectPrepared(t, prepared), want)
}

func TestHandoffOracle(t *testing.T) {
	if mode := os.Getenv("FATHOMRY_VIPER_BROKEN_HANDOFF"); mode != "" {
		runHandoffOracle(t, mode)
		fmt.Println("handoff oracle returned")
		return
	}
	runHandoffOracle(t, "positive")
	runHandoffOracle(t, "lower-layer-positive")
}

func TestLossyHandoffsFailRealPreparationOracle(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"null", "case", "number", "lower-layer"} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		command := exec.CommandContext(ctx, executable, "-test.run=^TestHandoffOracle$", "-test.count=1")
		command.Env = append(os.Environ(), "FATHOMRY_VIPER_BROKEN_HANDOFF="+mode)
		output, err := command.CombinedOutput()
		cancel()
		expected := "preparation contract: effective values changed"
		if mode == "lower-layer" {
			expected = "preparation contract: invalid lower layer accepted"
		}
		if err == nil || !bytes.Contains(output, []byte(expected)) || !bytes.Contains(output, []byte("handoff oracle returned")) ||
			bytes.Contains(output, []byte("panic:")) {
			t.Fatal("broken handoff failed for the wrong reason or escaped the oracle")
		}
	}
}

func ExampleLoad() {
	inputs := []viper.LoadInput{
		{Options: viper.OptionsV1{Encoding: "yaml"}, Reader: strings.NewReader("prefix: base")},
		{Options: viper.OptionsV1{Encoding: "json"}, Reader: strings.NewReader(`{"prefix":"local"}`)},
	}
	documents, err := viper.Load(context.Background(), inputs)
	if err != nil {
		panic(err)
	}
	native, err := documents[0].ValueCopy("PREFIX")
	if err != nil {
		panic(err)
	}
	fmt.Println("native:", native)
	type config struct {
		Prefix string `json:"prefix"`
	}
	prepared, err := resource.Prepare(resource.Schema[config]{Format: 1, Validate: func(value config) error {
		if value.Prefix != "local" {
			return errors.New("unexpected effective settings")
		}
		return nil
	}}, resource.Input{
		Identity: resource.Identity{Provider: "example.settings", Name: "selected"}, Format: 1,
		Layers: []resource.Layer{
			{Kind: resource.Local, Content: documents[1].RawCopy()},
			{Kind: resource.Base, Content: documents[0].RawCopy()},
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("prepared:", prepared.Description().Format)
	// Output:
	// native: base
	// prepared: 1
}

func fileInput(t testing.TB, encoding, content string) viper.LoadInput {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "selected-*.data")
	if err != nil {
		t.Fatal("real file fixture creation failed")
	}
	path := file.Name()
	_, writeErr := file.WriteString(content)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal("real file fixture write/close failed")
	}
	return viper.LoadInput{Options: viper.OptionsV1{Encoding: encoding}, File: path}
}

func TestFilePreparationLayersAndFreeze(t *testing.T) {
	selection := []selectedInput{
		{kind: resource.Local, input: fileInput(t, "yaml", "items: []\noptional: null\nheaders: {Empty-Key: ''}\nlimit: 0")},
		{kind: resource.Variables, input: fileInput(t, "json", `{"connection":{"host":"variables"},"enabled":false,"limit":18446744073709551615}`)},
		{kind: resource.Base, input: fileInput(t, "yaml", "connection: {host: base}\nheaders: {X-Tenant: base, x-tenant: independent}\nitems: [base]\noptional: base")},
		{kind: resource.Environment, input: fileInput(t, "json", `{"connection":{"host":"environment"},"headers":{"X-Tenant":"environment"},"items":["environment"]}`)},
	}
	layers, documents, err := acquireLayers(context.Background(), selection)
	if err != nil {
		t.Fatal("real multi-file acquisition failed", err)
	}
	definition := schema()
	validation := definition.Validate
	validations := 0
	definition.Validate = func(value settings) error { validations++; return validation(value) }
	prepared, err := prepareLayers(definition, 1, layers)
	if err != nil || validations != 1 {
		t.Fatal("real-file preparation did not validate exactly once", err)
	}
	want := settings{
		Connection: connection{Host: "variables", Port: 443},
		Headers:    map[string]string{"Default-Key": "inherited", "X-Tenant": "environment", "x-tenant": "independent", "Empty-Key": ""},
		Items:      []string{}, Optional: nil, Enabled: false, Limit: ^uint64(0),
	}
	expectSettings(t, inspectPrepared(t, prepared), want)
	host, err := documents[2].ValueCopy("connection.host")
	if err != nil || host != "base" {
		t.Fatal("real file native lookup was not exercised")
	}
	original := documents[2].RawCopy()
	revision := prepared.Description().Revision
	for _, input := range selection {
		if err := os.WriteFile(input.input.File, []byte("{"), 0600); err != nil {
			t.Fatal("fixture mutation failed")
		}
	}
	if _, changed, err := acquireLayers(context.Background(), selection); err == nil || changed != nil {
		t.Fatal("failed file reload published a batch")
	}
	if !bytes.Equal(original, documents[2].RawCopy()) || prepared.Description().Revision != revision {
		t.Fatal("failed file reload relabeled previous state")
	}
	expectSettings(t, inspectPrepared(t, prepared), want)
}

func TestFilePreparationRejectsOverriddenInvalidLayers(t *testing.T) {
	cases := []struct{ name, raw string }{
		{"wrong-type", `{"enabled":1}`}, {"null-scalar", `{"enabled":null}`},
		{"case", `{"Enabled":true}`}, {"unknown", `{"unknown":"file-canary"}`},
		{"nested-type", `{"connection":{"port":"443"}}`},
		{"negative-unsigned", `{"limit":-1}`}, {"fraction", `{"limit":1.5}`},
		{"exponent", `{"limit":1e2}`}, {"numeric-string", `{"limit":"1"}`},
		{"overflow", `{"limit":18446744073709551616}`},
		{"duplicate", `{"enabled":true,"enabled":false}`},
		{"null-document", "null"}, {"sequence-document", "[]"},
		{"empty", ""}, {"malformed", "{"},
	}
	for _, encoding := range []string{"yaml", "json"} {
		for _, test := range cases {
			t.Run(encoding+"/"+test.name, func(t *testing.T) {
				selection := []selectedInput{
					{kind: resource.Base, input: fileInput(t, encoding, test.raw)},
					{kind: resource.Variables, input: fileInput(t, encoding, `{"enabled":false,"limit":1,"connection":{"host":"good","port":443},"headers":{},"items":[]}`)},
				}
				layers, _, err := acquireLayers(context.Background(), selection)
				var prepared resource.Prepared[settings]
				if err == nil {
					prepared, err = prepareLayers(schema(), 1, layers)
				}
				if err == nil || prepared.Description().Revision != "" {
					t.Fatal("real invalid lower file was masked")
				}
				conformance.Private(t, err, "file-canary", selection[0].input.File)
			})
		}
	}
	for _, raw := range []string{
		"headers: {1: wrong}", "headers: {<<: {key: wrong}}", "enabled: !!bool true",
		"enabled: ! true", "connection: {host: 2026-09-10}", "items: &anchor [a]",
		"items: *anchor", "{}\n---\n{}",
	} {
		layers, _, err := acquireLayers(context.Background(), []selectedInput{
			{kind: resource.Base, input: fileInput(t, "yaml", raw)},
			{kind: resource.Local, input: fileInput(t, "yaml", "{}")},
		})
		var prepared resource.Prepared[settings]
		if err == nil {
			prepared, err = prepareLayers(schema(), 1, layers)
		}
		if err == nil || prepared.Description().Revision != "" {
			t.Fatal("unsupported real YAML file produced preparation")
		}
	}
	selection := []selectedInput{
		{kind: resource.Base, input: fileInput(t, "yaml", "{}")},
		{kind: resource.Base, input: fileInput(t, "json", "{}")},
	}
	layers, _, err := acquireLayers(context.Background(), selection)
	if err != nil {
		t.Fatal("duplicate-layer acquisition control failed")
	}
	if _, err := prepareLayers(schema(), 1, layers); !errors.Is(err, resource.ErrConfiguration) {
		t.Fatal("duplicate real-file layer accepted")
	}
	if _, err := prepareLayers(schema(), 2, layers[:1]); !errors.Is(err, resource.ErrConfiguration) {
		t.Fatal("unsupported schema format accepted")
	}
	if got, err := viper.Load(context.Background(), []viper.LoadInput{fileInput(t, "toml", "{}")}); err == nil || got != nil {
		t.Fatal("unsupported file encoding accepted")
	}
}

func TestFileAcquisitionByteAndCountBounds(t *testing.T) {
	jsonAtSize := func(size int) string { return `{"value":"` + strings.Repeat("x", size-len(`{"value":""}`)) + `"}` }
	for _, encoding := range []string{"yaml", "json"} {
		t.Run(encoding, func(t *testing.T) {
			exact := fileInput(t, encoding, jsonAtSize(viper.MaxDocumentBytes))
			documents, err := viper.Load(context.Background(), []viper.LoadInput{exact})
			if err != nil || len(documents) != 1 || len(documents[0].RawCopy()) != viper.MaxDocumentBytes {
				t.Fatal("exact real-file bound rejected", err)
			}
			oversize := fileInput(t, encoding, jsonAtSize(viper.MaxDocumentBytes+1))
			if got, err := viper.Load(context.Background(), []viper.LoadInput{oversize}); got != nil || !errors.Is(err, viper.ErrLimit) {
				t.Fatal("oversized real file was accepted")
			}
			batch := []viper.LoadInput{exact, exact, exact, exact}
			documents, err = viper.Load(context.Background(), batch)
			if err != nil || len(documents) != 4 {
				t.Fatal("exact aggregate real-file bound rejected", err)
			}
			batch = append(batch, fileInput(t, encoding, "{}"))
			if got, err := viper.Load(context.Background(), batch); got != nil || !errors.Is(err, viper.ErrLimit) {
				t.Fatal("real-file aggregate overflow published a partial batch")
			}
		})
	}
	small := fileInput(t, "json", "{}")
	inputs := make([]viper.LoadInput, viper.MaxSources+1)
	for index := range inputs {
		inputs[index] = small
	}
	if got, err := viper.Load(context.Background(), inputs[:viper.MaxSources]); err != nil || len(got) != viper.MaxSources {
		t.Fatal("exact file source-count bound rejected", err)
	}
	if got, err := viper.Load(context.Background(), inputs); got != nil || !errors.Is(err, viper.ErrInput) {
		t.Fatal("file source-count overflow accepted")
	}
}

func TestFileNativeQueryRejectsNegativeIndex(t *testing.T) {
	for _, encoding := range []string{"yaml", "json"} {
		input := fileInput(t, encoding, `{"items":["first","second"]}`)
		documents, err := viper.Load(context.Background(), []viper.LoadInput{input})
		if err != nil {
			t.Fatal("real-file native query fixture failed")
		}
		minimum := strconv.Itoa(-int(^uint(0)>>1) - 1)
		for _, key := range []string{"items.-1", "items.-01", "items." + minimum} {
			got, err := documents[0].ValueCopy(key)
			if got != nil || !errors.Is(err, viper.ErrInput) {
				t.Fatal("unsafe negative-index query was not refused")
			}
		}
	}
}

func TestActualConsumerBuildAndBehavior(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	executable := filepath.Join(t.TempDir(), "consumer")
	command := exec.CommandContext(ctx, "go", "build", "-o", executable, "./testdata/consumer")
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("acceptance executable build failed: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, executable).Output()
	if err != nil {
		t.Fatal("acceptance executable behavior failed")
	}
	var report struct {
		Go                         string
		Modules                    []struct{ Path, Version, Sum, Replacement, ReplacementVersion string }
		OptionsContract            string
		Provider                   string
		Encoding                   string
		PreparationFormat          uint32
		NativeAndPreparationChecks bool
	}
	if err := json.Unmarshal(output, &report); err != nil {
		t.Fatal("consumer report invalid")
	}
	if report.Go != runtime.Version() || report.Provider != "configsource.viper.v1" || report.OptionsContract != "OptionsV1" ||
		report.Encoding != "yaml" || report.PreparationFormat != 1 || !report.NativeAndPreparationChecks {
		t.Fatal("actual build / behavior axes conflated")
	}
	info, err := buildinfo.ReadFile(executable)
	if err != nil {
		t.Fatal("cannot independently inspect consuming executable")
	}
	expected := map[string]string{
		"github.com/spf13/viper": "v1.21.0", "go.yaml.in/yaml/v3": "v3.0.5",
		"github.com/spf13/afero": "v1.15.0", "github.com/spf13/cast": "v1.10.0",
		"github.com/go-viper/mapstructure/v2": "v2.4.0",
	}
	if len(report.Modules) != len(expected) {
		t.Fatal("selected implementation build facts missing")
	}
	for _, observed := range report.Modules {
		if expected[observed.Path] == "" || observed.Version != expected[observed.Path] ||
			observed.Replacement != "" || observed.ReplacementVersion != "" || observed.Sum == "" {
			t.Fatal("untested version/replacement combination; rerun behavioral acceptance before updating expectations")
		}
		found := false
		for _, module := range info.Deps {
			if module.Path == observed.Path {
				found = module.Version == observed.Version && module.Sum == observed.Sum && module.Replace == nil
			}
		}
		if !found {
			t.Fatal("self-reported module facts differ from actual executable")
		}
	}
	t.Logf("consuming executable: Go=%s Viper=%s YAML=%s; no replacements; native and preparation checks executed",
		report.Go, expected["github.com/spf13/viper"], expected["go.yaml.in/yaml/v3"])
}

func TestFileStructuralBounds(t *testing.T) {
	withinDepth := `{"value":` + strings.Repeat("[", viper.MaxDepth-2) + "0" + strings.Repeat("]", viper.MaxDepth-2) + "}"
	overDepth := `{"value":` + strings.Repeat("[", viper.MaxDepth+1) + "0" + strings.Repeat("]", viper.MaxDepth+1) + "}"
	withinNodes := `{"value":[` + strings.Repeat("0,", viper.MaxNodes-6) + "0]}"
	overNodes := `{"value":[` + strings.Repeat("0,", viper.MaxNodes) + "0]}"
	for _, encoding := range []string{"yaml", "json"} {
		for _, raw := range []string{withinDepth, withinNodes} {
			input := fileInput(t, encoding, raw)
			if documents, err := viper.Load(context.Background(), []viper.LoadInput{input}); err != nil || len(documents) != 1 {
				t.Fatal("within-budget real structure was rejected", err)
			}
		}
		for _, raw := range []string{overDepth, overNodes} {
			input := fileInput(t, encoding, raw)
			if documents, err := viper.Load(context.Background(), []viper.LoadInput{input}); documents != nil || !errors.Is(err, viper.ErrLimit) {
				t.Fatal("over-budget real structure reached native publication")
			}
		}
	}
}
