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

package configuration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type model struct {
	Service struct {
		Host string `json:"host"`
		Port uint16 `json:"port"`
	} `json:"service"`
	Values     map[string]int `json:"values"`
	Items      []int          `json:"items"`
	Credential struct {
		Access string `json:"access"`
		Secret string `json:"secret"`
	} `json:"credential"`
}

func modelSchema() Schema[model] {
	defaults := model{Values: map[string]int{"default": 1}, Items: []int{9}}
	defaults.Service.Host, defaults.Service.Port = "default", 8080
	return Schema[model]{Version: 1, Defaults: defaults, Validate: func(_ context.Context, value model) error {
		if value.Service.Host == "" || value.Service.Port == 0 || (value.Credential.Access == "") != (value.Credential.Secret == "") {
			return errors.New("private-validation-canary")
		}
		value.Values["validator"] = 1
		return nil
	}}
}
func sourceProvider(source configsource.Source, encoding Encoding) Provider {
	return Provider{state: &providerPlan{name: "test", layers: []layer{{Kind: Base, Encoding: encoding}}, open: func(context.Context, *adapters.Runtime) (sourceBinding, error) {
		return sourceBinding{source: source}, nil
	}}}
}

func write(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}
func TestLoad(t *testing.T) {
	root := t.TempDir()
	paths := []string{filepath.Join(root, "base.yaml"), filepath.Join(root, "environment.yaml"), filepath.Join(root, "local.yaml")}
	write(t, paths[0], "service: {port: 443}\nvalues: {base: 2}\nitems: [1, 2]\n")
	write(t, paths[2], "service: {host: local}\nvalues: {local: 3}\nitems: []\n")
	provider, err := Viper(ViperOptions{Documents: []File{{Path: paths[0], Kind: Base, Encoding: YAML}, {Path: paths[1], Kind: Environment, Encoding: YAML, Optional: true}, {Path: paths[2], Kind: Override, Encoding: YAML, Optional: true}}})
	if err != nil {
		t.Fatal(err)
	}
	deps := Dependencies{Provider: provider}
	t.Setenv("FATHOMRY_CONFIG_HOST", "must-not-be-read")
	declaration := Declaration[model]{
		Schema:    modelSchema(),
		Variables: []Variable{{Value: "environment", Present: true, Path: "/service/host"}, {Value: "65535", Present: true, Path: "/service/port", JSON: true}, {Value: `{"variable":4}`, Present: true, Path: "/values", JSON: true}},
	}
	result, err := Load(context.Background(), declaration, deps)
	if err != nil {
		t.Fatal(err)
	}
	state := result.State
	if len(result.Records) != 2 {
		t.Fatal("missing owned scenario records")
	}
	for _, record := range result.Records {
		if !record.Info().Released {
			t.Fatal("live record escaped Load")
		}
	}
	accepted, err := state.Capture()
	if err != nil {
		t.Fatal(err)
	}
	value, err := accepted.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	if value.Service.Host != "environment" || value.Service.Port != 65535 || len(value.Items) != 0 || !reflect.DeepEqual(value.Values, map[string]int{"default": 1, "base": 2, "local": 3, "variable": 4}) {
		t.Fatal("layer or variable semantics changed")
	}
	value.Values["changed"] = 1
	declaration.Schema.Defaults.Values["default"] = 99
	view, err := state.Reader().Capture()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settings.As[model](view)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := snapshot.ValueCopy()
	if again.Values["default"] != 1 || again.Values["changed"] != 0 {
		t.Fatal("accepted alias")
	}
	if len(accepted.Description().Revision) != 32 || accepted.Sequence() != 1 || accepted.View() != (view) {
		t.Fatal("value/revision/view not coherent")
	}
	status, _ := state.Status()
	if status.Accepted != 1 || status.Published != 1 || !status.Closed || status.Adoption != nil {
		t.Fatal("wrong finite status")
	}
	t.Run("invalid lower layer cannot be masked", func(t *testing.T) {
		write(t, paths[0], "service: {unknown: 1}\n")
		result, err := Load(context.Background(), declaration, deps)
		if result.State != nil || !errors.Is(err, configsource.ErrDecode) {
			t.Fatal("invalid layer published", err)
		}
	})
	t.Run("required missing and optional present empty", func(t *testing.T) {
		if err := os.Remove(paths[0]); err != nil {
			t.Fatal(err)
		}
		if result, err := Load(context.Background(), declaration, deps); result.State != nil || !errors.Is(err, ErrMissing) {
			t.Fatal(err)
		}
		write(t, paths[0], "{}")
		write(t, paths[2], "")
		if result, err := Load(context.Background(), declaration, deps); result.State != nil || !errors.Is(err, configsource.ErrDecode) {
			t.Fatal("present empty treated as missing", err)
		}
	})
	t.Run("strong validation does not expose rejected input", func(t *testing.T) {
		write(t, paths[2], "credential: {access: private-credential-canary}\n")
		result, err := Load(context.Background(), declaration, deps)
		if result.State != nil || err == nil || strings.Contains(fmt.Sprintf("%+v", err), "canary") {
			t.Fatal("validation leaked or published")
		}
	})
}
func TestDeclaration(t *testing.T) {
	guarded := Declaration[model]{Schema: modelSchema()}
	if _, err := json.Marshal(guarded); !errors.Is(err, ErrSerialization) {
		t.Fatal("runtime declaration serialized", err)
	}
	if err := json.Unmarshal([]byte("{}"), &guarded); !errors.Is(err, ErrSerialization) || guarded.Schema.Validate == nil {
		t.Fatal("declaration validation silently reconstructed", err)
	}
	deps := Dependencies{Provider: Values()}
	if result, err := Load(context.Background(), Declaration[struct{}]{Schema: Schema[struct{}]{Version: 1}}, deps); err != nil || result.State == nil {
		t.Fatal(err)
	}
	provider, err := Viper(ViperOptions{Documents: []File{{Path: filepath.Join(t.TempDir(), "missing"), Kind: Base, Encoding: JSON}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []struct {
		declaration Declaration[model]
		provider    Provider
	}{
		{Declaration[model]{}, provider},
		{Declaration[model]{Schema: modelSchema()}, Provider{}},
		{Declaration[model]{Schema: modelSchema(), Variables: []Variable{{Path: "bad-path"}}}, Values()},
		{Declaration[model]{Schema: modelSchema(), Variables: []Variable{{Path: "/values/key", JSON: true}}}, Values()},
	} {
		if result, err := Load(context.Background(), invalid.declaration, Dependencies{Provider: invalid.provider}); err == nil || result.State != nil {
			t.Fatal("invalid declaration published")
		}
	}

	if _, err := new(State[model]).Capture(); !errors.Is(err, ErrHandle) {
		t.Fatal(err)
	}
	if _, err := Watch(context.Background(), Declaration[model]{Schema: modelSchema()}, deps, WatchOptions{}); !errors.Is(err, ErrDeclaration) {
		t.Fatal(err)
	}
}
