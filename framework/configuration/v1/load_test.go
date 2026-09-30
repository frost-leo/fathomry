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
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
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

func modelSchema() configsource.Schema[model] {
	defaults := model{Values: map[string]int{"default": 1}, Items: []int{9}}
	defaults.Service.Host, defaults.Service.Port = "default", 8080
	return configsource.Schema[model]{Version: 1, Defaults: defaults, Validate: func(_ context.Context, value model) error {
		if value.Service.Host == "" || value.Service.Port == 0 || (value.Credential.Access == "") != (value.Credential.Secret == "") {
			return errors.New("private-validation-canary")
		}
		value.Values["validator"] = 1
		return nil
	}}
}
func dependencies(t *testing.T) (Dependencies, *framework.Runtime, *viper.Client) {
	t.Helper()
	runtime, err := framework.New(context.Background(), framework.Options{Operations: adapters.Options{MaxWorkBytes: 128 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := adapters.NewInbox[Evidence](adapters.EvidenceOptions{Capacity: 128})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := framework.StartReceiver(context.Background(), evidence, framework.ReceiverOptions{}, func(context.Context, adapters.Snapshot[Evidence]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	sourceEvidence, _ := adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{Capacity: 128})
	sourceReceiver, err := framework.StartReceiver(context.Background(), sourceEvidence, framework.ReceiverOptions{}, func(context.Context, adapters.Snapshot[viper.Evidence]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	client, err := viper.New(viper.Dependencies{Runtime: runtime.Operations(), Evidence: sourceEvidence})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := receiver.Finish(ctx); err != nil {
			t.Error(err)
		}
		if err := sourceReceiver.Finish(ctx); err != nil {
			t.Error(err)
		}
	})
	return Dependencies{Runtime: runtime.Operations(), Evidence: evidence}, runtime, client
}
func write(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}
func TestLoad(t *testing.T) {
	deps, _, client := dependencies(t)
	root := t.TempDir()
	paths := []string{filepath.Join(root, "base.yaml"), filepath.Join(root, "environment.yaml"), filepath.Join(root, "local.yaml")}
	write(t, paths[0], "service: {port: 443}\nvalues: {base: 2}\nitems: [1, 2]\n")
	write(t, paths[2], "service: {host: local}\nvalues: {local: 3}\nitems: []\n")
	source, err := client.Source(viper.WatchSettings{Paths: paths})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("FATHOMRY_CONFIG_HOST", "environment")
	t.Setenv("FATHOMRY_CONFIG_PORT", "65535")
	t.Setenv("FATHOMRY_CONFIG_VALUES", `{"variable":4}`)
	declaration := Declaration[model]{
		Schema: modelSchema(), Source: source,
		Layers:      []Layer{{Kind: configsource.Base, Encoding: configsource.YAML}, {Kind: configsource.Environment, Encoding: configsource.YAML, Optional: true}, {Kind: configsource.Local, Encoding: configsource.YAML, Optional: true}},
		Environment: []Environment{{Name: "FATHOMRY_CONFIG_HOST", Path: "/service/host"}, {Name: "FATHOMRY_CONFIG_PORT", Path: "/service/port", JSON: true}, {Name: "FATHOMRY_CONFIG_VALUES", Path: "/values", JSON: true}},
	}
	state, err := Load(context.Background(), declaration, deps)
	if err != nil {
		t.Fatal(err)
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
		if result != nil || !errors.Is(err, configsource.ErrDecode) {
			t.Fatal("invalid layer published", err)
		}
	})
	t.Run("required missing and optional present empty", func(t *testing.T) {
		if err := os.Remove(paths[0]); err != nil {
			t.Fatal(err)
		}
		if result, err := Load(context.Background(), declaration, deps); result != nil || !errors.Is(err, ErrMissing) {
			t.Fatal(err)
		}
		write(t, paths[0], "{}")
		write(t, paths[2], "")
		if result, err := Load(context.Background(), declaration, deps); result != nil || !errors.Is(err, configsource.ErrDecode) {
			t.Fatal("present empty treated as missing", err)
		}
	})
	t.Run("strong validation does not expose rejected input", func(t *testing.T) {
		write(t, paths[2], "credential: {access: private-credential-canary}\n")
		result, err := Load(context.Background(), declaration, deps)
		if result != nil || err == nil || strings.Contains(fmt.Sprintf("%+v", err), "canary") {
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
	deps, _, client := dependencies(t)
	if result, err := Load(context.Background(), Declaration[struct{}]{Schema: configsource.Schema[struct{}]{Version: 1}}, deps); err != nil || result == nil {
		t.Fatal(err)
	}
	var typedNil *viper.Source
	if result, err := Load(context.Background(), Declaration[struct{}]{Schema: configsource.Schema[struct{}]{Version: 1}, Source: typedNil}, deps); err != nil || result == nil {
		t.Fatal(err)
	}
	source, _ := client.Source(viper.WatchSettings{Paths: []string{filepath.Join(t.TempDir(), "missing")}})
	for _, declaration := range []Declaration[model]{
		{Schema: modelSchema(), Source: source},
		{Schema: modelSchema(), Layers: []Layer{{Kind: configsource.Base, Encoding: configsource.JSON}}},
		{Schema: modelSchema(), Source: source, Layers: []Layer{{Kind: configsource.Variables, Encoding: configsource.JSON}}},
		{Schema: modelSchema(), Source: source, Layers: []Layer{{Kind: configsource.Base, Encoding: "toml"}}},
		{Schema: modelSchema(), Environment: []Environment{{Name: "bad=name", Path: "/service/host"}}},
		{Schema: modelSchema(), Environment: []Environment{{Name: "VALID", Path: "/values/key", JSON: true}}},
	} {
		if result, err := Load(context.Background(), declaration, deps); err == nil || result != nil {
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
