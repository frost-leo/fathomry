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

// This is an in-module acceptance executable, not a public startup command.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/frost-leo/fathomry/internal/compatibility"
	viper "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
	"github.com/frost-leo/fathomry/internal/resource"
)

type configuration struct {
	Headers  map[string]string `json:"headers"`
	Optional *string           `json:"optional"`
	Large    uint64            `json:"large"`
}

type moduleFact struct {
	Path               string
	Version            string
	Sum                string
	Replacement        string
	ReplacementVersion string
}

func main() {
	documents, err := viper.Load(context.Background(), []viper.LoadInput{{
		Options: viper.OptionsV1{Encoding: "yaml", Defaults: []viper.Default{{Key: "optional", Value: "fallback"}}},
		Reader:  strings.NewReader("headers: {X-Tenant: selected}\noptional: null\nlarge: 18446744073709551615"),
	}})
	if err != nil {
		panic("consumer acquisition failed")
	}
	optional, err := documents[0].ValueCopy("OPTIONAL")
	if err != nil || optional != "fallback" {
		panic("consumer native fallback changed")
	}
	headers, err := documents[0].ValueCopy("headers")
	if err != nil || headers.(map[string]any)["x-tenant"] != "selected" {
		panic("consumer native key normalization changed")
	}
	validations := 0
	prepared, err := resource.Prepare(resource.Schema[configuration]{Format: 1, Validate: func(value configuration) error {
		validations++
		if value.Optional != nil || value.Headers["X-Tenant"] != "selected" || value.Large != ^uint64(0) {
			return errors.New("consumer preparation semantics changed")
		}
		return nil
	}}, resource.Input{
		Identity: resource.Identity{Provider: "fixture.config", Name: "selected"},
		Format:   1, Layers: []resource.Layer{{Kind: resource.Base, Content: documents[0].RawCopy()}},
	})
	if err != nil || validations != 1 || prepared.Description().Revision == "" {
		panic("consumer preparation failed")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{
		"github.com/spf13/viper", "go.yaml.in/yaml/v3", "github.com/spf13/afero",
		"github.com/spf13/cast", "github.com/go-viper/mapstructure/v2",
	}})
	if err != nil {
		panic("consumer build inspection failed")
	}
	modules := make([]moduleFact, len(build.SDKs))
	for index, module := range build.SDKs {
		modules[index] = moduleFact{Path: module.Path.Value, Version: module.Version.Value, Sum: module.Sum.Value,
			Replacement: string(module.Replacement.Kind), ReplacementVersion: module.Replacement.Version.Value}
	}
	report := struct {
		Go                         string
		Modules                    []moduleFact
		OptionsContract            string
		Provider                   string
		Encoding                   string
		PreparationFormat          uint32
		NativeAndPreparationChecks bool
		Revision, Modified         string
	}{
		Go: build.Go.Value, Modules: modules, OptionsContract: "OptionsV1", Provider: viper.ProviderID,
		Encoding: documents[0].Encoding(), PreparationFormat: prepared.Description().Format,
		NativeAndPreparationChecks: true, Revision: build.Framework.VCS.Revision.Value, Modified: build.Framework.VCS.Modified.Value,
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		panic("consumer report failed")
	}
}
