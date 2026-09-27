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
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	adapters "github.com/frost-leo/fathomry/adapters/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/internal/resource"
)

type selectedInput struct {
	source      source.Selection
	description source.Description
	documents   []LayerDocument
}
type admitted[T any] struct {
	version   uint32
	defaults  envelope[T]
	validate  func(Settings[T]) error
	inputs    []selectedInput
	variables []resource.Layer
}

// Catalogs is a configuration convenience over layer-level registration. It adds
// configuration/shared-source declarations; selected implementation modules remain
// explicit. Querying this set constructs no source and performs no configuration I/O.
func Catalogs(modules ...adapters.Module) (framework.CatalogSet, error) {
	if len(modules) > 64-5 {
		return framework.CatalogSet{}, fail(ErrLimit)
	}
	selected := make([]adapters.Module, 0, len(modules)+1)
	selected = append(selected, source.Module())
	selected = append(selected, modules...)
	return framework.Catalogs([]framework.Module{Module()}, selected...)
}
func admit[T any](schema Schema[T], plan Plan, watch bool) (*admitted[T], error) {
	if schema.FormatVersion == 0 || reflect.TypeFor[T]().Kind() != reflect.Struct {
		return nil, fail(ErrSchema)
	}
	if len(plan.Inputs) > 3 || len(plan.Variables) > 64 {
		return nil, fail(ErrLimit)
	}
	// Semantic required fields may be supplied later. Only shape/representation is
	// admitted here; no user validator or mandatory Core check runs on defaults.
	defaults := envelope[T]{Format: schema.FormatVersion, Framework: schema.Defaults.Framework, Project: schema.Defaults.Project}
	prepared, err := resource.PrepareData(resource.Schema[envelope[T]]{Format: schema.FormatVersion, Defaults: defaults}, nil)
	if err != nil {
		return nil, fail(ErrSchema)
	}
	defaults, err = prepared.ValueCopy()
	if err != nil {
		return nil, fail(ErrSchema)
	}
	catalogs, err := Catalogs(plan.Modules...)
	if err != nil {
		return nil, err
	}
	if watch && len(plan.Inputs) == 0 {
		return nil, fail(ErrPlan)
	}
	result := &admitted[T]{version: schema.FormatVersion, defaults: defaults, validate: schema.Validate}
	names := make(map[string]bool)
	layers := make(map[Layer]bool)
	total := 0
	for _, input := range plan.Inputs {
		if nilValue(input.Source) || len(input.Documents) == 0 || len(input.Documents) > 3-total {
			return nil, fail(ErrPlan)
		}
		description, err := input.Source.Description()
		if err != nil {
			return nil, err
		}
		if !label(description.Name) || names[description.Name] || len(description.Documents) != len(input.Documents) || watch && !description.Observable {
			return nil, fail(ErrPlan)
		}
		if _, found, err := catalogs.Errors.Module(description.Module); err != nil || !found {
			return nil, fail(ErrPlan)
		}
		names[description.Name] = true
		total += len(input.Documents)
		slots := make(map[string]bool)
		for _, name := range description.Documents {
			if !label(name) || slots[name] {
				return nil, fail(ErrPlan)
			}
			slots[name] = true
		}
		selected := selectedInput{source: input.Source, description: source.Description{Name: strings.Clone(description.Name), Module: strings.Clone(description.Module), Observable: description.Observable}}
		for _, document := range input.Documents {
			if !slots[document.Document] || document.Layer < Base || document.Layer > Local || layers[document.Layer] {
				return nil, fail(ErrPlan)
			}
			delete(slots, document.Document)
			layers[document.Layer] = true
			selected.documents = append(selected.documents, LayerDocument{Document: strings.Clone(document.Document), Layer: document.Layer, Optional: document.Optional})
			selected.description.Documents = append(selected.description.Documents, strings.Clone(document.Document))
		}
		result.inputs = append(result.inputs, selected)
	}
	variables, err := captureVariables[envelope[T]](plan.Variables)
	if err != nil {
		return nil, err
	}
	if len(variables) != 0 {
		result.variables = []resource.Layer{resource.JSONVariables(variables)}
		// Malformed/unknown/incorrectly typed environment data rejects before I/O.
		if _, err := resource.PrepareData(resource.Schema[envelope[T]]{Format: result.version, Defaults: result.defaults}, result.variables); err != nil {
			return nil, fail(ErrPlan)
		}
	}
	return result, nil
}

var localeCatalog = sync.OnceValues(func() (*i18n.Catalog, error) { return i18n.Prepare(Sources()...) })

func validateCore(core Core) error {
	if len(core.I18n.DefaultLocale) == 0 || len(core.I18n.DefaultLocale) > 128 {
		return fail(ErrPreparation)
	}
	catalog, err := localeCatalog()
	if err != nil {
		return err
	}
	if _, err := catalog.Resolve(ModuleID+":invalid_value", core.I18n.DefaultLocale); err != nil {
		return fail(ErrPreparation)
	}
	zone := core.Time.DisplayZone
	if zone == "" || len(zone) > 128 || !utf8.ValidString(zone) || zone == "Local" || strings.HasPrefix(zone, "/") || strings.ContainsAny(zone, "\\:") {
		return fail(ErrPreparation)
	}
	for _, part := range strings.Split(zone, "/") {
		if part == "" || part == "." || part == ".." {
			return fail(ErrPreparation)
		}
	}
	for _, char := range zone {
		if unicode.IsControl(char) {
			return fail(ErrPreparation)
		}
	}
	if core.Instance.Name != "" && !label(core.Instance.Name) {
		return fail(ErrPreparation)
	}
	return nil
}

type envValue struct {
	value   string
	present bool
}

func captureVariables[T any](variables []Variable) ([]byte, error) {
	targets := make([][]string, 0, len(variables))
	for index, variable := range variables {
		if !environmentName(variable.Name) || variable.Encoding != Text && variable.Encoding != JSON || len(variable.Field) > 4096 || !strings.HasPrefix(variable.Field, "/") {
			return nil, fail(ErrPlan)
		}
		path := strings.Split(variable.Field[1:], "/")
		if len(path) < 2 || path[0] != "framework" && path[0] != "project" {
			return nil, fail(ErrPlan)
		}
		kind := reflect.TypeFor[T]()
		for _, part := range path {
			for kind.Kind() == reflect.Pointer {
				kind = kind.Elem()
			}
			if kind.Kind() != reflect.Struct {
				return nil, fail(ErrPlan)
			}
			found := false
			for field := 0; field < kind.NumField(); field++ {
				entry := kind.Field(field)
				if entry.Tag.Get("json") == part {
					kind = entry.Type
					found = true
					break
				}
			}
			if !found {
				return nil, fail(ErrPlan)
			}
		}
		leaf := kind
		for leaf.Kind() == reflect.Pointer {
			leaf = leaf.Elem()
		}
		if variable.Encoding == Text && leaf.Kind() != reflect.String {
			return nil, fail(ErrPlan)
		}
		for prior := 0; prior < index; prior++ {
			other := variables[prior].Field
			if variable.Field == other || strings.HasPrefix(variable.Field, other+"/") || strings.HasPrefix(other, variable.Field+"/") {
				return nil, fail(ErrPlan)
			}
		}
		targets = append(targets, path)
	}
	values := make(map[string]envValue)
	total := 0
	document := make(map[string]any)
	for index, variable := range variables {
		value, exists := values[variable.Name]
		if !exists {
			value.value, value.present = os.LookupEnv(variable.Name)
			if len(value.value) > 1<<20-total {
				return nil, fail(ErrLimit)
			}
			total += len(value.value)
			value.value = strings.Clone(value.value)
			values[variable.Name] = value
		}
		if !value.present {
			if variable.Required {
				return nil, fail(ErrPlan)
			}
			continue
		}
		if !utf8.ValidString(value.value) {
			return nil, fail(ErrPlan)
		}
		var scalar any = value.value
		if variable.Encoding == JSON {
			if !json.Valid([]byte(value.value)) {
				return nil, fail(ErrPlan)
			}
			scalar = json.RawMessage(value.value)
		}
		target := document
		path := targets[index]
		for _, part := range path[:len(path)-1] {
			if target[part] == nil {
				target[part] = make(map[string]any)
			}
			target = target[part].(map[string]any)
		}
		target[path[len(path)-1]] = scalar
	}
	if len(document) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(document)
	if err != nil {
		return nil, fail(ErrPlan)
	}
	if len(data) > 1<<20 {
		return nil, fail(ErrLimit)
	}
	return data, nil
}
func environmentName(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	for index, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char == '_' || index > 0 && char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}
