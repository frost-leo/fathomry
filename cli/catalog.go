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

package cli

import (
	"embed"
	"sync"

	"github.com/frost-leo/fathomry/cli/internal/command/project"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

//go:embed resources/*.json
var resources embed.FS

// CatalogSet contains explicitly shared immutable definition/resource handles.
// Each Catalogs call returns fresh handle copies sharing immutable storage.
// Query snapshots own their mutable storage. No invocation locale is retained.
type CatalogSet struct {
	Errors   *failure.DefinitionCatalog
	Messages *i18n.Catalog
	Bindings *i18n.Bindings
}

var preparedCatalogs = sync.OnceValues(loadCatalogs)

// Catalogs provides the complete built-in CLI/root-project and i18n condition
// inventory without running a command, inspecting argv/environment, opening a
// project, starting services or observing runtime failures. Embedded resources
// are prepared once; errors are returned rather than recursively localized.
func Catalogs() (CatalogSet, error) {
	prepared, err := preparedCatalogs()
	if err != nil {
		return CatalogSet{}, err
	}
	definitions, messages, bindings := *prepared.Errors, *prepared.Messages, *prepared.Bindings
	return CatalogSet{Errors: &definitions, Messages: &messages, Bindings: &bindings}, nil
}

func hostDefinition() failure.ModuleDefinition {
	module := failure.ModuleDefinition{ID: "fathomry.cli", Source: "cli.host"}
	for _, condition := range []failure.Condition{ErrInputs, ErrDefinition, ErrUsage, ErrLanguage, ErrOutput} {
		declaration := failure.ConditionDefinition{Condition: condition, Contract: "v1"}
		if condition == ErrOutput {
			declaration.Facts = "fathomry.cli:output"
		}
		module.Conditions = append(module.Conditions, declaration)
	}
	module.Contracts = []failure.FactContract{{
		ID: "fathomry.cli:output", Revision: "v1", Use: failure.ErrorFacts, Access: failure.OwnerFacts,
		Fields: []failure.FactField{{Name: "stream", Kind: failure.EnumFact, Required: true, UnknownAllowed: true, Values: []string{"unknown", "stdout", "stderr"}}},
	}}
	return module
}

func hostBindings() []i18n.Binding {
	var result []i18n.Binding
	for _, key := range []string{"root", "help", "helpFlag", "language", "usage", "commands", "flags", "invalid", "failed", "canceled"} {
		result = append(result, i18n.Binding{ID: "fathomry.cli:" + key, Surface: "cli", Role: "text", Message: "fathomry.cli:" + key, MessageContract: "v1"})
	}
	for _, entry := range hostDefinition().Conditions {
		key := "failed"
		if entry.Condition == ErrUsage || entry.Condition == ErrLanguage {
			key = "invalid"
		}
		result = append(result, i18n.Binding{
			ID: "fathomry.cli:" + string(entry.Condition)[len("fathomry.cli."):], Condition: entry.Condition, ConditionContract: "v1",
			Surface: "cli", Role: "explanation", Message: "fathomry.cli:" + key, MessageContract: "v1",
		})
	}
	return result
}

func loadCatalogs() (CatalogSet, error) {
	var result CatalogSet
	root := hostDefinition()
	root.Children = []failure.ModuleDefinition{project.Definition()}
	var err error
	result.Errors, err = failure.PrepareDefinitions(root, i18n.Definitions())
	if err != nil {
		return CatalogSet{}, err
	}
	sources, err := project.Sources()
	if err != nil {
		return CatalogSet{}, err
	}
	for _, name := range []string{"en", "zh-cn"} {
		data, err := resources.ReadFile("resources/" + name + ".json")
		if err != nil {
			return CatalogSet{}, err
		}
		sources = append(sources, i18n.Source{Name: "cli." + name, Data: data})
	}
	result.Messages, err = i18n.Prepare(sources...)
	if err != nil {
		return CatalogSet{}, err
	}
	result.Messages, err = result.Messages.WithModules(
		i18n.Module{ID: "fathomry.cli", Owners: []string{"fathomry.cli"}, Children: []i18n.Module{{ID: "fathomry.cli.project", Owners: []string{"fathomry.cli.project"}}}},
		i18n.Module{ID: "fathomry.i18n"},
	)
	if err != nil {
		return CatalogSet{}, err
	}
	result.Bindings, err = i18n.PrepareBindings(result.Errors, result.Messages, append(hostBindings(), project.Bindings()...)...)
	if err != nil {
		return CatalogSet{}, err
	}
	return result, nil
}

func prepareHostBindings(catalog *i18n.Catalog) (*i18n.Bindings, error) {
	definitions, err := failure.PrepareDefinitions(hostDefinition())
	if err != nil {
		return nil, err
	}
	return i18n.PrepareBindings(definitions, catalog, hostBindings()...)
}
