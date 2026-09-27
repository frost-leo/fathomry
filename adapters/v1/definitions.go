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

package adapters

import (
	"embed"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

// ModuleID owns this layer's catalog admission failures.
const ModuleID = "fathomry.adapters"
const (
	ErrCatalog failure.Condition = ModuleID + ".invalid_catalog"
	ErrLimit   failure.Condition = ModuleID + ".limit"
)

//go:embed resources/*.json
var resources embed.FS

// Definition returns owned declarations without feature or service startup.
func Definition() failure.ModuleDefinition {
	return failure.ModuleDefinition{ID: ModuleID, Source: "adapters.v1", Conditions: []failure.ConditionDefinition{
		{Condition: ErrCatalog, Contract: "v1"}, {Condition: ErrLimit, Contract: "v1"},
	}}
}

// Sources returns named original resources; bytes belong to the caller.
func Sources() []i18n.Source {
	var result []i18n.Source
	for _, locale := range []string{"en", "zh-CN"} {
		data, _ := resources.ReadFile("resources/" + locale + ".json")
		result = append(result, i18n.Source{Name: ModuleID + "." + locale, Data: data})
	}
	return result
}

// Bindings returns owned static associations without inspecting occurrences.
func Bindings() []i18n.Binding {
	var result []i18n.Binding
	for _, item := range Definition().Conditions {
		key := string(item.Condition)[len(ModuleID)+1:]
		result = append(result, i18n.Binding{ID: ModuleID + ":" + key, Condition: item.Condition, ConditionContract: "v1", Surface: "catalog", Role: "explanation", Message: ModuleID + ":" + key, MessageContract: "v1"})
	}
	return result
}

// ModuleContribution returns the built-in layer module, included by Catalogs.
// Explicitly supplying it again is a duplicate, not an override.
func ModuleContribution() Module {
	return Module{Definition: Definition(), Messages: Sources(), Grouping: i18n.Module{ID: ModuleID, Owners: []string{ModuleID}}, Bindings: Bindings()}
}
