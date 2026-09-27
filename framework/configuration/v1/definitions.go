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
	"embed"
	"github.com/frost-leo/fathomry/failure/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

// ModuleID is a static implementation identity, never an instance label.
const ModuleID = "fathomry.framework.configuration"

// Conditions are usable without catalogs; presentation never performs source I/O.
const (
	ErrSchema       failure.Condition = ModuleID + ".invalid_schema"
	ErrPlan         failure.Condition = ModuleID + ".invalid_plan"
	ErrLimit        failure.Condition = ModuleID + ".limit"
	ErrMissing      failure.Condition = ModuleID + ".missing"
	ErrFormat       failure.Condition = ModuleID + ".format"
	ErrPreparation  failure.Condition = ModuleID + ".preparation"
	ErrValue        failure.Condition = ModuleID + ".invalid_value"
	ErrCursor       failure.Condition = ModuleID + ".invalid_cursor"
	ErrBusy         failure.Condition = ModuleID + ".concurrent_wait"
	ErrClosed       failure.Condition = ModuleID + ".closed"
	ErrWait         failure.Condition = ModuleID + ".wait"
	ErrCleanup      failure.Condition = ModuleID + ".cleanup"
	ErrPresentation failure.Condition = ModuleID + ".presentation"
)

//go:embed resources/*.json
var resources embed.FS

// Definition returns owned static condition declarations.
func Definition() failure.ModuleDefinition {
	result := failure.ModuleDefinition{ID: ModuleID, Source: "configuration.v1"}
	for _, condition := range []failure.Condition{ErrSchema, ErrPlan, ErrLimit, ErrMissing, ErrFormat, ErrPreparation, ErrValue, ErrCursor, ErrBusy, ErrClosed, ErrWait, ErrCleanup, ErrPresentation} {
		result.Conditions = append(result.Conditions, failure.ConditionDefinition{Condition: condition, Contract: "v1"})
	}
	return result
}

// Sources returns original embedded resources with caller-owned bytes.
func Sources() []i18n.Source {
	var result []i18n.Source
	for _, locale := range []string{"en", "zh-CN"} {
		data, _ := resources.ReadFile("resources/" + locale + ".json")
		result = append(result, i18n.Source{Name: ModuleID + "." + locale, Data: data})
	}
	return result
}

// Bindings returns fact-free, same-condition presentation associations.
func Bindings() []i18n.Binding {
	var result []i18n.Binding
	for _, definition := range Definition().Conditions {
		key := string(definition.Condition)[len(ModuleID)+1:]
		result = append(result, i18n.Binding{ID: ModuleID + ":" + key, Condition: definition.Condition, ConditionContract: "v1", Surface: "configuration", Role: "explanation", Message: ModuleID + ":" + key, MessageContract: "v1"})
	}
	return result
}

// Module supplies static contributions without constructing a source.
func Module() framework.Module {
	return framework.Module{Definition: Definition(), Messages: Sources(), Grouping: i18n.Module{ID: ModuleID, Owners: []string{ModuleID}}, Bindings: Bindings()}
}
