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

package viper

import (
	"embed"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	adapters "github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

// ModuleID is a static implementation identity, never an instance label.
const ModuleID = "fathomry.adapters.configsource.viper"

// Conditions are usable without catalogs; presentation never performs source I/O.
const (
	ErrSettings failure.Condition = ModuleID + ".invalid_settings"
	ErrRead     failure.Condition = ModuleID + ".read"
	ErrLimit    failure.Condition = ModuleID + ".limit"
	ErrEncoding failure.Condition = ModuleID + ".encoding"
	ErrClose    failure.Condition = ModuleID + ".cleanup"
)

//go:embed resources/*.json
var resources embed.FS

// Definition returns owned static condition declarations.
func Definition() failure.ModuleDefinition {
	result := failure.ModuleDefinition{ID: ModuleID, Source: "viper.v1"}
	for _, condition := range []failure.Condition{ErrSettings, ErrRead, ErrLimit, ErrEncoding, ErrClose} {
		declaration := failure.ConditionDefinition{Condition: condition, Contract: "v1"}
		if condition != ErrSettings {
			declaration.Facts = ModuleID + ":acquisition"
		}
		result.Conditions = append(result.Conditions, declaration)
	}
	result.Contracts = []failure.FactContract{{
		ID: ModuleID + ":acquisition", Revision: "v1", Use: failure.ErrorFacts, Access: failure.PublicFacts,
		Fields: []failure.FactField{
			{Name: "source", Kind: failure.StringFact, Required: true},
			{Name: "document", Kind: failure.StringFact, UnknownAllowed: true},
			{Name: "phase", Kind: failure.EnumFact, Required: true, Values: []string{string(source.SelectPhase), string(source.CapturePhase), string(source.ObservePhase), string(source.ClosePhase)}},
		},
	}}
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

// Bindings uses known source/phase facts from the same acquisition occurrence.
// Optional document identity is available through direct Acquisition inspection.
func Bindings() []i18n.Binding {
	var result []i18n.Binding
	for _, definition := range Definition().Conditions {
		key := string(definition.Condition)[len(ModuleID)+1:]
		binding := i18n.Binding{ID: ModuleID + ":" + key, Condition: definition.Condition, ConditionContract: "v1", Surface: "configuration", Role: "explanation", Message: ModuleID + ":" + key, MessageContract: "v1"}
		if definition.Facts != "" {
			binding.Input, binding.InputRevision = definition.Facts, "v1"
			binding.Arguments = []i18n.ArgumentBinding{{Argument: "source", Fact: "source"}, {Argument: "phase", Fact: "phase"}}
		}
		result = append(result, binding)
	}
	return result
}

// Module supplies static contributions without constructing a source.
func Module() adapters.Module {
	return adapters.Module{Definition: Definition(), Messages: Sources(), Grouping: i18n.Module{ID: ModuleID, Owners: []string{ModuleID}}, Bindings: Bindings()}
}
