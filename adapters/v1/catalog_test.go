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

package adapters_test

import (
	"errors"
	"testing"

	adapters "github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func TestStaticAdmissionRejectsInvalidAndBoundedInputs(t *testing.T) {
	for _, module := range []adapters.Module{
		{},
		{Definition: failure.ModuleDefinition{ID: ".invalid", Source: "test"}, Grouping: i18n.Module{ID: ".invalid"}},
		{Definition: failure.ModuleDefinition{ID: "fathomry.adapters.child", Source: "test", Conditions: []failure.ConditionDefinition{{Condition: "wrong.owner.error", Contract: "v1"}}}, Grouping: i18n.Module{ID: "fathomry.adapters.child"}},
	} {
		if _, err := adapters.Catalogs(module); err == nil {
			t.Fatal("invalid module disappeared")
		} else if _, ok := failure.Inspect(err); !ok {
			t.Fatal("bare definition sentinel escaped", err)
		}
	}
	module := adapters.Module{Definition: failure.ModuleDefinition{ID: "fathomry.adapters.child", Source: "test"}, Grouping: i18n.Module{ID: "fathomry.adapters.child"}}
	cycle := make([]failure.ModuleDefinition, 1)
	cycle[0] = failure.ModuleDefinition{ID: "fathomry.adapters.child.loop", Source: "test", Children: cycle}
	module.Definition.Children = cycle
	if _, err := adapters.Catalogs(module); err == nil {
		t.Fatal("cyclic declaration accepted")
	}
	module.Definition.Children = nil
	module.Messages = make([]i18n.Source, i18n.MaxSources)
	if _, err := adapters.Catalogs(module); !errors.Is(err, adapters.ErrLimit) {
		t.Fatal("built-ins excluded from input count", err)
	}
	module.Messages = []i18n.Source{{Name: "oversize", Data: make([]byte, i18n.MaxDocumentBytes+1)}}
	if _, err := adapters.Catalogs(module); !errors.Is(err, adapters.ErrLimit) {
		t.Fatal("document work not bounded", err)
	}
	module.Messages = nil
	module.Bindings = make([]i18n.Binding, i18n.MaxBindings)
	if _, err := adapters.Catalogs(module); !errors.Is(err, adapters.ErrLimit) {
		t.Fatal("built-ins excluded from binding count", err)
	}
	module.Bindings = nil
	module.Definition.Children = make([]failure.ModuleDefinition, failure.MaxModules-2)
	if _, err := adapters.Catalogs(module); !errors.Is(err, adapters.ErrLimit) {
		t.Fatal("top-level attachment work excluded built-ins", err)
	}
}
func TestLayerCatalogsAreDetachedAndConditionQueriesAreOffline(t *testing.T) {
	first, err := adapters.Catalogs()
	if err != nil {
		t.Fatal(err)
	}
	module := adapters.ModuleContribution()
	module.Messages[0].Data[0] = '!'
	module.Definition.Conditions[0].Condition = "other.owner.condition"
	second, err := adapters.Catalogs()
	if err != nil {
		t.Fatal("static contribution borrowed mutation", err)
	}
	for _, catalog := range []adapters.CatalogSet{first, second} {
		definitions, found, err := catalog.Errors.Definitions(adapters.ModuleID, true)
		if err != nil || !found || len(definitions) != 2 {
			t.Fatal("incomplete layer inventory", err)
		}
	}
}
