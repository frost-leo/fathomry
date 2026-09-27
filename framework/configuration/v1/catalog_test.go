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

package configuration_test

import (
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	adapters "github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func TestCompleteLayerNamespacesAndBindings(t *testing.T) {
	catalogs, err := configuration.Catalogs(viper.Module(), nacos.Module())
	if err != nil {
		t.Fatal(err)
	}
	parents := map[string]string{
		"fathomry.adapters":                    "",
		"fathomry.adapters.configsource":       "fathomry.adapters",
		"fathomry.adapters.configsource.viper": "fathomry.adapters.configsource",
		"fathomry.adapters.configsource.nacos": "fathomry.adapters.configsource",
		"fathomry.framework":                   "",
		"fathomry.framework.configuration":     "fathomry.framework",
		"fathomry.i18n":                        "",
	}
	modules, err := catalogs.Errors.Modules()
	if err != nil || len(modules) != len(parents) {
		t.Fatalf("unexpected inventory: %v %d", err, len(modules))
	}
	for _, module := range modules {
		parent, exists := parents[module.ID]
		if !exists || module.Parent != parent {
			t.Fatalf("incorrect error topology: %+v", module)
		}
		grouped, found, err := catalogs.Messages.Module(module.ID)
		if err != nil || !found || grouped.Parent != parent {
			t.Fatalf("incorrect resource topology: %+v %v", grouped, err)
		}
	}
	conditions := map[failure.Condition]string{
		source.ErrValue:         "fathomry.adapters.configsource.invalid_value",
		viper.ErrRead:           "fathomry.adapters.configsource.viper.read",
		nacos.ErrRead:           "fathomry.adapters.configsource.nacos.read",
		configuration.ErrSchema: "fathomry.framework.configuration.invalid_schema",
	}
	for condition, want := range conditions {
		if string(condition) != want {
			t.Fatalf("condition %q, want %q", condition, want)
		}
		definition, found, err := catalogs.Errors.Lookup(condition)
		if err != nil || !found {
			t.Fatalf("condition lookup: %v %v", found, err)
		}
		occurrence, err := failure.New(condition)
		if err != nil || !errors.Is(occurrence, condition) {
			t.Fatalf("condition identity changed: %v", err)
		}
		key := definition.Module + ":" + want[len(definition.Module)+1:]
		binding, found, err := catalogs.Bindings.Lookup(key)
		if err != nil || !found || binding.Condition != condition {
			t.Fatalf("binding identity: %v %v", found, err)
		}
		english, err := catalogs.Bindings.Resolve(key, "en")
		if err != nil {
			t.Fatal(err)
		}
		translated, err := catalogs.Bindings.Resolve(key, "zh-CN")
		if err != nil {
			t.Fatal(err)
		}
		var facts []i18n.Argument
		if binding.Input != "" {
			facts = []i18n.Argument{{Name: "source", Value: "fixture"}, {Name: "phase", Value: "capture"}}
		}
		en, err := english.Render(facts)
		if err != nil {
			t.Fatal(err)
		}
		zh, err := translated.Render(facts)
		if err != nil {
			t.Fatal(err)
		}
		metadata, err := translated.Metadata()
		if err != nil || metadata.Resource.Owner != definition.Module || metadata.Resource.Locale != "zh-CN" || metadata.Fallback != "" || en.Text == "" || zh.Text == "" || en.Text == zh.Text {
			t.Fatalf("resource owner or translation mismatch: %+v %v", metadata, err)
		}
	}
	for _, old := range []failure.Condition{"fathomry.configsource.invalid_value", "fathomry.configsource.viper.read", "fathomry.configuration.invalid_schema"} {
		if _, found, err := catalogs.Errors.Lookup(old); err != nil || found {
			t.Fatalf("obsolete unqualified identity remains: %q", old)
		}
	}
}

func TestLayerRegistrationIsExplicitAndRejectsDuplicates(t *testing.T) {
	if _, err := adapters.Catalogs(source.Module(), viper.Module()); err != nil {
		t.Fatal(err)
	}
	if _, err := framework.Catalogs([]framework.Module{configuration.Module()}); err != nil {
		t.Fatal(err)
	}
	if _, err := configuration.Catalogs(viper.Module(), viper.Module()); !errors.Is(err, framework.ErrCatalog) {
		t.Fatalf("duplicate accepted: %v", err)
	}
	if _, err := adapters.Catalogs(adapters.ModuleContribution()); !errors.Is(err, adapters.ErrCatalog) {
		t.Fatalf("built-in duplicate accepted: %v", err)
	}
	if _, err := framework.Catalogs([]framework.Module{framework.ModuleContribution()}); !errors.Is(err, framework.ErrCatalog) {
		t.Fatalf("framework duplicate accepted: %v", err)
	}
}
