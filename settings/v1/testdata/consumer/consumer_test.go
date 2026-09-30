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

package consumer

import (
	"errors"
	"maps"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"example.org/settings-consumer/component"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type account struct {
	Sites []string `json:"sites"`
}

func cloneAccount(value account) account { value.Sites = slices.Clone(value.Sites); return value }

type project struct {
	Application struct {
		I18n    component.Preferences `json:"i18n"`
		Workers int                   `json:"workers"`
	} `json:"application"`
	Custom  map[string]account `json:"custom"`
	created time.Time
}

func cloneProject(value project) project {
	value.Application.I18n = component.Clone(value.Application.I18n)
	value.Custom = maps.Clone(value.Custom)
	for name, account := range value.Custom {
		value.Custom[name] = cloneAccount(account)
	}
	return value
}

func TestProjectAndIndependentComponent(t *testing.T) {
	if _, _, err := component.Language(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("component hid missing application configuration")
	}
	rejected := errors.New("project validation rejected settings")
	var copies atomic.Int32
	prepare := func(value project) (settings.Snapshot[project], error) {
		if value.Application.Workers <= 0 || value.Application.I18n.Language == "" {
			return settings.Snapshot[project]{}, rejected
		}
		return settings.New(value, func(value project) project { copies.Add(1); return cloneProject(value) })
	}
	store := settings.NewStore[project]()
	apply := func(value project) error {
		snapshot, err := prepare(value)
		if err != nil {
			return err
		}
		return store.Publish(snapshot)
	}
	input := project{Custom: map[string]account{"account/one": {Sites: []string{"initial"}}}, created: time.Unix(100, 0)}
	input.Application.Workers = 4
	input.Application.I18n = component.Preferences{Language: "en", Fallbacks: []string{"zh-CN"}}
	if err := apply(input); err != nil {
		t.Fatal(err)
	}
	input.Application.I18n.Fallbacks[0] = "mutated"
	input.Custom["account/one"].Sites[0] = "mutated"
	if err := settings.Configure(store.Reader()); err != nil {
		t.Fatal(err)
	}
	old, err := settings.Default()
	if err != nil {
		t.Fatal(err)
	}
	language, found, err := component.Language()
	if err != nil || !found || language != "en" {
		t.Fatal("component could not read the application preference")
	}
	preferences, found, err := component.Read(old)
	if err != nil || !found || preferences.Fallbacks[0] != "zh-CN" {
		t.Fatal("input copy did not isolate component data")
	}
	preferences.Fallbacks[0] = "changed"
	preferences, _, _ = component.Read(old)
	if preferences.Fallbacks[0] != "zh-CN" || copies.Load() != 1 {
		t.Fatal("section read copied the root or escaped ownership")
	}

	recovered, err := settings.As[project](old)
	if err != nil {
		t.Fatal(err)
	}
	full, err := recovered.ValueCopy()
	if err != nil || !full.created.Equal(time.Unix(100, 0)) || full.Custom["account/one"].Sites[0] != "initial" {
		t.Fatal("custom/private project data lost")
	}
	selected, found, err := settings.Read(old, "/custom/account~1one", cloneAccount)
	if err != nil || !found || selected.Sites[0] != "initial" {
		t.Fatal("project-specific subsection unavailable")
	}
	selected.Sites[0] = "changed"
	selected, _, _ = settings.Read(old, "/custom/account~1one", cloneAccount)
	if selected.Sites[0] != "initial" {
		t.Fatal("custom selection aliased stored data")
	}

	invalid := cloneProject(full)
	invalid.Application.Workers = 0
	if err := apply(invalid); !errors.Is(err, rejected) {
		t.Fatal("strong project validation ignored")
	}
	if language, _, _ := component.Language(); language != "en" {
		t.Fatal("rejected update replaced last-good settings")
	}
	full.Application.I18n.Language = "zh-CN"
	if err := apply(full); err != nil {
		t.Fatal(err)
	}
	if language, _, _ := component.Language(); language != "zh-CN" {
		t.Fatal("component retained a stale copy of the process preference")
	}
	oldPreferences, _, _ := component.Read(old)
	if oldPreferences.Language != "en" {
		t.Fatal("captured operation view changed")
	}

	business := settings.NewStore[account]()
	snapshot, err := settings.New(account{Sites: []string{"separate"}}, cloneAccount)
	if err != nil {
		t.Fatal(err)
	}
	if err := business.Publish(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := settings.Configure(business.Reader()); !errors.Is(err, settings.ErrConfigured) {
		t.Fatal("business domain replaced application")
	}
	if language, _, _ := component.Language(); language != "zh-CN" {
		t.Fatal("business publication changed application")
	}

	_, missingCopy := settings.New(full, nil)
	core, ok := failure.Inspect(missingCopy)
	if !ok || !errors.Is(missingCopy, settings.ErrCopy) || core.Diagnostic().Definition.Code.Domain() != failure.DomainConfiguration {
		t.Fatal("settings error bypassed shared failure")
	}
	catalog, err := failure.Prepare(append(failure.Definitions(), settings.Definitions()...)...)
	if err != nil {
		t.Fatal(err)
	}
	definition, found, err := catalog.Lookup(settings.ErrCopy)
	if err != nil || !found || definition.Identifier != "fathomry.settings.invalid_copy" {
		t.Fatal("numeric error atlas unavailable")
	}
}
