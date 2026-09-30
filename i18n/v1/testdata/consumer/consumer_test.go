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
	"io/fs"
	"testing"

	"example.org/i18n-consumer/component"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type project struct {
	Application struct {
		I18n i18n.Preferences `json:"i18n"`
	} `json:"application"`
}

func TestIndependentPresentationAndAtlas(t *testing.T) {
	catalog, err := i18n.Prepare(append(i18n.CoreComponents(), component.Bundle())...)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	native := &fs.PathError{Op: "read", Path: "private-native-path", Err: fs.ErrPermission}
	early := component.Fail(presenter, native)
	var shown *i18n.Presented
	if !errors.As(early, &shown) || !errors.Is(shown.Issue(), settings.ErrUnconfigured) || !errors.Is(early, native) {
		t.Fatal("early error was replaced")
	}
	store := settings.NewStore[project]()
	publish := func(locale string) {
		var value project
		value.Application.I18n.Locale = locale
		snapshot, err := settings.New(value, func(value project) project { return value })
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Publish(snapshot); err != nil {
			t.Fatal(err)
		}
	}
	publish("en")
	if err := settings.Configure(store.Reader()); err != nil {
		t.Fatal(err)
	}
	english := component.Fail(presenter, native)
	if !errors.As(english, &shown) || shown.Issue() != nil || shown.Info().Message.Locale != "en" {
		t.Fatal("default English presentation failed")
	}
	chinese, found, err := catalog.Explain(component.SourceFailed, "zh-CN")
	if err != nil || !found || chinese.Message.Locale != "zh-CN" {
		t.Fatal("Chinese code explanation followed English settings")
	}
	var original *fs.PathError
	if !errors.As(english, &original) || original != native || !errors.Is(english, component.SourceFailed) {
		t.Fatal("native/type/numeric identity lost")
	}
	publish("zh-CN")
	later := component.Fail(presenter, native)
	var latest *i18n.Presented
	if !errors.As(later, &latest) || latest.Info().Message.Locale != "zh-CN" || shown.Info().Message.Locale != "en" {
		t.Fatal("language update rewrote old presentation")
	}
	owners, err := catalog.Components()
	if err != nil || len(owners) != 4 {
		t.Fatal("component atlas incomplete")
	}
	coverage, err := catalog.Coverage("fr")
	if err != nil || len(coverage) != 4 {
		t.Fatal("coverage reverse lookup unavailable")
	}
}
