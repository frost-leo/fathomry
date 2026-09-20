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

package i18n_test

import (
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/i18n"
)

func TestSettingsSelectionAndValidation(t *testing.T) {
	defaults := i18n.DefaultSettings()
	if defaults.DefaultLocale != "en" {
		t.Fatal("default source changed")
	}
	selected := defaults
	selected.DefaultLocale = "zh-CN"
	if defaults.DefaultLocale != "en" {
		t.Fatal("settings are not independent")
	}
	if locale, err := selected.Locale(""); err != nil || locale != "zh-CN" {
		t.Fatal("project locale not selected")
	}
	if locale, err := selected.Locale("en-US"); err != nil || locale != "en-US" {
		t.Fatal("invocation override not selected")
	}
	for _, value := range []string{"", "not_a_valid_locale!", "en-u-ca-gregory"} {
		settings := i18n.Settings{DefaultLocale: value}
		if !errors.Is(settings.Validate(), i18n.InvalidLocale) {
			t.Fatal("invalid project locale accepted")
		}
		if locale, err := settings.Locale("en"); err == nil || locale != "" {
			t.Fatal("override hid invalid settings")
		}
	}
	if locale, err := defaults.Locale("invalid!"); err == nil || locale != "" {
		t.Fatal("invalid override accepted")
	}
}
