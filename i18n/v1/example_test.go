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
	"embed"
	"fmt"

	"github.com/frost-leo/fathomry/i18n/v1"
)

//go:embed testdata/resources/en.json testdata/resources/ru.json
var exampleResources embed.FS

func ExamplePrepare() {
	english, _ := exampleResources.ReadFile("testdata/resources/en.json")
	russian, _ := exampleResources.ReadFile("testdata/resources/ru.json")
	catalog, err := i18n.Prepare(
		i18n.Source{Name: "inventory.source", Data: english},
		i18n.Source{Name: "inventory.russian", Data: russian},
	)
	if err != nil {
		panic(err)
	}
	definition, err := catalog.Lookup("example.inventory:items", "ru")
	if err != nil {
		panic(err)
	}
	fmt.Println(definition.MessageExists, definition.TranslationExists)
	selection, err := catalog.Resolve("example.inventory:items", "en-US")
	if err != nil {
		panic(err)
	}
	count := uint64(21)
	result, err := selection.Render([]i18n.Argument{{Name: "place", Value: "shelf"}}, &count)
	if err != nil {
		panic(err)
	}
	info, err := selection.Metadata()
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Text)
	fmt.Println(info.Candidate, info.Resource.Locale, result.Category, result.Variant)
	// Output:
	// true true
	// 21 items at shelf
	// en en other other
}
