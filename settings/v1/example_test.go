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

package settings_test

import (
	"errors"
	"fmt"
	"slices"

	"github.com/frost-leo/fathomry/settings/v1"
)

func ExampleNewStore() {
	type preferences struct {
		Language  string   `json:"language"`
		Fallbacks []string `json:"fallbacks"`
	}
	type project struct {
		Presentation preferences `json:"presentation"`
		Workers      int         `json:"workers"`
	}
	clonePreferences := func(value preferences) preferences {
		value.Fallbacks = slices.Clone(value.Fallbacks)
		return value
	}
	cloneProject := func(value project) project {
		value.Presentation = clonePreferences(value.Presentation)
		return value
	}
	input := project{Presentation: preferences{Language: "en", Fallbacks: []string{"zh-CN"}}, Workers: 4}
	if input.Workers <= 0 {
		panic("project requires positive workers")
	}
	snapshot, err := settings.New(input, cloneProject)
	if err != nil {
		panic(err)
	}
	store := settings.NewStore[project]()
	if err := store.Publish(snapshot); err != nil {
		panic(err)
	}
	captured, err := store.Reader().Capture()
	if err != nil {
		panic(err)
	}
	input.Presentation.Language = "zh-CN"
	next, err := settings.New(input, cloneProject)
	if err != nil {
		panic(err)
	}
	if err := store.Publish(next); err != nil {
		panic(err)
	}
	current, err := store.Reader().Capture()
	if err != nil {
		panic(err)
	}
	before, found, err := settings.Read(captured, "/presentation", clonePreferences)
	if err != nil || !found {
		panic("missing captured preferences")
	}
	after, found, err := settings.Read(current, "/presentation", clonePreferences)
	if err != nil || !found {
		panic("missing current preferences")
	}
	fmt.Println(before.Language, after.Language)
	// Output:
	// en zh-CN
}

func ExampleAs() {
	type project struct{ Workers int }
	snapshot, err := settings.New(project{Workers: 4}, func(value project) project { return value })
	if err != nil {
		panic(err)
	}
	recovered, err := settings.As[project](snapshot.View())
	if err != nil {
		panic(err)
	}
	copied, err := recovered.ValueCopy()
	if err != nil {
		panic(err)
	}
	_, wrongType := settings.As[struct{ Workers int }](snapshot.View())
	fmt.Println(copied.Workers, errors.Is(wrongType, settings.ErrType))
	// Output:
	// 4 true
}
