/*
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

package resource

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestSourceNeutralDataUsesPreparationWithoutResourceAuthority(t *testing.T) {
	type settings struct {
		Values map[string]string `json:"values"`
	}
	prepared, err := PrepareData(Schema[settings]{Format: 2, Defaults: settings{Values: map[string]string{"base": "kept"}}},
		[]Layer{{Kind: Local, Content: []byte("values: {local: added}")}})
	if err != nil {
		t.Fatal(err)
	}
	description := prepared.Description()
	if description.Identity != (Identity{}) || description.Format != 2 || description.Revision == "" {
		t.Fatal("data preparation invented source identity")
	}
	first, err := prepared.Copy()
	if err != nil {
		t.Fatal(err)
	}
	first.Values["base"] = "changed"
	second, _ := prepared.Copy()
	if !reflect.DeepEqual(second.Values, map[string]string{"base": "kept", "local": "added"}) {
		t.Fatal("Copy aliases frozen data")
	}
	called := false
	selected := Select(prepared, func(context.Context, settings) (Resource[int], error) { called = true; return Resource[int]{}, nil })
	assembly, err := Assemble(context.Background(), context.Background(), "test", selected)
	if !errors.Is(err, ErrSelection) || assembly != nil || called {
		t.Fatal("source-neutral data granted resource construction authority")
	}
	if _, err := (Prepared[settings]{}).Copy(); err == nil {
		t.Fatal("zero data yielded usable settings")
	}
	if _, err := PrepareData(Schema[settings]{}, nil); !errors.Is(err, ErrConfiguration) {
		t.Fatal("zero format accepted")
	}
}
