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
	"testing"

	"github.com/frost-leo/fathomry/i18n"
)

func FuzzPrepare(f *testing.F) {
	for _, resource := range resources(f) {
		f.Add(resource.Data)
	}
	f.Add([]byte{})
	f.Add([]byte("{"))
	f.Add([]byte(`{"a":1,"a":2}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		catalog, err := i18n.Prepare([]i18n.Resource{{Name: "fuzz.json", Data: data}})
		if err != nil {
			if catalog != nil {
				t.Fatal("partial publication")
			}
			return
		}
		snapshot := catalog.Snapshot()
		if snapshot.ID == "" || len(snapshot.Sources) == 0 || snapshot.Profile != i18n.Profile {
			t.Fatal("invalid prepared snapshot")
		}
		for _, source := range snapshot.Sources {
			result, err := catalog.Render("en", source.ID, nil)
			if err != nil && result != (i18n.Result{}) {
				t.Fatal("partial render failure")
			}
			if len(result.Text) > i18n.MaxOutputBytes {
				t.Fatal("unbounded output")
			}
		}
	})
}

func FuzzRenderBoundaries(f *testing.F) {
	catalog := prepared(f, resources(f))
	f.Add("en", "1", "")
	f.Add("ru", "2", "")
	f.Add("zh-Hans", "", "")
	f.Fuzz(func(t *testing.T, locale, count, name string) {
		result, err := catalog.Render(locale, "demo.files", i18n.Arguments{"Count": i18n.Number(count), "Owner": name})
		if err != nil && result != (i18n.Result{}) {
			t.Fatal("partial render failure")
		}
		if len(result.Text) > i18n.MaxOutputBytes {
			t.Fatal("unbounded output")
		}
	})
}
