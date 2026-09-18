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

package main

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/i18n"
)

func TestConsumerOwnsEmbeddedResources(t *testing.T) {
	var inputs []i18n.Resource
	for _, name := range []string{"en.json", "zh.json"} {
		data, err := files.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, i18n.Resource{Name: name, Data: data})
	}
	catalog, err := i18n.Prepare(inputs)
	if err != nil {
		t.Fatal(err)
	}
	goldenBytes, err := files.ReadFile("golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		En string
		Zh string
	}
	if err := json.Unmarshal(goldenBytes, &golden); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for index := 0; index < 8; index++ {
		group.Go(func() {
			for repeat := 0; repeat < 25; repeat++ {
				for _, pair := range [][2]string{{"en", golden.En}, {"zh-CN", golden.Zh}} {
					result, err := catalog.Render(pair[0], "business.receipt", i18n.Arguments{"Count": i18n.Number("2")})
					if err != nil || result.Text != pair[1] {
						t.Errorf("consumer render mismatch: %v", err)
						return
					}
				}
			}
		})
	}
	group.Wait()
}
