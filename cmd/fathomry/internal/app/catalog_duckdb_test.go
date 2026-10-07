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

package app

import (
	"encoding/json"
	"testing"

	"github.com/frost-leo/fathomry/adapters/sqlengine/duckdb/v1"
	"github.com/frost-leo/fathomry/failure/v1"
)

func TestDuckDBOfflineCatalog(t *testing.T) {
	definitions := duckdb.Definitions()
	for _, locale := range []string{"en", "zh-CN"} {
		output, diagnostic, err := execute([]string{"error", "list", "--component", "database_duckdb", "--lang", locale, "--output", "json"}, "")
		var listing struct {
			Data []struct {
				Definition failure.Definition `json:"definition"`
				Locale     string             `json:"locale"`
				Message    string             `json:"message"`
			} `json:"data"`
		}
		if err != nil || diagnostic != "" || json.Unmarshal([]byte(output), &listing) != nil || len(listing.Data) != len(definitions) {
			t.Fatal("offline DuckDB listing failed", err)
		}
		for index, item := range listing.Data {
			if item.Definition != definitions[index] || item.Locale != locale || item.Message == "" || (item.Message == item.Definition.Message) != (locale == "en") {
				t.Fatal("offline DuckDB listing lost its stable or localized identity")
			}
			for _, identity := range []string{item.Definition.Code.String(), string(item.Definition.Identifier)} {
				output, diagnostic, err := execute([]string{"error", "explain", identity, "--lang", locale, "--output", "json"}, "")
				var explanation struct {
					Data struct {
						Definition failure.Definition `json:"definition"`
						Domain     failure.Domain     `json:"domain"`
						Message    string             `json:"message"`
						Locale     string             `json:"locale"`
					} `json:"data"`
				}
				if err != nil || diagnostic != "" || json.Unmarshal([]byte(output), &explanation) != nil || explanation.Data.Definition != item.Definition || explanation.Data.Domain != failure.DomainDatabase || explanation.Data.Message != item.Message || explanation.Data.Locale != locale {
					t.Fatal("offline DuckDB explanation disagrees with listing", err)
				}
			}
		}
	}
}
