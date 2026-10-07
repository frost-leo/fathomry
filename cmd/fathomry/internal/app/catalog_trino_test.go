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
	"errors"
	"strconv"
	"testing"

	"github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestTrinoOfflineCatalog(t *testing.T) {
	for _, definition := range trino.Definitions() {
		for _, identity := range []string{definition.Code.String(), strconv.FormatUint(uint64(definition.Code), 10), string(definition.Identifier)} {
			for _, locale := range []string{"en", "zh-CN"} {
				output, diagnostic, err := execute([]string{"error", "explain", identity, "--output", "json", "--lang", locale}, "")
				var result struct {
					Data struct {
						Definition failure.Definition `json:"definition"`
						Message    string             `json:"message"`
						Locale     string             `json:"locale"`
					} `json:"data"`
				}
				if err != nil || diagnostic != "" || json.Unmarshal([]byte(output), &result) != nil ||
					result.Data.Definition != definition || result.Data.Locale != locale || result.Data.Message == "" {
					t.Fatal("Trino offline explanation changed", identity, locale, err)
				}
				if (result.Data.Message == definition.Message) != (locale == "en") {
					t.Fatal("Trino localization lost baseline or translation")
				}
			}
		}
	}
	output, diagnostic, err := execute([]string{"error", "components", "--module", "fathomry", "--component", "database_trino", "--output", "json"}, "")
	var inventory struct {
		Data []failure.Component `json:"data"`
	}
	if err != nil || diagnostic != "" || json.Unmarshal([]byte(output), &inventory) != nil || len(inventory.Data) != 1 ||
		inventory.Data[0].Facility != failure.FacilityTrino || inventory.Data[0].Domain != failure.DomainDatabase ||
		len(inventory.Data[0].Codes) != len(trino.Definitions()) {
		t.Fatal("Trino offline component inventory incomplete", err)
	}
	output, diagnostic, err = execute([]string{"error", "list", "--component", "database_trino", "--output", "json"}, "")
	var entries struct {
		Data []struct {
			Definition failure.Definition `json:"definition"`
		} `json:"data"`
	}
	if err != nil || diagnostic != "" || json.Unmarshal([]byte(output), &entries) != nil || len(entries.Data) != len(trino.Definitions()) {
		t.Fatal("Trino offline definition inventory incomplete", err)
	}
	for index, entry := range entries.Data {
		if entry.Definition != trino.Definitions()[index] {
			t.Fatal("Trino offline definition identity drifted")
		}
	}
	output, diagnostic, err = execute([]string{"i18n", "coverage", "zh-CN", "--component", "database_trino", "--output", "json"}, "")
	var coverage struct {
		Data struct {
			Components []struct {
				Module    string   `json:"module"`
				Component string   `json:"component"`
				Missing   []string `json:"missing"`
			} `json:"components"`
		} `json:"data"`
	}
	if err != nil || diagnostic != "" || json.Unmarshal([]byte(output), &coverage) != nil || len(coverage.Data.Components) != 1 ||
		coverage.Data.Components[0].Module != "fathomry" || coverage.Data.Components[0].Component != "database_trino" ||
		len(coverage.Data.Components[0].Missing) != 0 {
		t.Fatal("Trino offline Chinese coverage incomplete", err)
	}
	if _, err := settings.Default(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("offline Trino metadata configured an application runtime")
	}
}
