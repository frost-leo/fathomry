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
	"github.com/frost-leo/fathomry/adapters/httpclient/nethttp/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"testing"
)

func TestNetHTTPOfflineAtlas(t *testing.T) {
	for _, definition := range nethttp.Definitions() {
		for _, identity := range []string{definition.Code.String(), string(definition.Identifier)} {
			output, diagnostic, err := execute([]string{"error", "explain", identity, "--output", "json", "--lang", "zh-CN"}, "")
			var result struct {
				Data struct {
					Definition      failure.Definition
					Message, Locale string
				}
			}
			if err != nil || diagnostic != "" || json.Unmarshal([]byte(output), &result) != nil || result.Data.Definition != definition ||
				result.Data.Locale != "zh-CN" || result.Data.Message == definition.Message {
				t.Fatal("NetHTTP offline declaration/localization omitted", err)
			}
		}
	}
}
