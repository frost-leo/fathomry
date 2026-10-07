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

package httpclient

import (
	"encoding/json"
	"testing"
)

func TestInfoCopyAndRestrictedAttribution(t *testing.T) {
	value := Info{Provider: "httpclient.nethttp.v1", Provenance: []LayerInfo{{Fields: []string{"timeout_ns"}}}}
	copy := value.Clone()
	copy.Provenance[0].Fields[0] = "changed"
	if value.Provenance[0].Fields[0] != "timeout_ns" {
		t.Fatal("provenance alias")
	}
	for _, value := range []any{value, Attribution{ID: "private"}} {
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime attribution became durable")
		}
	}
}
