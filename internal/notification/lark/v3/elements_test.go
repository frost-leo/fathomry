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

package lark

import (
	"bytes"
	"context"
	"github.com/frost-leo/fathomry/internal/fault"
	"testing"
)

func TestChartEdgesAndElementBoundaries(t *testing.T) {
	for _, raw := range []string{
		`{"type":"line","data":{"values":[]},"xField":"month","yField":"value"}`,
		`{"type":"bar","data":{"values":[{"month":"Localized label","value":-2},{"month":"Zero","value":0}]},"xField":"month","yField":"value"}`,
	} {
		spec := jsonValue(t, raw)
		chart, err := Chart("chart_data", spec)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(chart.Bytes(), []byte(raw)) {
			t.Fatal("chart data silently transformed")
		}
	}
	for _, id := range []string{"", "2starts_digit", "invalid-id", "toolong_element_identity", "a/b"} {
		if elementIDValid(id) {
			t.Fatal("invalid element identity accepted")
		}
	}
	peer := newPeer(t, nil)
	bound := bindTest(t, testOptions(peer), 1)
	if _, err := bound.client.PatchElement(context.Background(), fault.Correlation{Call: "invalid"}, "card_fixture", "intro", Revision{Sequence: 2}, jsonValue(t, `{"tag":"chart"}`)); err == nil {
		t.Fatal("tag mutation accepted in patch")
	}
	if _, err := bound.client.DeleteElement(context.Background(), fault.Correlation{Call: "invalid"}, "card_fixture", "intro", Revision{}); err == nil {
		t.Fatal("zero sequence accepted")
	}
	if len(peer.snapshot()) != 0 {
		t.Fatal("invalid update reached network")
	}
}
