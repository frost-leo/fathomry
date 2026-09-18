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
	"context"
	"github.com/frost-leo/fathomry/internal/fault"
	"testing"
)

func BenchmarkNativeChartConstruction(b *testing.B) {
	spec := jsonValue(b, `{"type":"line","data":{"values":[{"month":"Jan","items":80},{"month":"Feb","items":120}]},"xField":"month","yField":"items"}`)
	b.ReportAllocs()
	for b.Loop() {
		chart, err := Chart("trend", spec)
		if err != nil {
			b.Fatal(err)
		}
		if _, err = ComposeCard("Monthly items", chart); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkWarmTokenLoopbackSend(b *testing.B) {
	peer := newPeer(b, nil)
	bound := bindTest(b, testOptions(peer), 1)
	content := textContent(b)
	if result := sendTest(b, bound.client, "warm"); result.Err() != nil {
		b.Fatal(result.Err())
	}
	drain(b, bound.inbox)
	b.ReportAllocs()
	for b.Loop() {
		receipt, err := bound.client.Send(context.Background(), fault.Correlation{Call: "benchmark"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "benchmark", content)
		if result := resolved(b, receipt, err); result.Err() != nil {
			b.Fatal(result.Err())
		}
		drain(b, bound.inbox)
	}
}
