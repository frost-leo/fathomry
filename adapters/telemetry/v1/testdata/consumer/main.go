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
	"fmt"

	"github.com/frost-leo/fathomry/adapters/telemetry/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	policy := telemetry.Policy{Budget: telemetry.Budget{WorkBytes: 11, EvidenceBytes: 13},
		Runtime:  adapters.Options{MaxActive: 2, MaxWorkBytes: 17},
		Evidence: adapters.EvidenceOptions{Capacity: 3, MaxBytes: 19}, SourceWorkBytes: 23, SourceEvidenceBytes: 29}
	info := telemetry.Info{Scope: "scope", Provider: "provider", Name: "source", Revision: "revision", FormatVersion: 2,
		Provenance: []telemetry.LayerInfo{{Kind: 1, Fields: []string{"timeout"}}}}
	copy := info.Clone()
	copy.Provenance[0].Fields[0] = "changed"
	if info.Provenance[0].Fields[0] != "timeout" || policy.Budget.WorkBytes != 11 || policy.SourceWorkBytes != 23 {
		panic("category data ownership lost")
	}
	result := telemetry.SignalResult{Signal: telemetry.Logs, Accepted: 2, Submitted: 3, Acknowledged: 1,
		Rejected: 2, TransportCalls: 1, Effect: telemetry.PartialEffect}
	attribution := telemetry.Attribution{Runtime: "runtime", Operation: "export", ID: "call", Sequence: 1,
		Parent: 0, Depth: 0, Source: adapters.Source{Name: "source", Generation: 2}}
	fact := telemetry.Fact{Kind: "declared", Value: "value"}
	option := telemetry.Option{Name: "non-secret", Value: "value"}
	if result.Effect == telemetry.NotAttempted || result.Effect == telemetry.UnknownEffect ||
		result.Acknowledged != 1 || attribution.Source.Generation != 2 || fact.Kind != "declared" || option.Name != "non-secret" {
		panic("category observation facts changed")
	}
	if fmt.Sprint(result) != "telemetry[restricted]" || fmt.Sprint(info) != "telemetry[restricted]" {
		panic("ordinary metadata disclosure")
	}
	fmt.Println("telemetry public contracts composed without native I/O")
}
