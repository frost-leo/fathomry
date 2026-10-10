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

	"github.com/frost-leo/fathomry/adapters/orchestration/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	policy := orchestration.Policy{
		Budget:          orchestration.Budget{WorkBytes: 11, WorkerBytes: 13, EvidenceBytes: 17},
		Runtime:         adapters.Options{MaxActive: 2, MaxWorkBytes: 41},
		Evidence:        adapters.EvidenceOptions{Capacity: 3, MaxBytes: 43},
		Workers:         adapters.EvidenceOptions{Capacity: 5, MaxBytes: 47},
		Tasks:           adapters.EvidenceOptions{Capacity: 7, MaxBytes: 53},
		SourceWorkBytes: 59, SourceEvidenceBytes: 61, NativeWorkBytes: 67, NativeWorkerBytes: 71,
	}
	copy := policy
	copy.Workers.Capacity = 73
	if policy.Workers.Capacity != 5 || policy.Budget.WorkerBytes != 13 || policy.Tasks.Capacity != 7 {
		panic("independent accounting lost")
	}
	source := orchestration.Attribution{SourceID: "source", Name: "named", Namespace: "namespace", Generation: 3}
	use := source
	use.UseID = 5
	if source.UseID != 0 || use.SourceID != source.SourceID || use.UseID == source.UseID {
		panic("source and use identity conflated")
	}
	if fmt.Sprint(source) != "orchestration[restricted]" {
		panic("ordinary attribution disclosure")
	}
	fmt.Println("orchestration public contracts composed without native I/O")
}
