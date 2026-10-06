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

package franz

import "testing"

func TestBudgetUsesNativeDefaults(t *testing.T) {
	for _, test := range []struct {
		name      string
		input     OptionsV1
		sourceMiB int64
	}{
		{"default", OptionsV1{Brokers: []string{"127.0.0.1:1"}}, 164},
		{"transaction", OptionsV1{Brokers: []string{"127.0.0.1:1"}, TransactionalID: "fixture"}, 244},
		{"two_brokers", OptionsV1{Brokers: []string{"127.0.0.1:1", "127.0.0.2:1"}}, 196},
		{"small", OptionsV1{Brokers: []string{"127.0.0.1:1"}, MaxActive: 1, MaxBatchBytes: 1 << 20, MaxWireBytes: 2 << 20}, 32},
		{"queued_group", OptionsV1{Brokers: []string{"127.0.0.1:1"}, QueuedCalls: 8, ConsumerGroup: "group", MaxAssignments: 1}, 164},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := defaults(test.input)
			budget := BudgetV1(test.input)
			limits := value.limits()
			if budget.WorkBytes != value.reservation() || budget.EvidenceBytes != value.evidenceReservation() ||
				budget.Active != limits.Active || budget.Queued != limits.Queued || budget.MaxRecords != value.MaxRecords {
				t.Fatal("budget differs from native prepared fields")
			}
			if budget.SourceBytes != test.sourceMiB<<20 {
				t.Fatal("persistent source/client accounting changed")
			}
			changed := test.input
			changed.MaxRecords = 1
			if BudgetV1(changed).MaxRecords != 1 {
				t.Fatal("explicit record bound lost")
			}
		})
	}
}
