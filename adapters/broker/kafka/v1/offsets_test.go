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

package kafka

import (
	"testing"
)

func TestStandaloneCheckpointAbsenceAndExplicitRewind(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	deps, inbox := testDependencies(t, options)
	owner := testOpen(t, options, deps)
	meta, err := owner.Client().Metadata(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	checkpoint := Checkpoint{Topic: "records", TopicID: meta.TopicsCopy()[0].ID, Partition: 0}
	absent, _ := owner.Client().FetchOffsets(testContext(t), []Checkpoint{checkpoint})
	ack(t, inbox)
	if absent.CheckpointsCopy()[0].State != CheckpointAbsent {
		t.Fatal("absent offset was not explicit")
	}
	for _, next := range []int64{3, 0} {
		checkpoint.Next = next
		committed, err := owner.Client().CommitOffsets(testContext(t), []Checkpoint{checkpoint})
		if err != nil || committed.CheckpointsCopy()[0].State != CheckpointCommitted {
			t.Fatal(err)
		}
		ack(t, inbox)
		observed, err := owner.Client().FetchOffsets(testContext(t), []Checkpoint{checkpoint})
		if err != nil || observed.CheckpointsCopy()[0].Checkpoint.Next != next {
			t.Fatal("standalone declaration was treated as a processed ledger", err)
		}
		ack(t, inbox)
	}
}
