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

func TestDirectConsumerFinalPageDoesNotCommitAtClose(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	deps, inbox := testDependencies(t, options)
	owner := testOpen(t, options, deps)
	position := send(t, owner.Client(), Message{Topic: "records", Value: []byte("page")}).WritesCopy()[0].Position
	ack(t, inbox)
	cursor, err := owner.Client().Consume(testContext(t), Range{Start: position, End: 1})
	if err != nil {
		t.Fatal(err)
	}
	page, err := cursor.Next(testContext(t))
	if err != nil || !page.Page().Complete() {
		t.Fatal(err)
	}
	ack(t, inbox)
	if _, ready := cursor.Receipt().Snapshot(); ready {
		t.Fatal("final page discarded explicit processing authority")
	}
	closed, err := cursor.Close(testContext(t))
	if err != nil || closed.ConsumerProgress().Received != 1 {
		t.Fatal(err)
	}
	ack(t, inbox)
	offsets, _ := owner.Client().FetchOffsets(testContext(t), []Checkpoint{{Topic: position.Topic, TopicID: position.TopicID, Partition: position.Partition}})
	ack(t, inbox)
	if offsets.CheckpointsCopy()[0].State != CheckpointAbsent {
		t.Fatal("direct close committed")
	}
}
