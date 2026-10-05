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

func TestMetadataUsesFrozenSourceAndDetachedResults(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	deps, inbox := testDependencies(t, options)
	owner := testOpen(t, options, deps)
	options.Topics[0] = "unselected"
	options.Brokers[0] = "127.0.0.1:1"
	value, err := owner.Client().Metadata(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	topics := value.TopicsCopy()
	if len(topics) != 1 || topics[0].Name != "records" || topics[0].ID == ([16]byte{}) {
		t.Fatal("settings mutation changed the source")
	}
	topics[0].Name = "changed"
	if value.TopicsCopy()[0].Name != "records" || value.Source().Revision != owner.Info().Revision {
		t.Fatal("source evidence mutated")
	}
}
