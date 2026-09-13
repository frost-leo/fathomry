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

import (
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestMetadataAndProfileCopies(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 2)
	receipt, err := fixture.client.Metadata(deadline(t), correlation("snapshot"))
	result := settle(t, receipt, err)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	first := result.Outcome.Value.TopicsCopy()
	originalID := first[0].ID
	first[0].ID[0] ^= 1
	first[0].Name = "changed"
	first[0].Partitions = 99
	fresh := result.Outcome.Value.TopicsCopy()
	if fresh[0].ID != originalID || fresh[0].Name == "changed" || fresh[0].Partitions != 2 || fresh[0].leaders != nil {
		t.Fatal("metadata snapshot aliased or retained routing resources")
	}
	profile := fixture.client.Profile()
	profile.Options[0].Value = "changed"
	if fixture.client.Profile().Options[0].Value == "changed" {
		t.Fatal("profile aliased")
	}
}
func TestBorrowedKafkaSourceKeepsOriginalOwnership(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 8)
	selected := resource.Borrow("alias", fixture.assembly, fixture.selected)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "borrower", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(deadline(t)); err != nil {
			t.Error(err)
		}
	})
	inbox, err := invocation.NewInbox[Result](1, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := client.Produce(deadline(t), correlation("borrowed"), []Message{{Topic: "records"}})
	result := settle(t, receipt, err)
	if result.Err() != nil || result.Source.Scope != "test" || result.Source.Configuration.Identity.Name != "data" || result.Source.Configuration.Revision != fixture.client.access.Info().Configuration.Revision {
		t.Fatal("borrowing changed source attribution")
	}
	delivery, err := inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	produce(t, fixture.client, "owner-still-open", Message{Topic: "records"})
}
func TestUnexpectedBrokerSetFailsAssembly(t *testing.T) {
	cluster := localCluster(t)
	other := localCluster(t)
	options := clusterOptions(cluster)
	options.Brokers = append(options.Brokers, other.ListenAddrs()...)
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(deadline(t), deadline(t), "wrong-set", selected)
	if !errors.Is(err, ErrAuthority) {
		t.Fatal("extra undeclared-cluster seed was accepted", err)
	}
	if assembly == nil {
		t.Fatal("failed construction lost cleanup ownership")
	}
	if err := assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}
