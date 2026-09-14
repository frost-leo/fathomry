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

package iceberg

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestOptionsRejectBeforeConstructionAndFreezeLayers(t *testing.T) {
	peer, options := newPeer(t)
	for _, change := range []func(*OptionsV1){
		func(o *OptionsV1) { o.Version = 2 },
		func(o *OptionsV1) { o.Namespace = "../outside" },
		func(o *OptionsV1) { o.Location = "s3://fixture/owned/../" },
		func(o *OptionsV1) { o.CatalogURI += "?unexpected=yes" },
		func(o *OptionsV1) { o.MaxActive = 5 },
		func(o *OptionsV1) { o.StorageSecretKey = "" },
		func(o *OptionsV1) { o.Location = "s3://fixture/" + strings.Repeat("a", 961) + "/" },
		func(o *OptionsV1) { o.Warehouse = "invalid\x00name" },
	} {
		invalid := options
		change(&invalid)
		if _, err := Select(invalid); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	peer.mu.Lock()
	requests := peer.requests
	peer.mu.Unlock()
	if requests != 0 {
		t.Fatal("invalid selection performed I/O")
	}
	content := []byte(`{"max_rows":32}`)
	selected, err := Select(options, resource.Layer{Kind: resource.Local, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	options.StorageSecretKey = "changed"
	clear(content)
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(deadline(t), deadline(t), "frozen", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(deadline(t))
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	if source.owner.settings.MaxRows != 32 || source.owner.settings.StorageSecretKey != "test-secret" {
		t.Fatal("selection did not freeze effective settings")
	}
}
func TestEmptyTableNamesAndZeroClientsAreRefused(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 2)
	if _, err := f.client.InspectTable(deadline(t), correlation("empty"), ""); !errors.Is(err, ErrInput) {
		t.Fatal("empty name accepted")
	}
	var client *Client
	if _, err := client.ListTables(context.Background(), correlation("zero")); !errors.Is(err, ErrInput) {
		t.Fatal("nil client not refused")
	}
}
