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

package adapters

import (
	"context"
	"testing"
)

func TestEndpointRuntimeIdentityIncludesAliasesAndNotNames(t *testing.T) {
	first, err := New(context.Background(), Options{Name: "same"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(context.Background(), Options{Name: "same"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := first.Close(context.Background()); err != nil {
			t.Error(err)
		}
		if err := second.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	inbox, err := NewInbox[bool](EvidenceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := Bind(first, Declaration[bool]{Evidence: inbox, Copy: func(value bool) bool { return value }})
	if err != nil {
		t.Fatal(err)
	}
	alias := *first
	if !endpoint.UsesRuntime(first) || !endpoint.UsesRuntime(&alias) || endpoint.UsesRuntime(second) || endpoint.UsesRuntime(nil) {
		t.Fatal("runtime identity followed wrapper storage or a non-unique name")
	}
	if (Endpoint[bool]{}).UsesRuntime(first) || endpoint.UsesRuntime(&Runtime{}) {
		t.Fatal("zero handles acquired identity")
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !endpoint.UsesRuntime(&alias) {
		t.Fatal("closure changed historical binding identity")
	}
}
