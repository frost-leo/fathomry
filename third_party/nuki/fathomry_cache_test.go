// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package tlsclient

import (
	"testing"

	"github.com/nukilabs/tlsclient/profiles"
	tls "github.com/nukilabs/utls"
)

func TestFathomryTerminalCloseClearsOnlyOwnedSessionCache(t *testing.T) {
	automatic := New(profiles.Chrome150)
	transport := automatic.Transport.(*RoundTripper)
	if transport.ownedSessionCache == nil {
		t.Fatal("native resumable profile lost automatic cache")
	}
	state := new(tls.ClientSessionState)
	transport.clientSessionCache.Put("fixture", state)
	if value, ok := transport.clientSessionCache.Get("fixture"); !ok || value != state {
		t.Fatal("automatic cache control")
	}
	if err := automatic.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := transport.clientSessionCache.Get("fixture"); ok {
		t.Fatal("owned session state retained after Close")
	}
	transport.clientSessionCache.Put("late", state)
	if _, ok := transport.clientSessionCache.Get("late"); ok {
		t.Fatal("terminal cache admitted late state")
	}

	borrowed := tls.NewLRUClientSessionCache(1)
	borrowed.Put("fixture", state)
	client := New(profiles.Chrome150, WithTLSConfig(&tls.Config{ClientSessionCache: borrowed}))
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if value, ok := borrowed.Get("fixture"); !ok || value != state {
		t.Fatal("Close cleared caller's borrowed cache")
	}
}
