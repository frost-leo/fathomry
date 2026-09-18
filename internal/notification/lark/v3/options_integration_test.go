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

package lark

import (
	"context"
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestStrictOverlaysAndMatchingLimits(t *testing.T) {
	peer := newPeer(t, nil)
	options := testOptions(peer)
	for _, content := range []string{"max_active: 2\nmax_active: 3\n", "unknown_option: true\n", "timeout_ns: wrong\n", "native_client: {}\n"} {
		if _, err := Select(options, resource.Layer{Kind: resource.Local, Content: []byte(content)}); err == nil {
			t.Fatal("ambiguous overlay accepted")
		}
	}
	selected, err := Select(options, resource.Layer{Kind: resource.Local, Content: []byte("max_response_bytes: 1048576\n")})
	if err != nil {
		t.Fatal(err)
	}
	undersized := resource.WithLimits(selected, resource.Limits{Active: 1, Bytes: 1024, MaxLeases: 1})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "undersized", undersized)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	inbox, _ := invocation.NewInbox[Result](1, 1<<20)
	if _, err = Bind(assembly, undersized, inbox, nil); !errors.Is(err, ErrInput) {
		t.Fatal("undersized policy bound")
	}
	if len(peer.snapshot()) != 0 {
		t.Fatal("validation performed network work")
	}
}
