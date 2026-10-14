/*
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

package chromedp

import (
	"context"
	"errors"
	"testing"

	"github.com/chromedp/cdproto/cdp"
	sdk "github.com/chromedp/chromedp"
)

func TestNodeFrameRevocationIsScopedAndDoesNotTrackUnobservedFrames(t *testing.T) {
	session := unitSession(t)
	first, second := &nodeFrame{}, &nodeFrame{}
	origin := &objectOrigin{nodeFrames: map[cdp.FrameID]*nodeFrame{"first": first, "second": second}}
	session.objectOrigin = origin
	node := &Node{session: session, native: &sdk.Node{Node: &cdp.Node{NodeID: 1}}, id: 1, frame: first, invalidated: make(chan struct{})}
	origin.invalidateNodeFrame("second", false)
	if err := node.valid(session); err != nil {
		t.Fatal("unrelated frame invalidated node", err)
	}
	for range 1000 {
		origin.invalidateNodeFrame("not-observed", false)
	}
	if len(origin.nodeFrames) != 2 {
		t.Fatal("frame observer grew without retained grants")
	}
	origin.invalidateNodeFrame("first", false)
	if err := node.valid(session); !errors.Is(err, ErrState) {
		t.Fatal("departed frame node survived", err)
	}
	origin.invalidateNodeFrame("root", true)
	if len(origin.nodeFrames) != 0 || second.epoch.Load() != 2 {
		t.Fatal("root replacement retained frame generations")
	}
}

func TestForeignNodeRefusalPrecedesNativeSelectorSetup(t *testing.T) {
	first, second := unitSession(t), unitSession(t)
	first.objectOrigin = &objectOrigin{}
	node := &Node{session: first, native: &sdk.Node{Node: &cdp.Node{NodeID: 1}}, id: 1, invalidated: make(chan struct{})}
	if _, err := RunAction(context.Background(), second, Click(sdk.CSS("input"), FromNode(node))); !errors.Is(err, ErrInput) || second.data.commands != 0 {
		t.Fatal("foreign node reached native query dispatch", err)
	}
}

func TestNodeIDSelectorCountAdmissionPrecedesAccessorAndNativeWork(t *testing.T) {
	session := unitSession(t)
	session.owner.settings.MaxCommandBytes = 64
	called := false
	option := SelectNodes(3, func(int) *Node { called = true; return nil })
	if _, err := RunAction(context.Background(), session, Click("", option)); !errors.Is(err, ErrLimit) || called || session.data.commands != 0 {
		t.Fatal("node collection was expanded before count admission", err)
	}
	for _, option := range []QueryOption{SelectNodes(-1, func(int) *Node { return nil }), SelectNodes(1, nil), SelectNodes(1, func(int) *Node { return nil })} {
		if _, err := RunAction(context.Background(), session, Click("", option)); !errors.Is(err, ErrInput) || session.data.commands != 0 {
			t.Fatal("invalid or nil node reached native dispatch", err)
		}
	}
}
