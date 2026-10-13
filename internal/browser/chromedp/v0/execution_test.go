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
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

func TestExecutionAndFrameRejectMissingForeignAndRetiredOrigins(t *testing.T) {
	first, second := unitSession(t), unitSession(t)
	grant := &ExecutionContext{session: first, origin: sdk.ExecutionOrigin{ID: 7, UniqueID: "observed"}}
	if err := grant.valid(second); !errors.Is(err, ErrInput) {
		t.Fatal("foreign page adopted context identity", err)
	}
	for _, value := range []*ExecutionContext{nil, {}, {session: first, origin: sdk.ExecutionOrigin{ID: 7}}} {
		if err := value.valid(first); !errors.Is(err, ErrInput) {
			t.Fatal("incomplete identity authorized evaluation", err)
		}
	}
	if _, err := first.executionOrigins(1); !errors.Is(err, ErrState) {
		t.Fatal("missing native target exposed identities", err)
	}
	first.objectOrigin = &objectOrigin{}
	frame := &Frame{session: first, id: cdp.FrameID("owned"), stamp: &nodeFrame{}}
	if err := frame.valid(second); !errors.Is(err, ErrInput) {
		t.Fatal("foreign page adopted frame identity", err)
	}
	if err := frame.valid(first); err != nil {
		t.Fatal(err)
	}
	frame.stamp.epoch.Add(1)
	if err := frame.valid(first); !errors.Is(err, ErrState) {
		t.Fatal("frame document retirement was ignored", err)
	}
	for _, value := range []*Frame{nil, {}, {session: first, id: "missing-stamp"}} {
		if err := value.valid(first); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
	}
	if _, err := (&ObjectGroup{}).EvaluateIn(context.Background(), grant, runtime.EvaluateParams{}); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if _, err := second.CreateWorld(context.Background(), frame, page.CreateIsolatedWorldParams{}); !errors.Is(err, ErrInput) || second.data.commands != 0 {
		t.Fatal("foreign frame reached command preparation", err)
	}
}

func TestFrameRetirementCancelsOnlyItsAdmittedPolls(t *testing.T) {
	first, cancelFirst := context.WithCancelCause(context.Background())
	second, cancelSecond := context.WithCancelCause(context.Background())
	defer cancelFirst(nil)
	defer cancelSecond(nil)
	left, right := &nodeFrame{}, &nodeFrame{}
	left.polls = map[*framePoll]struct{}{&framePoll{cancel: cancelFirst}: {}}
	right.polls = map[*framePoll]struct{}{&framePoll{cancel: cancelSecond}: {}}
	origin := &objectOrigin{nodeFrames: map[cdp.FrameID]*nodeFrame{"left": left, "right": right}}
	origin.invalidateNodeFrame("left", false)
	if !errors.Is(context.Cause(first), ErrState) || second.Err() != nil {
		t.Fatal("frame revocation canceled unrelated work or lost the cause")
	}
	origin.invalidateNodeFrame("", true)
	if !errors.Is(context.Cause(second), ErrState) || len(origin.nodeFrames) != 0 {
		t.Fatal("root retirement retained frame work")
	}
}

func TestWorldAttemptQuotaPrecedesParameterEncodingAndDispatch(t *testing.T) {
	session := unitSession(t)
	session.owner.settings.MaxEvents = 1
	session.owner.settings.MaxCommandBytes = 16
	session.worldAttempts = 1
	session.objectOrigin = &objectOrigin{}
	frame := &Frame{session: session, id: "frame", stamp: &nodeFrame{}}
	input := page.CreateIsolatedWorldParams{WorldName: "more than the configured command bytes"}
	if world, err := session.CreateWorld(context.Background(), frame, input); world != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("world quota did not refuse before native setup", err)
	}
	if session.worldAttempts != 1 || input.FrameID != "" {
		t.Fatal("refusal changed custody or caller input")
	}
}
