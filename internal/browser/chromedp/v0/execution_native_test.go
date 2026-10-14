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
	"encoding/json"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestNativeExecutionContextReobservationAndIsolatedWorld(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	retained, err := fixture.client.Retain(deadline(t), fault.Correlation{Call: "execution-probe"}, SessionOptionsV1{ManagedEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close(context.Background())
	session := retained.Page()
	if err := session.Navigate(deadline(t), "data:text/html,<p>execution-context probe</p>"); err != nil {
		t.Fatal(err)
	}
	target := sdk.FromContext(session.native).Target
	events, stop := target.Subscribe(runtime.ExecutionContextCreated.Method)
	defer stop()
	if _, err := cdp.Call(deadline(t), target, runtime.Enable, cdp.Empty{}); err != nil {
		t.Fatal(err)
	}
	var replayed []runtime.ExecutionContextDescription
	timer := time.NewTimer(100 * time.Millisecond)
collect:
	for {
		select {
		case data := <-events:
			var event runtime.EventExecutionContextCreated
			if err := json.Unmarshal(data, &event); err != nil || event.Context == nil || event.Context.UniqueID == "" {
				t.Fatal("native identity missing", err)
			}
			replayed = append(replayed, *event.Context)
		case <-timer.C:
			break collect
		}
	}
	t.Logf("repeated Runtime.enable reobserved contexts=%d", len(replayed))
	tree, err := cdp.Call(deadline(t), target, page.GetFrameTree, cdp.Empty{})
	if err != nil || tree.FrameTree == nil || tree.FrameTree.Frame == nil {
		t.Fatal(err)
	}
	world, err := cdp.Call(deadline(t), target, page.CreateIsolatedWorld, page.CreateIsolatedWorldParams{FrameID: tree.FrameTree.Frame.ID, WorldName: "fathomry-execution-probe"})
	if err != nil || world.ExecutionContextID == 0 {
		t.Fatal(err)
	}
	var created *runtime.ExecutionContextDescription
	for created == nil {
		select {
		case data := <-events:
			var event runtime.EventExecutionContextCreated
			if err := json.Unmarshal(data, &event); err != nil {
				t.Fatal(err)
			}
			if event.Context != nil && event.Context.ID == world.ExecutionContextID {
				created = event.Context
			}
		case <-session.Context().Done():
			t.Fatal("world response had no observable unique origin")
		}
	}
	if created.UniqueID == "" || created.Name != "fathomry-execution-probe" {
		t.Fatal("world provenance missing")
	}
	result, err := cdp.Call(deadline(t), target, runtime.Evaluate, runtime.EvaluateParams{UniqueContextID: created.UniqueID, Expression: "globalThis.contextProbe=41", ReturnByValue: new(true)})
	if err != nil || result.Result == nil || string(result.Result.Value) != "41" {
		t.Fatal("unique-context evaluation failed", err)
	}
	result, err = cdp.Call(deadline(t), target, runtime.Evaluate, runtime.EvaluateParams{Expression: "typeof globalThis.contextProbe", ReturnByValue: new(true)})
	if err != nil || result.Result == nil || string(result.Result.Value) != `"undefined"` {
		t.Fatal("isolated evaluation affected the default world", err)
	}
	if err := session.Navigate(deadline(t), "data:text/html,<p>replacement</p>"); err != nil {
		t.Fatal(err)
	}
	if _, err := cdp.Call(deadline(t), target, runtime.Evaluate, runtime.EvaluateParams{UniqueContextID: created.UniqueID, Expression: "globalThis.staleProbe=1"}); err == nil {
		t.Fatal("retired unique context fell back to default")
	}
}

func TestNativePollKeepsDefaultWorldAfterIsolatedWorldCreation(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	retained, err := fixture.client.Retain(deadline(t), fault.Correlation{Call: "default-world"}, SessionOptionsV1{ManagedEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close(context.Background())
	session := retained.Page()
	if _, err := RunAction(deadline(t), session, Evaluate[int]("globalThis.worldProbe=17")); err != nil {
		t.Fatal(err)
	}
	frames, err := session.Frames(deadline(t))
	if err != nil || len(frames) != 1 {
		t.Fatal(err)
	}
	world, err := session.CreateWorld(deadline(t), frames[0], page.CreateIsolatedWorldParams{WorldName: "poll-world"})
	if err != nil {
		t.Fatal(err)
	}
	group, err := session.NewObjectGroup(session.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := group.EvaluateIn(deadline(t), world, runtime.EvaluateParams{Expression: "globalThis.worldProbe=29"}); err != nil {
		t.Fatal(err)
	}
	// The Runtime event passes through the native frame cache on another loop.
	time.Sleep(30 * time.Millisecond)
	value, err := RunAction(deadline(t), session, Poll[int]("globalThis.worldProbe"))
	if err != nil || value != 17 {
		t.Fatalf("native Poll selected the wrong world: got=%d err=%v", value, err)
	}
}
