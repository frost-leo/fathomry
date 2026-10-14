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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/remote"
)

func borrowedControlFixture(t *testing.T) (*Owner, *Context, context.Context, Dependencies) {
	t.Helper()
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	profile := t.TempDir()
	allocator, stopAllocator := sdk.NewExecAllocator(context.Background(), sdk.ExecPath(path), sdk.Headless, sdk.NoFirstRun, sdk.Flag("disable-background-networking", true), sdk.UserDataDir(profile), remote.WebSocket)
	t.Cleanup(stopAllocator)
	peer, stopPeer := sdk.NewContext(allocator)
	t.Cleanup(func() {
		if err := sdk.Cancel(peer); err != nil {
			t.Error(err)
		}
		stopPeer()
	})
	if err := sdk.Do(peer); err != nil {
		t.Fatal(err)
	}
	info, infoErr := cdp.Call(bounded(t), sdk.FromContext(peer).Browser, target.GetTargetInfo, target.GetTargetInfoParams{TargetID: sdk.FromContext(peer).Target.TargetID})
	if infoErr != nil {
		t.Fatal(infoErr)
	}
	t.Logf("fixture target identity matches=%t; default browser-context identity empty=%t", info.TargetInfo.TargetID == sdk.FromContext(peer).Target.TargetID, info.TargetInfo.BrowserContextID == "")
	raw, err := os.ReadFile(filepath.Join(profile, "DevToolsActivePort"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatal("fixture endpoint missing")
	}
	prepared, err := Prepare(Settings{Name: "borrowed-controls", RemoteURL: "ws://127.0.0.1:" + lines[0] + lines[1], MaxSessions: new(1)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true, TargetID: string(sdk.FromContext(peer).Target.TargetID)})
	if err != nil {
		t.Fatal(err)
	}
	return owner, retained, peer, dependencies
}

func TestLiveBorrowedControlCleanupKeepsConnectionUntilDisableAndPeerAlive(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Fetch: &fetch.EnableParams{}, Dialogs: true})
	if err != nil {
		t.Fatal(err)
	}
	// Do not manually close control or context: source shutdown must do the work.
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("source could not clean borrowed target controls", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || snapshot.Cleanup() != nil {
		t.Fatal("cleanup connection disappeared too early", err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("31")); err != nil || value != 31 {
		t.Fatal("borrowed peer or existing page was destroyed", err)
	}
	if err := sdk.Do(peer, sdk.Navigate("data:text/html,<h1>peer remains usable</h1>")); err != nil {
		t.Fatal("interception remained installed", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveControlIgnoredConstructorCancellationClosesProducer(t *testing.T) {
	_, retained, dependencies := controlFixture(t, "data:text/html,<h1>canceled controller</h1>", 8)
	ctx, cancel := context.WithCancel(retained.Context())
	control, receipt, err := retained.Page().Control(ctx, ControlOptions{Fetch: &fetch.EnableParams{}, Dialogs: true})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := receipt.WaitReleased(bounded(t)); err != nil || !control.Closed() {
		t.Fatal("unused controller stayed live", err)
	}
	drain(t, dependencies, 1)
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<h1>after cancellation</h1>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
}

func TestLiveSourceReconcilesUncertainControlCustodyBeforePublicRelease(t *testing.T) {
	endpoint, peer := cleanupFaultPeer(t, map[string]int64{"Fetch.disable": -1, "Target.disposeBrowserContext": 1})
	prepared, err := Prepare(Settings{Name: "control-reconcile", RemoteURL: endpoint}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Fetch: &fetch.EnableParams{}})
	if err != nil {
		t.Fatal(err)
	}
	// Fetch.disable is never acknowledged. The first context disposal also fails.
	// Only the source's second disposal can establish termination of this child.
	for range 3 {
		_ = owner.Close(bounded(t))
		if owner.ShutdownComplete() {
			break
		}
	}
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("uncertain child prevented source-level reconciliation forever")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	var nativeError *cdproto.Error
	if !errors.As(snapshot.Cleanup(), &nativeError) || nativeError.Code != -32001 {
		t.Fatal("cleanup history erased by eventual context disposal")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("47")); err != nil || value != 47 {
		t.Fatal("source reconciliation stopped borrowed browser", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
