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

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/remote"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestLiveKeepOpenTransfersProcessAndTemporaryProfile(t *testing.T) {
	checkKeepOpen(t, false, false)
}
func TestLiveKeepOpenPreservesPersistentProfile(t *testing.T) {
	checkKeepOpen(t, true, false)
}
func TestLiveVisibleKeepOpenTransfersDefaultWindow(t *testing.T) {
	if os.Getenv("FATHOMRY_CHROMEDP_VISIBLE") != "1" || os.Getenv("DISPLAY") == "" {
		t.Skip("visible X11 fixture not explicitly selected")
	}
	checkKeepOpen(t, false, true)
}

func checkKeepOpen(t *testing.T, persistent, visible bool) {
	t.Helper()
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	settings := Settings{Name: "keep-open", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(1), Flags: map[string]string{"no-first-run": "", "disable-background-networking": ""}}
	if visible {
		settings.Environment = []string{"DISPLAY=" + os.Getenv("DISPLAY")}
		if value := os.Getenv("XDG_RUNTIME_DIR"); value != "" {
			settings.Environment = append(settings.Environment, "XDG_RUNTIME_DIR="+value)
		}
		settings.Flags["ozone-platform"] = "x11"
	} else {
		settings.Flags["headless"] = ""
	}
	marker := ""
	if persistent {
		settings.TempDir = ""
		settings.ProfileDir = t.TempDir()
		marker = filepath.Join(settings.ProfileDir, "caller-marker")
		if err := os.WriteFile(marker, []byte("caller property"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := Prepare(settings, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	oldLife, cancelOld := context.WithCancel(context.Background())
	defer cancelOld()
	owner, err := prepared.Open(oldLife, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	targets, _, err := RunOwnerCommand(bounded(t), owner, target.GetTargets, target.GetTargetsParams{})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	var initial target.ID
	for _, info := range targets.TargetInfos {
		if info.Type == "page" && info.URL == "about:blank" {
			initial = info.TargetID
			break
		}
	}
	if initial == "" {
		t.Fatal("task-owned initial default page not observed")
	}
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true, TargetID: string(initial)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<title>Fathomry transfer fixture</title><h1>Task-owned window</h1>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	keptEarly, refusedReceipt, refuseErr := owner.KeepOpen(context.Background())
	if keptEarly != nil || !errors.Is(refuseErr, ErrState) && !errors.Is(refuseErr, adapters.ErrLimit) {
		t.Fatal("active context was transferred", refuseErr)
	}
	if refusedReceipt != nil {
		drain(t, dependencies, 1)
	}
	if value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool]("true")); err != nil || !value {
		t.Fatal("refused transfer changed the old owner's authority", err)
	}
	drain(t, dependencies, 1)
	if err := retained.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	kept, receipt, err := owner.KeepOpen(context.Background())
	if err != nil || kept == nil {
		t.Fatal("quiescent owned process did not transfer", err)
	}
	t.Cleanup(func() { _ = kept.Close(bounded(t)) })
	profile := string(kept.ProfileDirectory())
	if !owner.ShutdownComplete() || kept.ProcessExited() || kept.Closed() || kept.Endpoint() == "" || profile == "" {
		t.Fatal("transfer confused source release with process exit")
	}
	cancelOld()
	if _, err := os.Stat(profile); err != nil {
		t.Fatal("old source removed transferred profile", err)
	}
	if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("new cleanup owner refunded its process reservation")
	}
	root, _ := owner.state.call.Receipt().Snapshot()
	facts, ok := root.ValueCopy()
	if !ok || !facts.Observation().Transferred || facts.Observation().ProcessExited {
		t.Fatal("old source claimed process termination instead of transfer")
	}
	if _, _, err := RunOwnerCommand(bounded(t), owner, browser.GetVersion, cdp.Empty{}); err == nil {
		t.Fatal("transferred source regained owner authority")
	}
	allocator, stopAllocator := remote.NewAllocator(context.Background(), kept.Endpoint(), remote.NoModifyURL)
	peer, stopPeer := sdk.NewContext(allocator)
	defer stopAllocator()
	defer stopPeer()
	version, err := sdk.CallBrowser(peer, browser.GetVersion, cdp.Empty{})
	if err != nil || version.Product == "" {
		t.Fatal("transferred peer died with its old source", err)
	}
	info, err := sdk.CallBrowser(peer, target.GetTargetInfo, target.GetTargetInfoParams{TargetID: initial})
	if err != nil || info.TargetInfo == nil || info.TargetInfo.Title != "Fathomry transfer fixture" {
		t.Fatal("borrowed default window was destroyed during transfer", err)
	}
	if visible {
		window, err := sdk.CallBrowser(peer, browser.GetWindowForTarget, browser.GetWindowForTargetParams{TargetID: initial})
		if err != nil || window.WindowID == 0 || window.Bounds == nil || window.Bounds.Width <= 0 {
			t.Fatal("visible native window not available", err)
		}
		t.Log("visible X11 launch and native window bounds verified; not a human visual review")
	}
	if err := sdk.Cancel(peer); err != nil {
		t.Fatal(err)
	}
	stopAllocator()
	if kept.ProcessExited() {
		t.Fatal("independent borrowed connection killed transferred process")
	}
	if persistent {
		if _, err := os.Stat(filepath.Join(profile, ".fathomry-browser.lock")); err != nil {
			t.Fatal("profile lock released before process exit", err)
		}
	}
	if err := kept.Close(bounded(t)); err != nil || !kept.ProcessExited() || !kept.Closed() {
		t.Fatal("new owner did not join process/profile cleanup", err)
	}
	if err := kept.Close(bounded(t)); err != nil {
		t.Fatal("repeat cleanup changed outcome", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	result, _ := snapshot.ValueCopy()
	if !result.Observation().Transferred || !result.Observation().ProcessExited {
		t.Fatal("new owner lost actual-exit evidence")
	}
	if persistent {
		if data, err := os.ReadFile(marker); err != nil || string(data) != "caller property" {
			t.Fatal("persistent caller data changed", err)
		}
		if _, err := os.Stat(filepath.Join(profile, ".fathomry-browser.lock")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("closed process retained its task lock", err)
		}
	} else if _, err := os.Stat(profile); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("transferred temporary profile leaked", err)
	}
	if strings.Contains(kept.Endpoint(), "@") {
		t.Fatal("unexpected authenticated fixture endpoint")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedBrowserCannotBeKeptByConnectionOwner(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if err := retained.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if kept, receipt, err := owner.KeepOpen(context.Background()); kept != nil || receipt == nil || !errors.Is(err, ErrState) {
		t.Fatal("external browser ownership was transferred", err)
	}
	drain(t, dependencies, 1)
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal(err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("29")); err != nil || value != 29 {
		t.Fatal("refused transfer damaged borrowed peer", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveKeepOpenNewLifetimeOwnsAutomaticCleanup(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>new cleanup lifetime</p>", 4)
	if err := retained.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	kept, receipt, err := owner.KeepOpen(lifetime)
	if err != nil || !owner.ShutdownComplete() || kept.ProcessExited() {
		t.Fatal("new process lifetime not established", err)
	}
	cancel()
	if _, err := receipt.WaitReleased(bounded(t)); err != nil || !kept.Closed() || !kept.ProcessExited() {
		t.Fatal("canceled new lifetime did not join its process owner", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
