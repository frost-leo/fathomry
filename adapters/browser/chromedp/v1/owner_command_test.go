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
	"bytes"
	"errors"
	"testing"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/systeminfo"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveOwnerObservationsRequirePhysicalOwnership(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>owner observations</p>", 4)
	version, receipt, err := RunOwnerCommand(bounded(t), owner, browser.GetVersion, cdp.Empty{})
	if err != nil || version.Product == "" || version.ProtocolVersion == "" {
		t.Fatal("owned browser version missing", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	result, _ := snapshot.ValueCopy()
	version.Product = "mutated-return"
	if bytes.Contains(result.PayloadCopy(), []byte(version.Product)) {
		t.Fatal("owner result aliases its evidence")
	}
	processes, _, err := RunOwnerCommand(bounded(t), owner, systeminfo.GetProcessInfo, cdp.Empty{})
	if err != nil || len(processes.ProcessInfo) == 0 {
		t.Fatal("owner process observation unavailable", err)
	}
	found := false
	for _, process := range processes.ProcessInfo {
		if process.Type == "browser" && process.ID > 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("native browser process absent")
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Command(systeminfo.GetProcessInfo, cdp.Empty{})); !errors.Is(err, ErrUnsupported) {
		t.Fatal("ordinary page obtained process authority", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("owner observation held shutdown")
	}
	if _, _, err := RunOwnerCommand(bounded(t), owner, browser.GetVersion, cdp.Empty{}); err == nil {
		t.Fatal("closed owner regained native authority")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedConnectionOwnerCannotInspectBrowserProcesses(t *testing.T) {
	owner, _, peer, dependencies := borrowedControlFixture(t)
	if _, receipt, err := RunOwnerCommand(bounded(t), owner, systeminfo.GetProcessInfo, cdp.Empty{}); !errors.Is(err, ErrState) || receipt == nil {
		t.Fatal("connection ownership became browser ownership", err)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal(err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("91")); err != nil || value != 91 {
		t.Fatal("refused owner operation affected the external peer", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveOwnerObservationAllowanceIsSharedAndCleanupIsIndependent(t *testing.T) {
	owner, _, dependencies := controlFixture(t, "data:text/html,<p>one owner command</p>", 1)
	if _, _, err := RunOwnerCommand(bounded(t), owner, browser.GetVersion, cdp.Empty{}); err != nil {
		t.Fatal(err)
	}
	copy := *owner
	if _, receipt, err := RunOwnerCommand(bounded(t), &copy, browser.GetVersion, cdp.Empty{}); !errors.Is(err, ErrLimit) || receipt == nil {
		t.Fatal("owner copy reset its command allowance", err)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("saturated owner observation allowance prevented cleanup", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
