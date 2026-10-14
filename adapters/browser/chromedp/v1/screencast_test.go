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
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveScreencastUsesBoundedControlAndPreAdmittedFrameAck(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<body style='background:green'>frame fixture</body>", 1)
	params := &page.StartScreencastParams{Format: page.StartScreencastFormatPng, MaxWidth: 160, MaxHeight: 120, MaxFramesInFlight: 1}
	control, receipt, err := retained.Page().Control(bounded(t), ControlOptions{Screencast: params})
	if err != nil {
		t.Fatal(err)
	}
	params.MaxWidth = 1
	frame, err := control.Next(bounded(t))
	if err != nil || frame.Method() != page.ScreencastFrame.Method {
		t.Fatal("native screencast frame missing", err)
	}
	var event page.EventScreencastFrame
	if err := json.Unmarshal(frame.DataCopy(), &event); err != nil || len(event.Data) == 0 || event.Metadata == nil {
		t.Fatal("native frame data or metadata missing", err)
	}
	if !bytes.HasPrefix(event.Data, []byte("\x89PNG\r\n\x1a\n")) {
		t.Fatal("native PNG frame encoding changed")
	}
	if err := frame.Reply(bounded(t), HandleDialog(page.HandleJavaScriptDialogParams{})); !errors.Is(err, ErrInput) {
		t.Fatal("frame token authorized a foreign control reply", err)
	}
	fillControlInbox(t, retained, dependencies)
	if err := frame.Reply(bounded(t), ScreencastFrameAck()); err != nil {
		t.Fatal("frame acknowledgement needed new evidence or ordinary command admission", err)
	}
	if err := frame.Reply(bounded(t), ScreencastFrameAck()); !errors.Is(err, ErrState) {
		t.Fatal("frame token allowed duplicate acknowledgement", err)
	}
	_ = control.Close(bounded(t))
	if !control.Closed() {
		t.Fatal("saturated screencast did not stop")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Primary(), ErrInput) || snapshot.Cleanup() != nil {
		t.Fatal("control outcome and cleanup were conflated", err)
	}
	evidence, _ := snapshot.ValueCopy()
	if !bytes.Contains(evidence.PayloadCopy(), []byte("Page.screencastFrameAck")) || !bytes.Contains(evidence.PayloadCopy(), []byte(`"acknowledged":true`)) {
		t.Fatal("frame acknowledgement lost independent evidence")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("screencast prevented source shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedScreencastStopsWithoutClosingPeer(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if err := sdk.Do(peer, sdk.Navigate("data:text/html,<h1>borrowed screencast</h1>")); err != nil {
		t.Fatal(err)
	}
	control, receipt, err := retained.Page().Control(bounded(t), ControlOptions{Screencast: &page.StartScreencastParams{MaxWidth: 128, MaxHeight: 96}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.Next(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("ignored frame/control did not cleanly stop", err)
	}
	if _, err := receipt.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("73")); err != nil || value != 73 {
		t.Fatal("screencast cleanup killed the borrowed peer", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveCanceledUnconsumedScreencastClosesProducer(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>unconsumed frame</p>", 1)
	ctx, cancel := context.WithCancel(retained.Context())
	control, receipt, err := retained.Page().Control(ctx, ControlOptions{Screencast: &page.StartScreencastParams{MaxWidth: 64, MaxHeight: 64}})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = control.Close(bounded(t))
	if !control.Closed() {
		t.Fatal("unconsumed producer survived cancellation")
	}
	if _, err := receipt.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("unconsumed screencast retained the source")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
