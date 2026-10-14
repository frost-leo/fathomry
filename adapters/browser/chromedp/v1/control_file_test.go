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
	"os"
	"path/filepath"
	"testing"

	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
)

func TestLiveFileChooserReplyAtSaturationAndStaleDocumentRefusal(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<input id='file' type='file'>", 24)
	file := filepath.Join(t.TempDir(), "chooser.txt")
	if err := os.WriteFile(file, []byte("synthetic chooser bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{FileChooser: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Click("#file")); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	pending, err := control.Next(bounded(t))
	if err != nil || pending.Method() != page.FileChooserOpened.Method {
		t.Fatal("native input chooser missing", err)
	}
	if err := pending.Reply(bounded(t), FileChooserFiles(nil)); !errors.Is(err, ErrInput) {
		t.Fatal("nil files consumed input chooser", err)
	}
	if err := pending.Reply(bounded(t), HandleDialog(page.HandleJavaScriptDialogParams{})); !errors.Is(err, ErrInput) {
		t.Fatal("chooser became a dialog grant", err)
	}
	exhausted := false
	for range 32 {
		_, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool]("true"))
		drain(t, dependencies, 1)
		if errors.Is(err, ErrLimit) {
			exhausted = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !exhausted {
		t.Fatal("ordinary command saturation not reached")
	}
	fillControlInbox(t, retained, dependencies)
	if err := pending.Reply(bounded(t), FileChooserFiles([]browser.HostPath{browser.HostPath(file)})); err != nil {
		t.Fatal("file chooser reply depended on saturated ordinary work", err)
	}
	if err := pending.Reply(bounded(t), FileChooserFiles([]browser.HostPath{browser.HostPath(file)})); !errors.Is(err, ErrState) {
		t.Fatal("input token reused", err)
	}
	_ = control.Close(bounded(t))
	if !control.Closed() {
		t.Fatal("file chooser interception did not disable")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := snapshot.ValueCopy()
	if !bytes.Contains(value.PayloadCopy(), []byte("DOM.setFileInputFiles")) || !bytes.Contains(value.PayloadCopy(), []byte(`"acknowledged":true`)) {
		t.Fatal("ignored chooser reply lost its independent evidence")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("saturated chooser prevented source cleanup")
	}
	if data, err := os.ReadFile(file); err != nil || string(data) != "synthetic chooser bytes" {
		t.Fatal("provider changed caller-owned upload file", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedFileChooserRetainsHostSemanticsAndRevokesOldFrame(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if err := sdk.Do(peer, sdk.Navigate("data:text/html,<input id='file' type='file'>")); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "remote-host-chooser.txt")
	if err := os.WriteFile(file, []byte("cohost remote chooser"), 0600); err != nil {
		t.Fatal(err)
	}
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{FileChooser: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := sdk.Do(peer, sdk.Click("#file")); err != nil {
		t.Fatal(err)
	}
	pending, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := pending.Reply(bounded(t), FileChooserFiles([]browser.HostPath{browser.HostPath(file)})); err != nil {
		t.Fatal(err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[string]("document.querySelector('#file').files[0].text()", sdk.EvalAwaitPromise)); err != nil || value != "cohost remote chooser" {
		t.Fatal("browser-host file data not acquired", err)
	}
	if err := sdk.Do(peer, sdk.Click("#file")); err != nil {
		t.Fatal(err)
	}
	stale, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<input id='newfile' type='file'>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if err := stale.Reply(bounded(t), FileChooserFiles([]browser.HostPath{browser.HostPath(file)})); !errors.Is(err, ErrState) {
		t.Fatal("old document chooser retained authority", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("borrowed chooser did not detach")
	}
	if count, err := sdk.Run(peer, sdk.Evaluate[int]("document.querySelector('#newfile').files.length")); err != nil || count != 0 {
		t.Fatal("stale chooser changed a new input or destroyed the peer", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
