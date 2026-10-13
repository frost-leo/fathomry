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
	"errors"
	"os"
	"path/filepath"
	"testing"

	sdk "github.com/chromedp/chromedp"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
)

func TestLiveUploadUsesBrowserHostFilesAndPreservesCallerFile(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<input id='file' type='file' multiple>", 64)
	file := filepath.Join(t.TempDir(), "browser-host-fixture.txt")
	content := []byte("synthetic upload bytes")
	if err := os.WriteFile(file, content, 0600); err != nil {
		t.Fatal(err)
	}
	paths := []browser.HostPath{browser.HostPath(file)}
	if _, _, err := RunAction(bounded(t), retained.Page(), SetUploadFiles("#file", paths)); err != nil {
		t.Fatal(err)
	}
	paths[0] = "/not-the-selected-path"
	text, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string]("document.querySelector('#file').files[0].text()", EvalAwaitPromise()))
	if err != nil || text != string(content) {
		t.Fatal("browser did not acquire the explicitly selected host file", err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), SetUploadFiles("#file", []browser.HostPath{})); err != nil {
		t.Fatal("native empty-file selection failed", err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Clear("#file")); err != nil {
		t.Fatal("native explicit file-input clear failed", err)
	}
	count, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int]("document.querySelector('#file').files.length"))
	if err != nil || count != 0 {
		t.Fatal("clearing selected files did not preserve native semantics", err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), SetUploadFiles("#file", nil)); !errors.Is(err, ErrInput) {
		t.Fatal("nil upload paths were accepted", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("file input retained browser ownership")
	}
	if data, err := os.ReadFile(file); err != nil || string(data) != string(content) {
		t.Fatal("provider deleted or changed a caller-owned upload file", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveRemoteUploadHasExplicitBrowserHostSemantics(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if err := sdk.Do(peer, sdk.Navigate("data:text/html,<input id='file' type='file'>")); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "peer-host-fixture.txt")
	if err := os.WriteFile(file, []byte("same-host remote control fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := sdk.Do(peer, sdk.SetUploadFiles("#file", []string{file}), sdk.SetUploadFiles("#file", []string{})); err != nil {
		t.Fatal(err)
	}
	nativeEmptyCount, err := sdk.Run(peer, sdk.Evaluate[int]("document.querySelector('#file').files.length"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), SetUploadFiles("#file", []browser.HostPath{browser.HostPath(file)})); err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), SetUploadFiles("#file", []browser.HostPath{})); err != nil {
		t.Fatal(err)
	}
	publicEmptyCount, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int]("document.querySelector('#file').files.length"))
	if err != nil || publicEmptyCount != nativeEmptyCount {
		t.Fatal("explicit empty files changed native behavior", err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), SetUploadFiles("#file", []browser.HostPath{browser.HostPath(file)})); err != nil {
		t.Fatal(err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("remote file-input connection did not detach")
	}
	value, err := sdk.Run(peer, sdk.Evaluate[string]("document.querySelector('#file').files[0].text()", sdk.EvalAwaitPromise))
	if err != nil || value != "same-host remote control fixture" {
		t.Fatal("remote cleanup destroyed the peer or caller's selected file", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
