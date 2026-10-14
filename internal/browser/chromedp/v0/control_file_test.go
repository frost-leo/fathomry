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
	"testing"

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/dom"
	"github.com/chromedp/cdproto/page"
)

func TestFileChooserOriginPrecedesQueuedDeliveryAndBoundsReply(t *testing.T) {
	state := &controlState{session: unitSession(t)}
	if err := state.observeChooser(page.FileChooserOpened.Method, jsonv2.Value(`{"frameId":"frame","mode":"selectSingle","backendNodeId":9}`), 1); err != nil {
		t.Fatal(err)
	}
	pending := &pendingState{control: state, method: page.FileChooserOpened.Method, sequence: 1, chooserNode: 9, chooserMode: page.FileChooserOpenedModeSelectSingle}
	if pending.stale() {
		t.Fatal("current chooser was invalidated")
	}
	files := []string{"/browser-host/file"}
	params, err := FileChooserFiles(files).params(pending)
	if err != nil || params.(dom.SetFileInputFilesParams).BackendNodeID != 9 {
		t.Fatal("chooser identity not bound", err)
	}
	files[0] = "changed"
	if params.(dom.SetFileInputFilesParams).Files[0] != "/browser-host/file" {
		t.Fatal("reply paths alias borrowed container")
	}
	for _, invalid := range [][]string{nil, {"one", "two"}} {
		if _, err := FileChooserFiles(invalid).params(pending); !errors.Is(err, ErrInput) {
			t.Fatal("invalid single selection accepted", err)
		}
	}
	if err := state.observeChooser(page.FrameNavigated.Method, jsonv2.Value(`{"frame":{"id":"frame"}}`), 2); err != nil {
		t.Fatal(err)
	}
	if !pending.stale() {
		t.Fatal("queued chooser retained a departed document")
	}
	if err := state.observeChooser(page.FileChooserOpened.Method, jsonv2.Value(`{"frameId":"frame","mode":"selectMultiple","backendNodeId":10}`), 3); err != nil {
		t.Fatal(err)
	}
	if !pending.stale() {
		t.Fatal("new chooser revived an old backend node")
	}
	for _, data := range []string{`{}`, `{"frameId":"frame","mode":"unknown"}`, `{"frameId":"frame","mode":"selectSingle","backendNodeId":-1}`} {
		if err := state.observeChooser(page.FileChooserOpened.Method, jsonv2.Value(data), 4); err == nil {
			t.Fatal("malformed chooser identity accepted")
		}
	}
	pending.chooserNode = 0
	if _, err := FileChooserFiles([]string{"file"}).params(pending); !errors.Is(err, ErrUnsupported) {
		t.Fatal("non-input chooser invented DOM authority", err)
	}
}
