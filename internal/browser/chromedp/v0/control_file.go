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
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/debugger"
	"github.com/chromedp/cdproto/dom"
	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
)

// FileChooserFiles keeps browser-host path data borrowed until Reply returns.
func FileChooserFiles[S ~string](files []S) Reply {
	return Reply{method: dom.SetFileInputFiles.Method, params: func(p *pendingState) (any, error) {
		if p.method != page.FileChooserOpened.Method {
			return nil, failure(ErrInput, "file-chooser-grant")
		}
		if p.chooserNode <= 0 {
			return nil, failure(ErrUnsupported, "file-chooser-without-input")
		}
		if files == nil || p.chooserMode == page.FileChooserOpenedModeSelectSingle && len(files) > 1 {
			return nil, failure(ErrInput, "file-chooser-files")
		}
		if int64(len(files)) > p.control.session.owner.settings.MaxCommandBytes/2 {
			return nil, failure(ErrLimit, "file-chooser-files")
		}
		copied := make([]string, len(files))
		for index, file := range files {
			copied[index] = string(file)
		}
		return dom.SetFileInputFilesParams{Files: copied, BackendNodeID: p.chooserNode}, nil
	}}
}

// Called under the control lock, in native wire order before queue delivery.
func (state *controlState) observeChooser(method string, data jsonv2.Value, sequence uint64) error {
	switch method {
	case page.FileChooserOpened.Method:
		var event page.EventFileChooserOpened
		if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if event.FrameID == "" || event.BackendNodeID < 0 || event.Mode != page.FileChooserOpenedModeSelectSingle && event.Mode != page.FileChooserOpenedModeSelectMultiple {
			return failure(ErrNative, "file-chooser-identity")
		}
		state.chooserFrame, state.chooserCurrent = event.FrameID, sequence
	case page.FrameNavigated.Method:
		var event page.EventFrameNavigated
		if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if event.Frame == nil || event.Frame.ID == "" {
			return failure(ErrNative, "file-chooser-frame")
		}
		if event.Frame.ParentID == "" || event.Frame.ID == state.chooserFrame {
			state.chooserCurrent = 0
		}
	case page.FrameDetached.Method:
		var event page.EventFrameDetached
		if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if event.FrameID == "" {
			return failure(ErrNative, "file-chooser-frame")
		}
		if event.FrameID == state.chooserFrame {
			state.chooserCurrent = 0
		}
	}
	return nil
}

func (pending *pendingState) stale() bool {
	switch pending.method {
	case debugger.Paused.Method:
		return pending.control.debugger == nil || pending.sequence != pending.control.debugger.pause
	case page.JavascriptDialogOpening.Method:
		return pending.sequence != pending.control.dialogCurrent
	case page.FileChooserOpened.Method:
		return pending.sequence != pending.control.chooserCurrent
	default:
		return false
	}
}
