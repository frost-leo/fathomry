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

	"github.com/chromedp/cdproto/browser"
	category "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// DownloadOptions enables native allowAndName policy for the retained context,
// with target-scoped begin/progress events in Control.Next. Only one Control in
// that context can set download policy at a time. Other pages share the policy
// but their events and transfers are not owned by this page controller.
//
// Exactly one destination is required. LocalDirectory is an existing absolute,
// caller-owned directory and requires a locally owned process. BrowserDirectory
// is an explicit path on the browser host; it never authorizes Go filesystem IO.
// Native GUID filenames prevent suggested names from selecting local paths.
// Completed and partial files remain caller property; Close never deletes them.
//
// MaxEvents/MaxResultBytes bound the event ledger. MaxStreamBytes bounds observed
// cumulative download bytes and local reads, not browser network or disk writes.
// Delayed native events can report an already exceeded threshold. Overflow stops
// observation, and missing download identity requires actual context destruction
// or process exit before cleanup can be declared complete.
type DownloadOptions struct {
	private
	LocalDirectory   string
	BrowserDirectory category.HostPath
}

// Download owns only the transfer identified by its original page begin event.
// Copies share offsets. Progress is independent of application byte consumption.
// Close its Control (or Page/Context) to cancel unfinished transfers and release
// the local directory handle without needing new Inbox admission.
type Download struct {
	private
	native  native.Download
	control *Control
}

// Download accepts a begin event, never a GUID copied from native event data.
func (pending Pending) Download() (Download, error) {
	if pending.control == nil {
		return Download{}, fail(ErrInput, "download-origin")
	}
	value, err := pending.native.Download()
	return Download{native: value, control: pending.control}, translate(err, "download-origin")
}

// Progress returns a detached native observation without FilePath. A completed
// event is not evidence that a file exists or the application read its bytes.
func (download Download) Progress() (browser.EventDownloadProgress, bool) {
	return download.native.Progress()
}

// CancelDownload is a pre-admitted reply to a begin event. Its acknowledgement is
// independent from the native completed/canceled state recorded in progress events.
func CancelDownload() Reply { return Reply{native: native.CancelDownload()} }

// Read returns at most 65536 bytes from the original completed GUID-named file.
// The caller directory is borrowed, the file descriptor closes before return,
// and symlinks/nonregular files are rejected. A remote/browser-host destination
// cannot be read even when its path also happens to exist on the Go host.
func (download Download) Read(ctx context.Context, size int) (chunk Chunk, receipt *adapters.Receipt[Result], err error) {
	if ctx == nil || download.control == nil || download.control.state == nil {
		return chunk, nil, fail(ErrInput, "download-read")
	}
	if download.control.Closed() {
		return chunk, nil, fail(ErrState, "download-read")
	}
	state := download.control.state
	family := state.context
	var primary error
	receipt, err = family.endpoint.Child(ctx, state.call.Scope(), request("download_read", family.id, 0, family.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, e := state.page.beginOperation()
		if e != nil {
			primary = e
			_ = call.Resolve(adapters.Outcome[Result]{Primary: e})
			return
		}
		defer finish()
		value, e := download.native.Read(call.Context(), size)
		chunk, primary = Chunk{native: value}, translate(e, "download-read")
		info, _ := call.Receipt().Snapshot()
		_ = call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{payload: value.DataCopy(), attribution: attribution(info.Info()), observation: category.Observation{Returned: e == nil, EOF: value.EOF(), Effect: category.Unknown}}, Primary: primary})
	})
	return chunk, receipt, errors.Join(err, receiptError(receipt, primary))
}
