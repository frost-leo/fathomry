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
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	sdk "github.com/chromedp/chromedp"
)

// DownloadOptions selects exactly one caller-owned destination. LocalDirectory
// permits bounded local reads only for an owned local browser. BrowserDirectory
// is literal browser-host data and never grants Go filesystem access. Files and
// partial files remain caller property after cleanup. Names use native GUIDs,
// never untrusted suggested filenames.
type DownloadOptions struct {
	private
	LocalDirectory   string
	BrowserDirectory string
}

type downloadState struct {
	identity                    cdp.BrowserContextID
	directory                   string
	root                        *os.Root
	enabled, uncertain, stopped bool
	records                     map[string]*downloadRecord
	bytes                       int64
}

type downloadRecord struct {
	guid     string
	progress browser.EventDownloadProgress
	observed bool
	evidence downloadEvidence
}
type downloadEvidence struct {
	GUID               string                        `json:"guid"`
	State              browser.DownloadProgressState `json:"state"`
	Observed           bool                          `json:"observed"`
	ReceivedBytes      float64                       `json:"received_bytes"`
	CancelAttempted    bool                          `json:"cancel_attempted"`
	CancelAcknowledged bool                          `json:"cancel_acknowledged"`
	ReadBytes          int64                         `json:"read_bytes"`
	EOF                bool                          `json:"eof"`
}

// Download is a capability from a target-scoped native begin event. Its aliases
// share read offset, terminal observation and cancellation. It never adopts a GUID
// or path supplied by the caller.
type Download struct {
	private
	control *controlState
	record  *downloadRecord
}

func (session *Session) checkDownloads(options DownloadOptions) error {
	if session.borrowed && !session.sharedDownloads {
		return failure(ErrState, "shared-downloads-grant-required")
	}
	if session.browserID == "" && !(session.borrowed && session.defaultContext && session.defaultIdentityObserved) {
		return failure(ErrState, "download-context-identity")
	}
	if (options.LocalDirectory == "") == (options.BrowserDirectory == "") {
		return failure(ErrInput, "download-destination")
	}
	if int64(len(options.LocalDirectory)+len(options.BrowserDirectory)) > session.owner.settings.MaxCommandBytes/2 ||
		strings.ContainsAny(options.LocalDirectory+options.BrowserDirectory, "\x00\r\n") {
		return failure(ErrInput, "download-destination")
	}
	if options.LocalDirectory != "" && (session.owner.settings.RemoteURL != "" || !filepath.IsAbs(options.LocalDirectory)) {
		return failure(ErrState, "local-download-host")
	}
	return nil
}

func (state *controlState) prepareDownloads(options DownloadOptions) error {
	own := state.session.owner
	own.mu.Lock()
	for existing := range own.cleanupControls {
		if existing.downloads != nil && existing.session.browserID == state.session.browserID {
			own.mu.Unlock()
			return failure(ErrState, "download-policy-owner")
		}
	}
	if own.cleanupControls == nil {
		own.cleanupControls = make(map[*controlState]struct{})
	}
	state.downloads = &downloadState{identity: state.session.browserID, directory: options.BrowserDirectory, records: make(map[string]*downloadRecord)}
	own.cleanupControls[state] = struct{}{}
	own.mu.Unlock()
	if options.LocalDirectory != "" {
		state.downloads.directory = filepath.Clean(options.LocalDirectory)
		root, err := os.OpenRoot(state.downloads.directory)
		if err != nil {
			return failure(ErrInput, "download-directory", err)
		}
		state.downloads.root = root
	}
	return nil
}
func (state *controlState) enableDownloads(ctx context.Context) error {
	state.downloads.enabled = true
	_, err := cdp.Call(state.work(ctx), sdk.FromContext(state.session.native).Target, browser.SetDownloadBehavior,
		browser.SetDownloadBehaviorParams{Behavior: browser.SetDownloadBehaviorBehaviorAllowAndName, BrowserContextID: state.downloads.identity, DownloadPath: state.downloads.directory, EventsEnabled: new(true)})
	return err
}

func downloadGUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, char := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if char != '-' {
				return false
			}
		} else if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f' || char >= 'A' && char <= 'F') {
			return false
		}
	}
	return true
}

// The wire-order observer records identity before delivery can be canceled.
// It retains no URLs or filenames. Both the native queue and this finite ledger
// have independent admission bounds.
func (state *controlState) observeDownload(method string, data jsonv2.Value) error {
	downloads := state.downloads
	if downloads == nil {
		return nil
	}
	switch method {
	case browser.DownloadWillBegin.Method:
		if len(downloads.records) >= state.session.owner.settings.MaxEvents || state.session.owner.settings.MaxResultBytes-state.bytes < 512 {
			return failure(ErrLimit, "download-count")
		}
		state.bytes += 512
		var event browser.EventDownloadWillBegin
		if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if !downloadGUID(event.GUID) || event.FrameID == "" || downloads.records[event.GUID] != nil {
			return failure(ErrNative, "download-identity")
		}
		downloads.records[event.GUID] = &downloadRecord{guid: event.GUID, evidence: downloadEvidence{GUID: event.GUID}}
	case browser.DownloadProgress.Method:
		var message struct {
			browser.EventDownloadProgress
			Total    *float64 `json:"totalBytes"`
			Received *float64 `json:"receivedBytes"`
		}
		if err := jsonv2.Unmarshal(data, &message, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if message.Total == nil || message.Received == nil {
			return failure(ErrNative, "download-progress-bytes")
		}
		event := message.EventDownloadProgress
		event.TotalBytes, event.ReceivedBytes = *message.Total, *message.Received
		record := downloads.records[event.GUID]
		if record == nil || math.IsNaN(event.TotalBytes) || math.IsInf(event.TotalBytes, 0) || event.TotalBytes < 0 ||
			math.IsNaN(event.ReceivedBytes) || math.IsInf(event.ReceivedBytes, 0) || event.ReceivedBytes < record.progress.ReceivedBytes ||
			math.Trunc(event.TotalBytes) != event.TotalBytes || math.Trunc(event.ReceivedBytes) != event.ReceivedBytes ||
			event.State != browser.DownloadProgressStateInProgress && event.State != browser.DownloadProgressStateCanceled && event.State != browser.DownloadProgressStateCompleted {
			return failure(ErrNative, "download-progress")
		}
		if record.observed && record.progress.State != browser.DownloadProgressStateInProgress && record.progress.State != event.State {
			return failure(ErrNative, "download-terminal-state")
		}
		limit := state.session.owner.settings.MaxStreamBytes
		if event.ReceivedBytes > float64(limit) {
			downloads.bytes = limit + 1
		} else if downloads.bytes <= limit {
			downloads.bytes += int64(event.ReceivedBytes - record.progress.ReceivedBytes)
		}
		// Paths remain in the bounded event evidence, not a second retained string.
		event.FilePath = ""
		record.progress, record.observed = event, true
		record.evidence.State, record.evidence.Observed, record.evidence.ReceivedBytes = event.State, true, event.ReceivedBytes
		if event.ReceivedBytes > float64(state.session.owner.settings.MaxStreamBytes) ||
			event.TotalBytes > float64(state.session.owner.settings.MaxStreamBytes) ||
			downloads.bytes > state.session.owner.settings.MaxStreamBytes {
			err := failure(ErrLimit, "download-bytes")
			if state.primary == nil {
				state.primary = err
			}
			state.cancel(err)
		}
	}
	return nil
}
func (state *controlState) downloadLoss() {
	state.mu.Lock()
	if state.downloads != nil {
		state.downloads.uncertain = true
	}
	state.mu.Unlock()
}

// Download requires the begin record. Progress events remain ordinary native
// evidence, not newly minted authority.
func (pending Pending) Download() (Download, error) {
	if pending.state == nil || pending.state.method != browser.DownloadWillBegin.Method || pending.state.download == nil {
		return Download{}, failure(ErrInput, "download-origin")
	}
	return Download{control: pending.state.control, record: pending.state.download}, nil
}

// Progress omits FilePath. The exact native event, including its optional path,
// is intentionally available through Pending.DataCopy. No file existence follows
// from a Completed event.
func (download Download) Progress() (browser.EventDownloadProgress, bool) {
	if download.control == nil || download.record == nil {
		return browser.EventDownloadProgress{}, false
	}
	download.control.mu.Lock()
	defer download.control.mu.Unlock()
	return download.record.progress, download.record.observed
}

// CancelDownload uses only a begin capability and the recorded BrowserContext.
// A positive command acknowledgement does not imply receipt of a terminal event.
func CancelDownload() Reply {
	return Reply{method: browser.CancelDownload.Method, params: func(pending *pendingState) (any, error) {
		if pending.method != browser.DownloadWillBegin.Method || pending.download == nil {
			return nil, failure(ErrInput, "download-origin")
		}
		return browser.CancelDownloadParams{GUID: pending.download.guid, BrowserContextID: pending.control.downloads.identity}, nil
	}}
}

func (download Download) Read(ctx context.Context, size int) (chunk Chunk, err error) {
	if ctx == nil || download.control == nil || download.record == nil || size < 1 || size > 65536 {
		return chunk, failure(ErrInput, "download-read")
	}
	state, record := download.control, download.record
	state.mu.Lock()
	if state.closing || state.closed || state.active != 0 || state.life.Err() != nil || ctx.Err() != nil {
		state.mu.Unlock()
		return chunk, failure(ErrState, "download-read", ctx.Err(), context.Cause(ctx))
	}
	if state.downloads.root == nil {
		state.mu.Unlock()
		return chunk, failure(ErrState, "local-download-grant-required")
	}
	if !record.observed || record.progress.State != browser.DownloadProgressStateCompleted {
		state.mu.Unlock()
		return chunk, failure(ErrState, "download-not-completed")
	}
	if record.evidence.EOF {
		state.mu.Unlock()
		return Chunk{data: []byte{}, eof: true}, nil
	}
	state.active++
	state.idle = make(chan struct{})
	offset := record.evidence.ReadBytes
	root := state.downloads.root
	state.mu.Unlock()
	defer func() {
		state.mu.Lock()
		state.active--
		close(state.idle)
		state.mu.Unlock()
		if err != nil {
			err = failure(ErrNative, "download-read", err)
			state.fail(err)
		}
	}()
	if offset >= state.session.owner.settings.MaxStreamBytes {
		return chunk, failure(ErrLimit, "download-read-bytes")
	}
	before, err := root.Lstat(record.guid)
	if err != nil {
		return chunk, err
	}
	if !before.Mode().IsRegular() {
		return chunk, failure(ErrInput, "download-file-kind")
	}
	file, err := root.Open(record.guid)
	if err != nil {
		return chunk, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	actual, err := file.Stat()
	if err != nil {
		return chunk, err
	}
	if !actual.Mode().IsRegular() || !os.SameFile(before, actual) || actual.Size() > state.session.owner.settings.MaxStreamBytes {
		return chunk, failure(ErrInput, "download-file-identity")
	}
	buffer := make([]byte, min(size, int(state.session.owner.settings.MaxStreamBytes-offset)))
	count, readErr := file.ReadAt(buffer, offset)
	eof := errors.Is(readErr, io.EOF) || readErr == nil && offset+int64(count) == actual.Size()
	if errors.Is(readErr, io.EOF) {
		readErr = nil
	}
	err = errors.Join(readErr, ctx.Err(), context.Cause(ctx), state.life.Err(), context.Cause(state.life))
	state.mu.Lock()
	record.evidence.ReadBytes += int64(count)
	record.evidence.EOF = eof && err == nil
	state.mu.Unlock()
	return Chunk{data: buffer[:count], eof: eof && err == nil}, err
}

func (state *controlState) downloadContextGone() bool {
	session := state.session
	if session.owner.processDone != nil {
		select {
		case <-session.owner.processDone:
			return true
		default:
		}
	}
	if session.borrowed || !session.accounting().reserved {
		return false
	}
	session.owner.mu.Lock()
	defer session.owner.mu.Unlock()
	_, pending := session.owner.sessions[session.browserContext]
	return !pending
}

// Called only after setup, the pump and active local work have joined.
// Closing/detaching a page alone does not stop a browser-owned download.
func (state *controlState) closeDownloads(ctx context.Context) error {
	downloads := state.downloads
	if downloads == nil {
		return nil
	}
	if downloads.enabled {
		gone := state.downloadContextGone()
		if !gone {
			executor := sdk.FromContext(state.session.owner.root).Browser
			if !downloads.stopped {
				_, err := cdp.Call(state.work(ctx), sdk.FromContext(state.session.native).Target, browser.SetDownloadBehavior,
					browser.SetDownloadBehaviorParams{Behavior: browser.SetDownloadBehaviorBehaviorDefault, BrowserContextID: downloads.identity, EventsEnabled: new(false)})
				if err != nil {
					return failure(ErrCleanup, "download-policy-reset", err)
				}
				downloads.stopped = true
			}
			// Keep the wire observer until native event disable is acknowledged.
			// Its join freezes the cancellation ledger even if Next was never used.
			state.unsubscribe()
			var cleanup error
			for _, record := range downloads.records {
				if record.progress.State == browser.DownloadProgressStateCompleted || record.progress.State == browser.DownloadProgressStateCanceled || record.evidence.CancelAcknowledged {
					continue
				}
				state.mu.Lock()
				record.evidence.CancelAttempted = true
				state.mu.Unlock()
				_, err := cdp.Call(state.work(ctx), executor, browser.CancelDownload, browser.CancelDownloadParams{GUID: record.guid, BrowserContextID: downloads.identity})
				state.mu.Lock()
				record.evidence.CancelAcknowledged = err == nil
				state.mu.Unlock()
				cleanup = errors.Join(cleanup, err)
			}
			if downloads.uncertain {
				cleanup = errors.Join(cleanup, failure(ErrCleanup, "download-origin-lost"))
			}
			if cleanup != nil {
				return failure(ErrCleanup, "download-close", cleanup)
			}
		}
		downloads.enabled = false
	}
	state.unsubscribe()
	if downloads.root != nil {
		err := downloads.root.Close()
		downloads.root = nil
		if err != nil {
			return failure(ErrCleanup, "download-directory", err)
		}
	}
	return nil
}
