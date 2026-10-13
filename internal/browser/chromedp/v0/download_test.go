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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chromedp/cdproto/browser"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
)

const testDownloadGUID = "12345678-1234-1234-1234-123456789abc"

func TestDownloadIdentityBoundsAndProgressBeforeDelivery(t *testing.T) {
	for _, bad := range []string{"", "../x", "12345678-1234-1234-1234-123456789ab/", "12345678-1234-1234-1234-123456789abz"} {
		if downloadGUID(bad) {
			t.Fatal("malformed file identity accepted")
		}
	}
	session := unitSession(t)
	session.owner.settings.MaxEvents = 3
	life, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	state := &controlState{session: session, life: life, cancel: cancel, downloads: &downloadState{records: make(map[string]*downloadRecord)}}
	begin := jsonv2.Value(`{"guid":"` + testDownloadGUID + `","frameId":"frame","url":"private-download"}`)
	if err := state.observeDownload(browser.DownloadWillBegin.Method, begin); err != nil {
		t.Fatal(err)
	}
	record := state.downloads.records[testDownloadGUID]
	if record == nil || state.bytes != 512 {
		t.Fatal("identity was not admitted before asynchronous delivery")
	}
	if err := state.observeDownload(browser.DownloadWillBegin.Method, begin); !errors.Is(err, ErrNative) {
		t.Fatal("duplicate identity accepted", err)
	}
	for _, bad := range []string{
		`{"guid":"` + testDownloadGUID + `","state":"completed"}`,
		`{"guid":"` + testDownloadGUID + `","totalBytes":null,"receivedBytes":0,"state":"completed"}`,
		`{"guid":"foreign","totalBytes":0,"receivedBytes":0,"state":"completed"}`,
		`{"guid":"` + testDownloadGUID + `","totalBytes":-1,"receivedBytes":0,"state":"completed"}`,
		`{"guid":"` + testDownloadGUID + `","totalBytes":4,"receivedBytes":1.5,"state":"completed"}`,
		`{"guid":"` + testDownloadGUID + `","totalBytes":4,"receivedBytes":2,"state":"invalid"}`,
	} {
		if err := state.observeDownload(browser.DownloadProgress.Method, jsonv2.Value(bad)); !errors.Is(err, ErrNative) {
			t.Fatal("malformed/foreign progress accepted", err)
		}
	}
	progress := jsonv2.Value(`{"guid":"` + testDownloadGUID + `","totalBytes":4,"receivedBytes":4,"state":"completed","filePath":"/private/browser/path"}`)
	if err := state.observeDownload(browser.DownloadProgress.Method, progress); err != nil {
		t.Fatal(err)
	}
	if !record.observed || record.progress.FilePath != "" {
		t.Fatal("wire observation lost or retained an extra path")
	}
	if _, err := (Pending{state: &pendingState{method: browser.DownloadProgress.Method, download: record}}).Download(); !errors.Is(err, ErrInput) {
		t.Fatal("progress minted authority", err)
	}
	state.closed = true
	data, err := (&Control{state: state}).EvidenceCopy()
	if err != nil || !json.Valid(data) || !strings.Contains(string(data), testDownloadGUID) || !strings.Contains(string(data), `"state":"completed"`) {
		t.Fatal("never-delivered begin lost cleanup evidence", err)
	}
	state.closed = false
	state.bytes = session.owner.settings.MaxResultBytes
	if err := state.observeDownload(browser.DownloadWillBegin.Method, jsonv2.Value("malformed")); !errors.Is(err, ErrLimit) {
		t.Fatal("decoder ran before identity-ledger capacity check", err)
	}
}
func TestDownloadByteOverflowDoesNotWrapOrDeclareConsumption(t *testing.T) {
	session := unitSession(t)
	session.owner.settings.MaxEvents = 4
	session.owner.settings.MaxStreamBytes = 8
	life, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	state := &controlState{session: session, life: life, cancel: cancel, downloads: &downloadState{records: make(map[string]*downloadRecord)}}
	if err := state.observeDownload(browser.DownloadWillBegin.Method, jsonv2.Value(`{"guid":"`+testDownloadGUID+`","frameId":"frame"}`)); err != nil {
		t.Fatal(err)
	}
	if err := state.observeDownload(browser.DownloadProgress.Method, jsonv2.Value(`{"guid":"`+testDownloadGUID+`","receivedBytes":1e100,"totalBytes":1e100,"state":"inProgress"}`)); err != nil {
		t.Fatal(err)
	}
	if state.downloads.bytes != 9 || !errors.Is(context.Cause(life), ErrLimit) || state.downloads.records[testDownloadGUID].evidence.ReadBytes != 0 {
		t.Fatal("overflow wrapped, escaped cancellation, or became consumed bytes")
	}
}
func TestDownloadDestinationAndCleanupProof(t *testing.T) {
	session := unitSession(t)
	session.browserContext = &browserContext{browserID: "context"}
	for _, options := range []DownloadOptions{{}, {LocalDirectory: "relative"}, {LocalDirectory: "/local", BrowserDirectory: "/remote"}, {BrowserDirectory: "bad\x00path"}} {
		if err := session.checkDownloads(options); err == nil {
			t.Fatal("invalid destination accepted")
		}
	}
	session.borrowed = true
	if err := session.checkDownloads(DownloadOptions{BrowserDirectory: "/browser-host"}); !errors.Is(err, ErrState) {
		t.Fatal("borrow lacked grant", err)
	}
	session.sharedDownloads = true
	if err := session.checkDownloads(DownloadOptions{BrowserDirectory: `C:\browser-host\downloads`}); err != nil {
		t.Fatal("browser-host path rewritten with local OS rules", err)
	}
	session.owner.settings.RemoteURL = "ws://external"
	if err := session.checkDownloads(DownloadOptions{LocalDirectory: t.TempDir()}); !errors.Is(err, ErrState) {
		t.Fatal("remote controller acquired Go-local file access", err)
	}
	state := &controlState{session: session}
	if state.downloadContextGone() {
		t.Fatal("borrowed page closure implied context destruction")
	}
	session.targetGone = true
	if state.downloadContextGone() {
		t.Fatal("target destruction implied download producer destruction")
	}
	session.owner.processDone = make(chan struct{})
	close(session.owner.processDone)
	if !state.downloadContextGone() {
		t.Fatal("owned process exit did not end download producer")
	}
}
func TestDownloadReadsRejectSymlinksAndPreserveErrors(t *testing.T) {
	session := unitSession(t)
	life, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	directory := t.TempDir()
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	state := &controlState{session: session, life: life, cancel: cancel, downloads: &downloadState{root: root}}
	record := &downloadRecord{guid: testDownloadGUID, observed: true, progress: browser.EventDownloadProgress{State: browser.DownloadProgressStateCompleted}}
	download := Download{control: state, record: record}
	outside := filepath.Join(t.TempDir(), "private-download-file")
	if err := os.WriteFile(outside, []byte("must not be read"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(directory, testDownloadGUID)); err != nil {
		t.Fatal(err)
	}
	if chunk, err := download.Read(context.Background(), 64); err == nil || len(chunk.DataCopy()) != 0 {
		t.Fatal("download followed an ungranted path", err)
	}
	if err := os.Remove(filepath.Join(directory, testDownloadGUID)); err != nil {
		t.Fatal(err)
	}
	if _, err := download.Read(context.Background(), 64); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing file lost native errors.Is", err)
	} else {
		if strings.Contains(fmt.Sprintf("%+v", err), directory) {
			t.Fatal("path leaked through ordinary error")
		}
	}
	if err := os.WriteFile(filepath.Join(directory, testDownloadGUID), []byte("1234"), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := download.Read(context.Background(), 2)
	if err != nil || first.EOF() || string(first.DataCopy()) != "12" {
		t.Fatal("first bounded read", err)
	}
	alias := download
	second, err := alias.Read(context.Background(), 2)
	if err != nil || !second.EOF() || string(second.DataCopy()) != "34" {
		t.Fatal("alias reset offset or lost EOF", err)
	}
	if record.evidence.ReadBytes != 4 || !record.evidence.EOF {
		t.Fatal("actual consumption not recorded")
	}
	for _, size := range []int{0, 65537} {
		if _, err := download.Read(context.Background(), size); !errors.Is(err, ErrInput) {
			t.Fatal("invalid size accepted", err)
		}
	}
}

func TestDownloadSourceCustodyKeepsCapacityAndActualReadJoin(t *testing.T) {
	session := unitSession(t)
	session.owner.settings.MaxSessions = 1
	life, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	setup, pump, idle := make(chan struct{}), make(chan struct{}), make(chan struct{})
	close(setup)
	close(pump)
	state := &controlState{session: session, life: life, cancel: cancel, setupDone: setup, pumpDone: pump, idle: idle, active: 1, gate: make(chan struct{}, 1), stopCaller: func() bool { return true }, unsubscribe: func() {}, downloads: &downloadState{enabled: true, uncertain: true}}
	session.owner.cleanupControls = map[*controlState]struct{}{state: {}}
	if err := session.owner.reserve(&Session{sessionState: &sessionState{browserContext: &browserContext{}}}, 1); !errors.Is(err, ErrLimit) {
		t.Fatal("pending source cleanup refunded the context slot", err)
	}
	session.owner.processDone = make(chan struct{})
	close(session.owner.processDone)
	ctx, stop := context.WithCancel(context.Background())
	stop()
	if err := state.disposed(ctx); err == nil || (&Control{state: state}).Closed() {
		t.Fatal("process exit bypassed actual local read join", err)
	}
	state.mu.Lock()
	state.active = 0
	close(idle)
	state.mu.Unlock()
	if err := state.disposed(context.Background()); err != nil || !(&Control{state: state}).Closed() {
		t.Fatal("joined producer retained custody", err)
	}
	if len(session.owner.cleanupControls) != 0 {
		t.Fatal("download source record was not released")
	}
}
