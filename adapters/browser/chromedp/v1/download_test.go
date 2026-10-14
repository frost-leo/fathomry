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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/browser"
	sdk "github.com/chromedp/chromedp"
	category "github.com/frost-leo/fathomry/adapters/browser/v1"
)

func downloadServer(t *testing.T, slow bool) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/file" {
			w.Header().Set("Content-Disposition", `attachment; filename="../../not-a-path.txt"`)
			w.Header().Set("Content-Type", "application/octet-stream")
			if slow {
				w.Header().Set("Content-Length", "100000")
				_, _ = w.Write(bytes.Repeat([]byte("partial "), 512))
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				return
			}
			_, _ = w.Write([]byte("synthetic downloaded bytes"))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<a id='download' href='/file'>download</a>"))
	}))
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	return server
}
func downloadBegin(t *testing.T, control *Control) (Pending, Download) {
	t.Helper()
	for range 100 {
		pending, err := control.Next(bounded(t))
		if err != nil {
			t.Fatal(err)
		}
		if pending.Method() != browser.DownloadWillBegin.Method {
			continue
		}
		download, err := pending.Download()
		if err != nil {
			t.Fatal(err)
		}
		return pending, download
	}
	t.Fatal("no target-scoped begin event")
	return Pending{}, Download{}
}
func downloadComplete(t *testing.T, control *Control) {
	t.Helper()
	for range 100 {
		pending, err := control.Next(bounded(t))
		if err != nil {
			t.Fatal(err)
		}
		if pending.Method() != browser.DownloadProgress.Method {
			continue
		}
		if _, err := pending.Download(); !errors.Is(err, ErrInput) {
			t.Fatal("progress minted a new download grant", err)
		}
		var progress browser.EventDownloadProgress
		if err := json.Unmarshal(pending.DataCopy(), &progress); err != nil {
			t.Fatal(err)
		}
		if progress.State == browser.DownloadProgressStateCompleted {
			return
		}
		if progress.State == browser.DownloadProgressStateCanceled {
			t.Fatal("fixture download canceled")
		}
	}
	t.Fatal("no native completion event")
}

func TestLiveDownloadBytesEvidenceNamesAndCallerFiles(t *testing.T) {
	server := downloadServer(t, false)
	owner, retained, dependencies := controlFixture(t, server.URL, 128)
	directory := t.TempDir()
	sentinel := filepath.Join(directory, "caller-owned")
	if err := os.WriteFile(sentinel, []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	options := DownloadOptions{LocalDirectory: directory}
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Downloads: &options})
	if err != nil {
		t.Fatal(err)
	}
	options.LocalDirectory = t.TempDir()
	sibling, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	rejected, rejectedReceipt, err := sibling.Control(retained.Context(), ControlOptions{Downloads: &DownloadOptions{LocalDirectory: directory}})
	if !errors.Is(err, ErrState) {
		t.Fatal("concurrent page policy overwrote the context owner", err)
	}
	if rejected != nil {
		_ = rejected.Close(bounded(t))
	}
	if _, err := rejectedReceipt.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Click("#download")); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	begin, download := downloadBegin(t, control)
	downloadComplete(t, control)
	progress, observed := download.Progress()
	if !observed || progress.State != browser.DownloadProgressStateCompleted || progress.FilePath != "" {
		t.Fatal("wrong native completion facts")
	}
	var start browser.EventDownloadWillBegin
	if err := json.Unmarshal(begin.DataCopy(), &start); err != nil {
		t.Fatal(err)
	}
	if filepath.Base(start.GUID) != start.GUID {
		t.Fatal("fixture GUID is not a basename")
	}
	var bytesRead []byte
	alias := download
	for range 64 {
		chunk, r, err := alias.Read(bounded(t), 7)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := r.WaitReleased(bounded(t))
		if err != nil {
			t.Fatal(err)
		}
		evidence, present := snapshot.ValueCopy()
		if !present || !bytes.Equal(evidence.PayloadCopy(), chunk.DataCopy()) || evidence.Observation().EOF != chunk.EOF() {
			t.Fatal("read evidence diverged")
		}
		bytesRead = append(bytesRead, chunk.DataCopy()...)
		drain(t, dependencies, 1)
		if chunk.EOF() {
			break
		}
	}
	if string(bytesRead) != "synthetic downloaded bytes" {
		t.Fatal("incremental bytes or shared offset lost")
	}
	if err := control.Close(bounded(t)); err != nil || !control.Closed() {
		t.Fatal("download cleanup did not join", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || snapshot.Cleanup() != nil {
		t.Fatal("download receipt cleanup", err, snapshot.Cleanup())
	}
	evidence, _ := snapshot.ValueCopy()
	if !bytes.Contains(evidence.PayloadCopy(), []byte(`"read_bytes":26`)) || !bytes.Contains(evidence.PayloadCopy(), []byte(`"eof":true`)) {
		t.Fatal("independent download consumption evidence missing", string(evidence.PayloadCopy()))
	}
	drain(t, dependencies, 1)
	if _, receipt, err := download.Read(bounded(t), 1); !errors.Is(err, ErrState) || receipt != nil {
		t.Fatal("closed download was admitted or revived", err)
	}
	if retained.Context().Err() != nil {
		t.Fatal("test relied on context expiry instead of explicit cleanup")
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("source cleanup", err)
	}
	if data, err := os.ReadFile(filepath.Join(directory, start.GUID)); err != nil || string(data) != string(bytesRead) {
		t.Fatal("provider deleted or renamed caller download", err)
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "preserved" {
		t.Fatal("caller directory modified", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 2 {
		t.Fatal("suggested filename was used as a path or partial file remained", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveDownloadCancelAndCleanupNeedNoNewAdmission(t *testing.T) {
	server := downloadServer(t, true)
	owner, retained, dependencies := controlFixture(t, server.URL, 24)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Downloads: &DownloadOptions{LocalDirectory: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Click("#download")); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	pending, download := downloadBegin(t, control)
	if _, _, err := download.Read(bounded(t), 1); !errors.Is(err, ErrState) {
		t.Fatal("partial file presented as completed bytes", err)
	}
	drain(t, dependencies, 1)
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
		t.Fatal("ordinary command lane not saturated")
	}
	fillControlInbox(t, retained, dependencies)
	if err := pending.Reply(bounded(t), CancelDownload()); err != nil {
		t.Fatal("cancel depended on new admission", err)
	}
	if err := pending.Reply(bounded(t), CancelDownload()); !errors.Is(err, ErrState) {
		t.Fatal("download begin became reusable", err)
	}
	for range 100 {
		next, err := control.Next(bounded(t))
		if err != nil {
			t.Fatal(err)
		}
		if next.Method() != browser.DownloadProgress.Method {
			continue
		}
		var progress browser.EventDownloadProgress
		if err := json.Unmarshal(next.DataCopy(), &progress); err != nil {
			t.Fatal(err)
		}
		if progress.State == browser.DownloadProgressStateCanceled {
			break
		}
	}
	progress, ok := download.Progress()
	if !ok || progress.State != browser.DownloadProgressStateCanceled {
		t.Fatal("cancel acknowledged without terminal observation")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("saturated download cleanup incomplete")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := snapshot.ValueCopy()
	if !bytes.Contains(value.PayloadCopy(), []byte(`"cancel_acknowledged":true`)) || bytes.Contains(value.PayloadCopy(), []byte(`"eof":true`)) {
		t.Fatal("cancellation became file consumption")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedDownloadsRequireExplicitPolicyAndDoNotReadLocalFiles(t *testing.T) {
	server := downloadServer(t, false)
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	directory := t.TempDir()
	if _, _, err := retained.Page().Control(retained.Context(), ControlOptions{Downloads: &DownloadOptions{BrowserDirectory: category.HostPath(directory)}}); !errors.Is(err, ErrState) {
		t.Fatal("borrowed policy lacked an explicit grant", err)
	}
	drain(t, dependencies, 1)
	if err := retained.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true, SharedDownloads: true, TargetID: string(sdk.FromContext(peer).Target.TargetID)})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := retained.Page().Control(retained.Context(), ControlOptions{Downloads: &DownloadOptions{LocalDirectory: directory}}); !errors.Is(err, ErrState) {
		t.Fatal("cohost remote browser acquired Go-local authority", err)
	}
	drain(t, dependencies, 1)
	if err := sdk.Do(peer, sdk.Navigate(server.URL)); err != nil {
		t.Fatal(err)
	}
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Downloads: &DownloadOptions{BrowserDirectory: category.HostPath(directory)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := sdk.Do(peer, sdk.Click("#download")); err != nil {
		t.Fatal(err)
	}
	begin, download := downloadBegin(t, control)
	downloadComplete(t, control)
	if _, _, err := download.Read(bounded(t), 1); !errors.Is(err, ErrState) {
		t.Fatal("remote path was read on the Go host", err)
	}
	drain(t, dependencies, 1)
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("borrowed download cleanup", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || snapshot.Cleanup() != nil {
		t.Fatal("download cleanup not independently recorded", err)
	}
	value, _ := snapshot.ValueCopy()
	if !bytes.Contains(value.PayloadCopy(), []byte(`"read_bytes":0`)) {
		t.Fatal("remote completion claimed file bytes")
	}
	var ev browser.EventDownloadWillBegin
	if err := json.Unmarshal(begin.DataCopy(), &ev); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(directory, ev.GUID)); err != nil || !strings.HasPrefix(string(data), "synthetic") {
		t.Fatal("explicit cohost fixture did not receive its browser file", err)
	}
	if got, err := sdk.Run(peer, sdk.Evaluate[int]("39")); err != nil || got != 39 {
		t.Fatal("external peer was terminated", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestDownloadZeroAndDestinationPrivacy(t *testing.T) {
	if _, err := (Pending{}).Download(); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if _, _, err := (Download{}).Read(context.Background(), 1); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}

func TestLiveDownloadWithoutNextStillCancelsItsNativeTransfer(t *testing.T) {
	server := downloadServer(t, true)
	owner, retained, dependencies := controlFixture(t, server.URL, 128)
	directory := t.TempDir()
	lifetime, cancel := context.WithCancel(retained.Context())
	defer cancel()
	control, receipt, err := retained.Page().Control(lifetime, ControlOptions{Downloads: &DownloadOptions{LocalDirectory: directory}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Click("#download")); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	until := bounded(t)
	for {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			break
		}
		select {
		case <-until.Done():
			t.Fatal("partial browser file never appeared")
		case <-time.After(time.Millisecond):
		}
	}
	// No call to Next, and no returned Download handle is needed for cleanup.
	fillControlInbox(t, retained, dependencies)
	cancel()
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !control.Closed() || snapshot.Cleanup() != nil {
		t.Fatal("never-read downloads retained the native producer", err, snapshot.Cleanup())
	}
	value, _ := snapshot.ValueCopy()
	if !bytes.Contains(value.PayloadCopy(), []byte(`"cancel_acknowledged":true`)) || !bytes.Contains(value.PayloadCopy(), []byte(`"read_bytes":0`)) {
		t.Fatal("unread transfer cleanup evidence was discarded")
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("download source shutdown incomplete", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveDownloadByteLimitCancelsButDoesNotClaimDiskHardLimit(t *testing.T) {
	server := downloadServer(t, true)
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	prepared, err := Prepare(Settings{Name: "download-limit", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(1), MaxStreamBytes: new(int64(128)), Flags: map[string]string{"headless": "", "no-first-run": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), server.URL); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Downloads: &DownloadOptions{LocalDirectory: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Click("#download")); err != nil {
		t.Fatal(err)
	}
	// The download receipt can finish before the Click receipt. Do not assume
	// delivery order when acknowledging independent results.
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !control.Closed() || !errors.Is(snapshot.Primary(), ErrLimit) {
		t.Fatal("observed byte threshold did not stop the producer", err, snapshot.Primary())
	}
	value, _ := snapshot.ValueCopy()
	if !bytes.Contains(value.PayloadCopy(), []byte(`"cancel_acknowledged":true`)) || bytes.Contains(value.PayloadCopy(), []byte(`"eof":true`)) {
		t.Fatal("threshold was turned into successful byte acquisition")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("limited download prevented source cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveDownloadOverflowKeepsCleanupUntilNativeProducerEnds(t *testing.T) {
	server := downloadServer(t, true)
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	prepared, err := Prepare(Settings{Name: "download-events", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(1), MaxEvents: new(1), Flags: map[string]string{"headless": "", "no-first-run": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), server.URL); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Downloads: &DownloadOptions{LocalDirectory: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Click("#download")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-control.state.native.Stopping():
	case <-bounded(t).Done():
		t.Fatal("bounded cumulative/native producer did not report overflow")
	}
	// Overflow can stop the native queue before all begin identities reach Go.
	// Page detach is not proof that a browser-owned transfer ended.
	for range 3 {
		_ = owner.Close(bounded(t))
		if owner.ShutdownComplete() {
			break
		}
	}
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("owned context/process disposal did not settle limited downloads")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Primary(), ErrLimit) {
		t.Fatal("overflow missing from independent evidence", err, snapshot.Primary())
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
