/**
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
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/tracing"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
)

// The process is created by this fixture. No existing browser or process group
// is stopped. Stack inspection synchronizes the blocked native write, not quotas.
func TestBrowserOwnedShutdownSurvivesBlockedCDPWrite(t *testing.T) {
	checkOwnedShutdownSurvivesBlockedCDPWrite(t, false, false, false)
}

func TestBrowserOwnedTraceShutdownSurvivesBlockedCDPWrite(t *testing.T) {
	checkOwnedShutdownSurvivesBlockedCDPWrite(t, true, false, false)
}

func TestBrowserOwnedDownloadShutdownSurvivesBlockedCDPWrite(t *testing.T) {
	checkOwnedShutdownSurvivesBlockedCDPWrite(t, false, true, false)
}

func TestBrowserOwnedResponseStreamShutdownSurvivesBlockedCDPWrite(t *testing.T) {
	checkOwnedShutdownSurvivesBlockedCDPWrite(t, false, false, true)
}

func checkOwnedShutdownSurvivesBlockedCDPWrite(t *testing.T, withTrace, withDownload, withResponse bool) {
	t.Helper()
	options := browserOptions(t)
	options.MaxCommandBytes = 16 << 20
	options.CleanupTimeout = 100 * time.Millisecond
	f := bindFixture(t, options, 1)
	if err := f.client.owner.start(deadline(t)); err != nil {
		t.Fatal(err)
	}
	var trace *Trace
	if withTrace {
		var err error
		trace, err = StartTrace(deadline(t), Source{owner: f.client.owner}, tracing.StartParams{TraceConfig: &tracing.TraceConfig{TraceBufferSizeInKb: 1024, IncludedCategories: []string{"blink.user_timing"}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	process := f.client.owner.process.Process
	t.Cleanup(func() { _ = process.Signal(syscall.SIGCONT); _ = process.Kill(); <-f.client.owner.processDone })
	ctx, cancel := context.WithCancel(deadline(t))
	defer cancel()
	entered, done := make(chan struct{}), make(chan struct{})
	var runErr error
	var download *Control
	var response *Control
	var body *Stream
	var server *httptest.Server
	if withResponse {
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<title>owned response</title>"))
		}))
		t.Cleanup(server.Close)
	}
	directory := t.TempDir()
	go func() {
		_, runErr = f.client.Run(ctx, deadline(t), fault.Correlation{Call: "backpressure"}, func(session *Session) error {
			if withResponse {
				if err := session.Navigate(deadline(t), server.URL); err != nil {
					return err
				}
				var err error
				response, err = session.Control(session.Context(), ControlOptions{Fetch: &fetch.EnableParams{Patterns: []*fetch.RequestPattern{{URLPattern: server.URL + "/body", RequestStage: fetch.RequestStageResponse}}}})
				if err != nil {
					return err
				}
				if _, err := RunAction(deadline(t), session, Evaluate[bool](`fetch('/body').catch(()=>null);true`)); err != nil {
					return err
				}
				pending, err := response.Next(deadline(t))
				if err != nil {
					return err
				}
				body, err = pending.ResponseBodyStream(deadline(t))
				if err != nil {
					return err
				}
			}
			if withDownload {
				var err error
				download, err = session.Control(session.Context(), ControlOptions{Downloads: &DownloadOptions{LocalDirectory: directory}})
				if err != nil {
					return err
				}
			}
			if err := process.Signal(syscall.SIGSTOP); err != nil {
				return err
			}
			close(entered)
			return session.Actions(session.Context(), Evaluate[sdk.Void]("1"+strings.Repeat(" ", 8<<20)))
		})
		close(done)
	}()
	defer func() { cancel(); _ = process.Signal(syscall.SIGCONT); <-done }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("owned browser did not reach controlled stop")
	}
	blocked := false
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		data := make([]byte, 1<<20)
		count := runtime.Stack(data, true)
		stack := string(data[:count])
		if strings.Contains(stack, "github.com/chromedp/chromedp/remote.(*Conn).Write") && strings.Contains(stack, "internal/poll.(*FD).Write") {
			blocked = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("write-backpressure precondition not reproduced")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("caller did not finish canceled native command")
	}
	if !errors.Is(runErr, context.Canceled) {
		t.Fatal("native cancellation lost", runErr)
	}
	f.client.owner.mu.Lock()
	transferred := len(f.client.owner.pendingObjects) == 1 && len(f.client.owner.sessions) == 1
	f.client.owner.mu.Unlock()
	if !transferred {
		t.Fatal("joined value work did not transfer its uncertain native custody to the source")
	}
	first := f.assembly.Close(deadline(t))
	if first != nil {
		f.cleanupCause = context.DeadlineExceeded
	}
	select {
	case <-f.client.owner.processDone:
	case <-time.After(time.Second):
		t.Fatal("owned process kill was starved behind CDP connection shutdown")
	}
	result := f.assembly.Close(deadline(t))
	if first != nil && !errors.Is(result, context.DeadlineExceeded) {
		t.Error("cleanup history disappeared")
	}
	if !f.assembly.Snapshot().Sources[0].Released {
		t.Error("killed/joined process remained unreleasable")
	}
	f.client.owner.mu.Lock()
	objectsRemaining := len(f.client.owner.pendingObjects)
	f.client.owner.mu.Unlock()
	if objectsRemaining != 0 {
		t.Error("positive process exit did not settle transferred object custody")
	}
	if withTrace && !trace.Closed() {
		t.Error("owned process exit did not settle trace producer/IO custody")
	}
	if withDownload && (download == nil || !download.Closed() || download.state.downloads.root != nil) {
		t.Error("owned process exit left download control or directory ownership behind")
	}
	if withResponse && (response == nil || body == nil || !response.Closed() || !body.Closed()) {
		t.Error("owned process exit left response control or stream ownership behind")
	}
	delivery, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
}
