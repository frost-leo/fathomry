/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package chromedp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/remote"
	"github.com/gorilla/websocket"
)

// The proxy changes one reply from a task-owned browser, not a shared endpoint.
func streamCloseFaultPeer(t *testing.T) string {
	endpoint, _ := cleanupFaultPeer(t, map[string]int64{"IO.close": 1})
	return endpoint
}

func cleanupFaultPeer(t *testing.T, failures map[string]int64) (string, context.Context) {
	return cleanupFaultPeerMatching(t, failures, nil)
}

func cleanupFaultPeerMatching(t *testing.T, failures map[string]int64, match func(string, json.RawMessage) bool) (string, context.Context) {
	return cleanupFaultPeerReplies(t, failures, match, nil)
}

func cleanupFaultPeerReplies(t *testing.T, failures map[string]int64, match func(string, json.RawMessage) bool, rewrite func([]byte) []byte) (string, context.Context) {
	return cleanupFaultPeerTransform(t, failures, match, rewrite, nil)
}

func cleanupFaultPeerTransform(t *testing.T, failures map[string]int64, match func(string, json.RawMessage) bool, rewrite, rewriteRequest func([]byte) []byte) (string, context.Context) {
	t.Helper()
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	profile := t.TempDir()
	allocator, closeAllocator := sdk.NewExecAllocator(context.Background(), sdk.ExecPath(path), sdk.Headless, sdk.NoFirstRun,
		sdk.Flag("disable-background-networking", true), sdk.UserDataDir(profile), remote.WebSocket)
	t.Cleanup(closeAllocator)
	root, closeRoot := sdk.NewContext(allocator)
	t.Cleanup(func() {
		if err := sdk.Cancel(root); err != nil {
			t.Error(err)
		}
		closeRoot()
	})
	if err := sdk.Do(root); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(profile, "DevToolsActivePort"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatal("fixture endpoint missing")
	}
	endpoint := "ws://127.0.0.1:" + lines[0] + lines[1]
	counts := make(map[string]*atomic.Int64)
	for method := range failures {
		counts[method] = &atomic.Int64{}
	}
	var handlers sync.WaitGroup
	var connectionsMu sync.Mutex
	connections := make(map[*websocket.Conn]struct{})
	stopping := false
	track := func(connection *websocket.Conn) bool {
		connectionsMu.Lock()
		defer connectionsMu.Unlock()
		if stopping {
			_ = connection.Close()
			return false
		}
		connections[connection] = struct{}{}
		return true
	}
	untrack := func(connection *websocket.Conn) {
		connectionsMu.Lock()
		delete(connections, connection)
		connectionsMu.Unlock()
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectionsMu.Lock()
		if stopping {
			connectionsMu.Unlock()
			http.Error(w, "fixture closed", http.StatusServiceUnavailable)
			return
		}
		handlers.Add(1)
		connectionsMu.Unlock()
		defer handlers.Done()
		upstream, _, err := websocket.DefaultDialer.Dial(endpoint, nil)
		if err != nil {
			t.Error("fixture dial", err)
			return
		}
		defer upstream.Close()
		if !track(upstream) {
			return
		}
		defer untrack(upstream)
		upgrader := websocket.Upgrader{}
		downstream, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error("fixture upgrade", err)
			return
		}
		defer downstream.Close()
		if !track(downstream) {
			return
		}
		defer untrack(downstream)
		var writes sync.Mutex
		readerDone := make(chan struct{})
		go func() {
			defer close(readerDone)
			defer downstream.Close()
			for {
				kind, data, err := upstream.ReadMessage()
				if err != nil {
					return
				}
				if rewrite != nil {
					data = rewrite(data)
					if data == nil {
						continue
					}
				}
				writes.Lock()
				err = downstream.WriteMessage(kind, data)
				writes.Unlock()
				if err != nil {
					return
				}
			}
		}()
		defer func() { upstream.Close(); downstream.Close(); <-readerDone }()
		for {
			kind, data, err := downstream.ReadMessage()
			if err != nil {
				return
			}
			var request struct {
				ID        int64           `json:"id"`
				Method    string          `json:"method"`
				SessionID string          `json:"sessionId"`
				Params    json.RawMessage `json:"params"`
			}
			if err := json.Unmarshal(data, &request); err != nil {
				t.Error(err)
				return
			}
			count, limit := counts[request.Method], failures[request.Method]
			if count != nil && (match == nil || match(request.Method, request.Params)) && (limit < 0 || count.Add(1) <= limit) {
				response := map[string]any{"id": request.ID, "sessionId": request.SessionID,
					"error": map[string]any{"code": -32001, "message": "private-cleanup-canary"}}
				writes.Lock()
				err = downstream.WriteJSON(response)
				writes.Unlock()
			} else {
				if rewriteRequest != nil {
					data = rewriteRequest(data)
					if data == nil {
						continue
					}
				}
				err = upstream.WriteMessage(kind, data)
			}
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(func() {
		connectionsMu.Lock()
		stopping = true
		for connection := range connections {
			_ = connection.Close()
		}
		connectionsMu.Unlock()
		server.Close()
		handlers.Wait()
	})
	return "ws" + strings.TrimPrefix(server.URL, "http") + "/devtools/browser/fixture", root
}

func TestLiveStreamCleanupFailureSurvivesRetry(t *testing.T) {
	endpoint := streamCloseFaultPeer(t)
	prepared, err := Prepare(Settings{Name: "cleanup-history", RemoteURL: endpoint}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, retainedReceipt, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stream, receipt, err := retained.Page().PDFStream(bounded(t), page.PrintToPDFParams{})
	if err != nil {
		t.Fatal(err)
	}
	first := stream.Close(bounded(t))
	var cause *cdproto.Error
	if !errors.As(first, &cause) || cause.Code != -32001 || stream.Closed() {
		t.Fatal("fixture did not reject only the first close", first)
	}
	if strings.Contains(first.Error(), cause.Message) {
		t.Fatal("cleanup cause leaked")
	}
	if err := stream.Close(bounded(t)); !errors.Is(err, cause) || !stream.Closed() {
		t.Fatal("retry lost cleanup history or failed to release", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Primary() != nil || !errors.Is(snapshot.Cleanup(), cause) {
		t.Fatal("independent stream cleanup history lost")
	}
	if err := retained.Close(bounded(t)); !errors.Is(err, cause) {
		t.Fatal("context lost child cleanup history", err)
	}
	snapshot, err = retainedReceipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Cleanup(), cause) {
		t.Fatal("context receipt lost child cleanup history", err)
	}
	if err := owner.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	status, err := dependencies.Evidence.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, status.Outstanding)
}

func TestLivePDFStreamConcurrentContextClose(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	prepared, err := Prepare(Settings{Name: "stream-race", ExecPath: path, TempDir: t.TempDir(), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	sourceDelivery, err := dependencies.Evidence.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
		if err != nil {
			t.Fatal(err)
		}
		streams := make(chan *Stream, 1)
		go func() {
			stream, _, _ := retained.Page().PDFStream(context.Background(), page.PrintToPDFParams{})
			streams <- stream
		}()
		// Mix cancellation before creation with cancellation during creation.
		time.Sleep(time.Millisecond)
		_ = retained.Close(bounded(t))
		stream := <-streams
		if stream != nil && !stream.Closed() {
			_ = stream.Close(bounded(t))
			t.Fatal("context close missed a concurrently created stream")
		}
		retained.mu.Lock()
		complete := retained.complete
		retained.mu.Unlock()
		if !complete {
			t.Fatal("context remained incomplete")
		}
		status, err := dependencies.Evidence.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		drain(t, dependencies, status.Outstanding-1)
	}
	if err := owner.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if err := sourceDelivery.Ack(); err != nil {
		t.Fatal(err)
	}
}
