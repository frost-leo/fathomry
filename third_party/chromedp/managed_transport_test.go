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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/dom"
)

type managedBlockedTransport struct {
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (t *managedBlockedTransport) Read(context.Context, *cdproto.Message) error {
	<-t.closed
	return io.EOF
}
func (t *managedBlockedTransport) Write(context.Context, *cdproto.Message) error {
	close(t.entered)
	<-t.closed
	return io.ErrClosedPipe
}
func (t *managedBlockedTransport) Close() error { t.once.Do(func() { close(t.closed) }); return nil }
func TestManagedBrowserCancellationClosesBlockedTransport(t *testing.T) {
	transport := &managedBlockedTransport{entered: make(chan struct{}), closed: make(chan struct{})}
	defer transport.Close()
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	browser, err := NewBrowserTransport(lifetime, transport, WithBrowserLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	callDone := make(chan error, 1)
	go func() { callDone <- browser.Call(context.Background(), "Browser.getVersion", nil, nil) }()
	<-transport.entered
	cancel()
	select {
	case <-transport.closed:
	case <-time.After(time.Second):
		t.Error("canceled browser could not close its blocked transport")
		transport.Close()
	}
	select {
	case err := <-callDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller not joined")
	}
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := browser.WaitStopped(wait); err != nil {
		t.Fatal("native dispatch/read join", err)
	}
}

func TestManagedTargetStopUnblocksCallsWithoutClosingPeer(t *testing.T) {
	browser := &Browser{cmdQueue: make(chan *cdproto.Message), LostConnection: make(chan struct{})}
	target := &Target{browser: browser, stopped: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- target.Call(context.Background(), "IO.close", nil, nil) }()
	<-browser.cmdQueue
	close(target.stopped)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("detached target retained a pending control call")
	}
	select {
	case <-browser.LostConnection:
		t.Fatal("target close shut down the borrowed peer")
	default:
	}
	var called bool
	ctx := WithCallLimits(context.Background(), CallLimits{Before: func(string) error { called = true; return nil }})
	if err := target.Call(ctx, "IO.close", nil, nil); !errors.Is(err, context.Canceled) || called {
		t.Fatal("stopped target admitted more work", err)
	}
}

func TestManagedTargetStopPreservesConnectionCause(t *testing.T) {
	for _, stage := range []string{"before_admission", "before_submission", "waiting_reply"} {
		t.Run(stage, func(t *testing.T) {
			cause := errors.New("fixture connection loss")
			browser := &Browser{cmdQueue: make(chan *cdproto.Message), LostConnection: make(chan struct{}), lostReason: cause}
			target := &Target{browser: browser, stopped: make(chan struct{})}
			admitted := make(chan struct{})
			work := WithCallLimits(context.Background(), CallLimits{Before: func(string) error { close(admitted); return nil }})
			stop := func() { close(browser.LostConnection); close(target.stopped) }
			if stage == "before_admission" {
				stop()
			}
			done := make(chan error, 1)
			go func() { done <- target.Call(work, "Runtime.evaluate", nil, nil) }()
			switch stage {
			case "before_submission":
				<-admitted
				stop()
			case "waiting_reply":
				<-browser.cmdQueue
				stop()
			}
			select {
			case err := <-done:
				if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
					t.Fatal("target stop erased the native connection cause", err)
				}
			case <-time.After(time.Second):
				t.Fatal("stopped target call did not return")
			}
		})
	}
}

type managedReplyTransport struct {
	incoming chan *cdproto.Message
	closed   chan struct{}
	once     sync.Once
	writes   atomic.Int64
}

func (t *managedReplyTransport) Read(ctx context.Context, result *cdproto.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return io.EOF
	case message := <-t.incoming:
		*result = *message
		return nil
	}
}

func (t *managedReplyTransport) Write(ctx context.Context, message *cdproto.Message) error {
	t.writes.Add(1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return io.EOF
	case t.incoming <- &cdproto.Message{ID: message.ID, SessionID: message.SessionID, Result: jsonv2.Value(`{}`)}:
		return nil
	}
}

func (t *managedReplyTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

func TestManagedStoppedTargetCannotBlockBrowserOrPeer(t *testing.T) {
	for _, state := range []string{"active", "stopped"} {
		t.Run(state, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				life, stopBrowser := context.WithCancel(context.Background())
				transport := &managedReplyTransport{incoming: make(chan *cdproto.Message, 2048), closed: make(chan struct{})}
				browser, err := NewBrowserTransport(life, transport, WithBrowserLogf(func(string, ...any) {}), WithEventBuffer(EventBuffer{MaxEvents: 1, MaxBytes: 128}))
				if err != nil {
					stopBrowser()
					t.Fatal(err)
				}
				defer func() {
					stopBrowser()
					if err := browser.WaitStopped(context.Background()); err != nil {
						t.Error(err)
					}
				}()
				page, err := browser.newTarget(life, "page", "page-session")
				if err != nil {
					t.Fatal(err)
				}
				pageLife, stopPage := context.WithCancel(life)
				defer stopPage()
				go page.run(pageLife)
				peer, err := browser.newTarget(life, "peer", "peer-session")
				if err != nil {
					t.Fatal(err)
				}
				go peer.run(life)
				if state == "stopped" {
					stopPage()
					if err := page.WaitStopped(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				for range cap(page.messageQueue) + 4 {
					transport.incoming <- &cdproto.Message{SessionID: page.SessionID, Method: "Network.loadingFinished", Params: jsonv2.Value(`{"requestId":"request","timestamp":1,"encodedDataLength":0}`)}
				}
				synctest.Wait()
				for name, call := range map[string]func(context.Context) error{
					"browser": func(ctx context.Context) error { return browser.Call(ctx, "Browser.getVersion", nil, nil) },
					"peer":    func(ctx context.Context) error { return peer.Call(ctx, "Runtime.evaluate", nil, nil) },
				} {
					ctx, cancel := context.WithTimeout(life, time.Second)
					err := call(ctx)
					cancel()
					if err != nil {
						t.Errorf("%s call after %s target events: %v", name, state, err)
					}
				}
				if writes := transport.writes.Load(); writes != 2 {
					t.Errorf("command writes = %d, want 2", writes)
				}
			})
		})
	}
}

type managedBurstTransport struct {
	*managedReplyTransport
	burst int
	sent  atomic.Bool
}

func (t *managedBurstTransport) Write(ctx context.Context, message *cdproto.Message) error {
	if string(message.Method) != dom.GetDocument.Method || !t.sent.CompareAndSwap(false, true) {
		return t.managedReplyTransport.Write(ctx, message)
	}
	t.writes.Add(1)
	for range t.burst {
		t.incoming <- &cdproto.Message{SessionID: message.SessionID, Method: "Runtime.consoleAPICalled", Params: jsonv2.Value(`{"type":"log","args":[],"executionContextId":1,"timestamp":1}`)}
	}
	t.incoming <- &cdproto.Message{ID: message.ID, SessionID: message.SessionID, Result: jsonv2.Value(`{"root":{"nodeId":1,"nodeType":9,"nodeName":"#document","localName":"","nodeValue":""}}`)}
	return nil
}

func TestManagedStateQueueOverflowStopsOnlyItsTarget(t *testing.T) {
	for _, fixture := range []struct {
		name  string
		burst int
	}{
		{"within-capacity", 16},
		{"overflow", 6000},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				life, stopBrowser := context.WithCancel(context.Background())
				transport := &managedBurstTransport{managedReplyTransport: &managedReplyTransport{incoming: make(chan *cdproto.Message, fixture.burst+4), closed: make(chan struct{})}, burst: fixture.burst}
				var overflows atomic.Int64
				browser, err := NewBrowserTransport(life, transport, WithBrowserLogf(func(string, ...any) {}), WithEventBuffer(EventBuffer{MaxEvents: 256, MaxBytes: 2 << 20, OnOverflow: func(error) { overflows.Add(1) }}))
				if err != nil {
					stopBrowser()
					t.Fatal(err)
				}
				defer func() {
					stopBrowser()
					if err := browser.WaitStopped(context.Background()); err != nil {
						t.Error(err)
					}
				}()
				page, err := browser.newTarget(life, "page", "page-session")
				if err != nil {
					t.Fatal(err)
				}
				page.frames[page.cur] = &Frame{Frame: &cdp.Frame{ID: page.cur}, Nodes: make(map[cdp.NodeID]*Node)}
				go page.run(life)
				peer, err := browser.newTarget(life, "peer", "peer-session")
				if err != nil {
					t.Fatal(err)
				}
				go peer.run(life)
				transport.incoming <- &cdproto.Message{SessionID: page.SessionID, Method: "DOM.documentUpdated", Params: jsonv2.Value(`{}`)}
				synctest.Wait()
				ctx, cancel := context.WithTimeout(life, time.Second)
				defer cancel()
				if err := browser.Call(ctx, "Browser.getVersion", nil, nil); err != nil {
					t.Error("state queue blocked browser commands", err)
				}
				if err := peer.Call(ctx, "Runtime.evaluate", nil, nil); err != nil {
					t.Error("state queue blocked the independent peer", err)
				}
				if fixture.burst > 4096 {
					if overflows.Load() != 1 {
						t.Error("state queue did not report its overflow")
					}
					if err := page.Call(ctx, "Runtime.evaluate", nil, nil); !errors.Is(err, ErrEventOverflow) {
						t.Error("target stop lost the overflow cause", err)
					}
				} else if overflows.Load() != 0 {
					t.Error("state queue rejected the positive control")
				}
			})
		})
	}
}
