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
	"testing"
	"time"

	"github.com/chromedp/cdproto"
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
