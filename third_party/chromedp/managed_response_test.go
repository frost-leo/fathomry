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
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
)

func responseTestEvent(target *Target, event any) {
	target.listenersMu.Lock()
	defer target.listenersMu.Unlock()
	target.listeners = runListeners(target.listeners, event)
}

func TestManagedResponseFiltersEventsAndChildFragments(t *testing.T) {
	ctx, cancel := NewContext(allocCtx, WithBrowserOption(WithEventBuffer(EventBuffer{MaxEvents: 2, MaxBytes: 2048})))
	defer cancel()
	ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := Do(ctx); err != nil {
		t.Fatal(err)
	}
	response, err := RunResponse(ctx, Func(func(work context.Context, target *Target) error {
		target.frameMu.RLock()
		frame := target.cur
		target.frameMu.RUnlock()
		for range 1000 {
			responseTestEvent(target, &runtime.EventConsoleAPICalled{})
			responseTestEvent(target, &cdproto.Message{ID: 1})
			responseTestEvent(target, &page.EventNavigatedWithinDocument{FrameID: "child"})
			responseTestEvent(target, &page.EventFrameNavigated{Frame: &cdp.Frame{ID: "child", ParentID: frame}, Type: page.NavigationTypeBackForwardCacheRestore})
			responseTestEvent(target, &network.EventRequestWillBeSent{FrameID: "child", Type: network.ResourceTypeDocument})
		}
		responseTestEvent(target, &network.EventRequestWillBeSent{FrameID: frame, LoaderID: "loader", RequestID: "request", Type: network.ResourceTypeDocument})
		responseTestEvent(target, &network.EventResponseReceived{FrameID: frame, RequestID: "request", Type: network.ResourceTypeDocument, Response: &network.Response{Status: 418}})
		responseTestEvent(target, &page.EventLifecycleEvent{FrameID: frame, LoaderID: "loader", Name: "init"})
		responseTestEvent(target, &page.EventLoadEventFired{})
		if work.Err() != nil {
			t.Fatal("successful navigation canceled subsequent steps", work.Err())
		}
		return nil
	}))
	if err != nil || response == nil || response.Status != 418 {
		t.Fatal("irrelevant events or a child fragment ended the root response wait", response, err)
	}
}

func TestManagedResponseBufferOverflowCancelsTrigger(t *testing.T) {
	for _, mode := range []string{"count", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := NewContext(allocCtx, WithBrowserOption(WithEventBuffer(EventBuffer{MaxEvents: 2, MaxBytes: 512})))
			defer cancel()
			ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := Do(ctx); err != nil {
				t.Fatal(err)
			}
			stepErr := errors.New("response trigger failed")
			_, err := RunResponse(ctx, Func(func(work context.Context, target *Target) error {
				if mode == "count" {
					for range 3 {
						responseTestEvent(target, &page.EventLoadEventFired{})
					}
				} else {
					target.frameMu.RLock()
					frame := target.cur
					target.frameMu.RUnlock()
					responseTestEvent(target, &network.EventResponseReceived{FrameID: frame, Type: network.ResourceTypeDocument, Response: &network.Response{URL: strings.Repeat("x", 1024)}})
				}
				select {
				case <-work.Done():
					return stepErr
				default:
					return errors.New("overflow did not cancel the trigger")
				}
			}))
			if !errors.Is(err, ErrEventOverflow) || !errors.Is(err, stepErr) {
				t.Fatal("response overflow or trigger error was lost", err)
			}
		})
	}
}

func TestManagedResponseRemovesListenerOnEveryExit(t *testing.T) {
	ctx, cancel := testAllocate(t, "")
	defer cancel()
	if err := Do(ctx); err != nil {
		t.Fatal(err)
	}
	target := FromContext(ctx).Target
	target.listenersMu.Lock()
	original := len(target.listeners)
	target.listenersMu.Unlock()
	for _, mode := range []string{"step", "cancel", "fragment", "bfcache"} {
		t.Run(mode, func(t *testing.T) {
			work, cancel := context.WithCancel(ctx)
			defer cancel()
			stepErr := errors.New("response trigger failed")
			_, err := RunResponse(work, Func(func(context.Context, *Target) error {
				switch mode {
				case "step":
					return stepErr
				case "cancel":
					cancel()
				case "fragment", "bfcache":
					target.frameMu.RLock()
					frame := target.cur
					target.frameMu.RUnlock()
					if mode == "fragment" {
						responseTestEvent(target, &page.EventNavigatedWithinDocument{FrameID: frame})
					} else {
						responseTestEvent(target, &page.EventFrameNavigated{Frame: &cdp.Frame{ID: frame}, Type: page.NavigationTypeBackForwardCacheRestore})
					}
				}
				return nil
			}))
			if mode == "step" && !errors.Is(err, stepErr) || mode == "cancel" && !errors.Is(err, context.Canceled) || (mode == "fragment" || mode == "bfcache") && err != nil {
				t.Fatal("response exit changed", err)
			}
			target.listenersMu.Lock()
			count := len(target.listeners)
			target.listenersMu.Unlock()
			if count > original {
				t.Fatal("response listener retained on an idle target", count, original)
			}
		})
	}
}
