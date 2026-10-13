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
	"runtime"
	"strings"
	"testing"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/input"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveNodeHelperDoesNotHideNativeCleanupFailure(t *testing.T) {
	checkHelperCleanupFailure(t, "Runtime.releaseObject", Text("h1"))
}

func TestLiveNullValueKeepsIndependentHelperCleanupFailure(t *testing.T) {
	checkHelperCleanupFailure(t, "Runtime.releaseObject", Value(sdk.JSPath(`Object.assign(document.querySelector('h1'), {value:null})`)))
}

func TestLiveSearchHelperDoesNotRetryNativeCleanupFailure(t *testing.T) {
	checkHelperCleanupFailure(t, "DOM.discardSearchResults", Text("h1"))
}

func TestLiveDragKeepsSecondaryReleaseFailure(t *testing.T) {
	checkHelperCleanupFailure(t, "Input.dispatchMouseEvent", DragAndDropXY(1, 1, 20, 20))
}

func checkHelperCleanupFailure[T any](t *testing.T, method string, action Action[T]) {
	t.Helper()
	endpoint, _ := cleanupFaultPeer(t, map[string]int64{method: -1})
	prepared, err := Prepare(Settings{Name: "node-helper-cleanup", RemoteURL: endpoint, MaxSessions: new(1)}, NativeOptions{})
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
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<h1>helper cleanup</h1>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	_, receipt, err := RunAction(bounded(t), retained.Page(), action)
	var cause *cdproto.Error
	if !errors.As(err, &cause) || cause.Code != -32001 {
		t.Error("native node-helper release failure was discarded", err)
	}
	var cleanup *sdk.HelperCleanupError
	if !errors.As(err, &cleanup) || cleanup.Operation != method {
		t.Error("native cleanup attribution missing", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || snapshot.Err() == nil || snapshot.Cleanup() == nil {
		t.Error("independent helper evidence falsely reports success", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Error("known helper cleanup error prevented owned-context destruction")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveInputCallbackAbnormalExitKeepsCleanupEvidence(t *testing.T) {
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			endpoint, _ := cleanupFaultPeerMatching(t, map[string]int64{input.DispatchKeyEvent.Method: -1}, func(_ string, data json.RawMessage) bool {
				var params input.DispatchKeyEventParams
				return json.Unmarshal(data, &params) == nil && params.Type == input.DispatchKeyEventTypeKeyUp
			})
			prepared, err := Prepare(Settings{Name: "input-exit", RemoteURL: endpoint, MaxSessions: new(1)}, NativeOptions{})
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
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _, _ = RunAction(bounded(t), retained.Page(), KeyEvent("a", func(params *input.DispatchKeyEventParams) {
					if params.Type == input.DispatchKeyEventTypeChar {
						if mode == "goexit" {
							runtime.Goexit()
						}
						panic("private-input-option-canary")
					}
				}))
			}()
			select {
			case <-done:
			case <-bounded(t).Done():
				t.Fatal("abnormal callback did not join")
			}
			delivery, err := dependencies.Evidence.NextReleased(bounded(t))
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := delivery.Receipt()
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := receipt.WaitReleased(bounded(t))
			var cause *cdproto.Error
			if err != nil || snapshot.Primary() == nil || !errors.As(snapshot.Cleanup(), &cause) || cause.Code != -32001 {
				t.Error("abnormal input callback lost independent release failure", err)
			}
			if strings.Contains(fmt.Sprintf("%+v", snapshot.Err()), "private-") {
				t.Error("input callback or native cleanup text leaked")
			}
			if err := delivery.Ack(); err != nil {
				t.Fatal(err)
			}
			_ = owner.Close(bounded(t))
			if !owner.ShutdownComplete() {
				t.Fatal("abnormal callback prevented actual source cleanup")
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding)
		})
	}
}
