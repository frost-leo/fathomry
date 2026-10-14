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
	_ "embed"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/input"
)

//go:embed js/managedInputEvents.js
var managedInputEventsJS string

//go:embed js/managedInputState.js
var managedInputStateJS string

func TestManagedTouchSwipeRejectsInvalidInputBeforePress(t *testing.T) {
	ctx, cancel := testAllocate(t, "")
	defer cancel()
	if err := Do(ctx); err != nil {
		t.Fatal(err)
	}
	calls := 0
	work := WithCallLimits(ctx, CallLimits{Before: func(string) error { calls++; return nil }})
	for _, action := range []Action[Void]{
		TouchSwipeXY(0, 0, 0, 10, 0), TouchSwipeXY(0, 0, 0, 10, -time.Second),
		TouchSwipeXY(math.NaN(), 0, 0, 10, time.Second), TouchSwipeXY(0, 0, math.Inf(1), 10, time.Second),
	} {
		if err := Do(work, action); err == nil || calls != 0 {
			t.Fatal("invalid swipe dispatched native input", err, calls)
		}
	}
}

func TestManagedSendKeysWhileAttributesChange(t *testing.T) {
	ctx, cancel := testAllocate(t, "managed_dynamic_input.html")
	defer cancel()
	ctx, cancel = context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for range 200 {
		if err := Do(ctx, SendKeys(CSS("#field"), "x")); err != nil {
			t.Fatal(err)
		}
	}
	value, err := Run(ctx, Value(CSS("#field")))
	if err != nil || value != strings.Repeat("x", 200) {
		t.Fatal("typing changed while node attributes were updated", len(value), err)
	}
}

func TestManagedInputHelpersReleaseAfterAdmissionFailure(t *testing.T) {
	for _, fixture := range []struct {
		name, method, field string
		limit               int
		action              Action[Void]
	}{
		{"mouse-click", input.DispatchMouseEvent.Method, "mouse", 1, MouseClickXY(20, 20)},
		{"tap", input.DispatchTouchEvent.Method, "touch", 1, TapXY(20, 20)},
		{"touch-swipe", input.DispatchTouchEvent.Method, "touch", 1, TouchSwipeXY(20, 20, 40, 40, 100*time.Millisecond)},
		{"key", input.DispatchKeyEvent.Method, "key", 1, KeyEvent("a")},
		{"drag-release", input.DispatchMouseEvent.Method, "mouse", 2, DragAndDropXY(20, 20, 40, 40, 1)},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			ctx, cancel := testAllocate(t, "")
			defer cancel()
			ctx, cancel = context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if err := Do(ctx, Navigate("data:text/html,<body><div style='width:200px;height:200px'>input fixture</div></body>")); err != nil {
				t.Fatal(err)
			}
			if _, err := Run(ctx, Evaluate[Void](managedInputEventsJS)); err != nil {
				t.Fatal(err)
			}
			calls := 0
			work := WithCallLimits(ctx, CallLimits{Before: func(method string) error {
				if method == fixture.method {
					calls++
					if calls > fixture.limit {
						return ErrCommandLimit
					}
				}
				return nil
			}})
			if err := Do(work, fixture.action); !errors.Is(err, ErrCommandLimit) {
				t.Fatal("original admission refusal missing", err)
			}
			state, err := Run(ctx, Evaluate[map[string]bool](managedInputStateJS))
			if err != nil {
				t.Fatal(err)
			}
			if state[fixture.field] {
				t.Fatal("composite helper left its native input pressed")
			}
		})
	}
}

func TestManagedInputReleaseKeepsAcknowledgementAndDoesNotReplay(t *testing.T) {
	for _, mode := range []string{"refused", "canceled-before", "native-error", "lost-reply", "output-limit", "cleanup-failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int64
			cause := &cdproto.Error{Code: -32001, Message: "release fixture"}
			target := managedReplyTarget(t, func(message *cdproto.Message) *cdproto.Message {
				calls.Add(1)
				if string(message.Method) != input.DispatchMouseEvent.Method {
					t.Error("unexpected release method", message.Method)
				}
				switch mode {
				case "native-error", "cleanup-failure":
					return &cdproto.Message{Error: cause}
				case "lost-reply":
					cancel()
					return nil
				}
				return &cdproto.Message{Result: jsonv2.Value(`{}`)}
			})
			var observed *HelperCleanupError
			limits := CallLimits{Cleanup: func(err *HelperCleanupError) { observed = err }}
			if mode == "refused" || mode == "cleanup-failure" {
				limits.Before = func(string) error { return ErrCommandLimit }
			}
			if mode == "output-limit" {
				limits.MaxOutputBytes = 1
			}
			if mode == "canceled-before" {
				cancel()
			}
			err := helperReleaseInput(WithCallLimits(ctx, limits), target, input.DispatchMouseEvent, input.DispatchMouseEventParams{Type: input.DispatchMouseEventTypeMouseReleased, Button: input.MouseButtonLeft})
			if calls.Load() != 1 || err == nil {
				t.Fatal("release was repeated or its primary failure disappeared", calls.Load(), err)
			}
			wantCleanup := mode == "native-error" || mode == "lost-reply" || mode == "cleanup-failure"
			if isHelperCleanup(err) != wantCleanup || (observed != nil) != wantCleanup {
				t.Fatal("queued or acknowledged release was misclassified", err)
			}
			if (mode == "native-error" || mode == "cleanup-failure") && !errors.Is(err, cause) {
				t.Fatal("native release failure lost its cause", err)
			}
		})
	}
}

func TestManagedKeyPreparationChargesOnlyActualPackets(t *testing.T) {
	for _, control := range []bool{false, true} {
		t.Run(map[bool]string{false: "saturated-char", true: "suppressed-char"}[control], func(t *testing.T) {
			ctx, cancel := testAllocate(t, "")
			defer cancel()
			if err := Do(ctx, Navigate("data:text/html,<input>")); err != nil {
				t.Fatal(err)
			}
			limit, expectedCallbacks := 1, 1
			if control {
				limit, expectedCallbacks = 2, 3
			}
			admitted, queued, callbacks := 0, 0, 0
			work := WithCallLimits(ctx, CallLimits{
				Before: func(string) error {
					if admitted == limit {
						return ErrCommandLimit
					}
					admitted++
					return nil
				},
				Queued: func(string) { queued++ },
			})
			err := Do(work, KeyEvent("a", func(params *input.DispatchKeyEventParams) {
				callbacks++
				if control {
					KeyModifiers(ModifierCtrl)(params)
				}
			}))
			if control && err != nil || !control && !errors.Is(err, ErrCommandLimit) || callbacks != expectedCallbacks || queued != limit || admitted != limit {
				t.Fatal("key preparation or skipped-packet accounting changed", callbacks, admitted, queued, err)
			}
		})
	}
}
