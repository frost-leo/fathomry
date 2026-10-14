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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/input"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveWheelContainerAndTouchScrollOutcomes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><style>html,body{margin:0;touch-action:auto;scroll-behavior:auto}body{width:2400px;height:5000px}#box{position:fixed;left:50px;top:50px;width:160px;height:180px;overflow:auto}#inside{width:800px;height:900px;background:linear-gradient(red,blue)}#bottom{position:absolute;top:4200px}</style><div id="box"><div id="inside"></div></div><div id="bottom">bottom</div><script>window.touchState={started:0,ended:0,canceled:0,active:false};addEventListener('touchstart',()=>{touchState.started++;touchState.active=true},{passive:true});addEventListener('touchend',()=>{touchState.ended++;touchState.active=false},{passive:true});addEventListener('touchcancel',()=>{touchState.canceled++;touchState.active=false},{passive:true});</script>`))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 2048)
	page := retained.Page()
	pageActionValue(t, page, dependencies, EmulateViewport(800, 600))
	reset := func() {
		t.Helper()
		pageActionValue(t, page, dependencies, Evaluate[bool](`scrollTo(0,0);new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(()=>r(true))))`, EvalAwaitPromise()))
	}
	pageActionValue(t, page, dependencies, MouseEvent(input.DispatchMouseEventTypeMouseWheel, 450, 300, func(params *input.DispatchMouseEventParams) { params.DeltaY = 350 }))
	pageActionValue(t, page, dependencies, Poll[bool]("scrollY > 100", WithPollingTimeout(3*time.Second)))
	reset()
	pageActionValue(t, page, dependencies, Command(input.DispatchMouseEvent, input.DispatchMouseEventParams{Type: input.DispatchMouseEventTypeMouseWheel, X: 450, Y: 300, DeltaX: 350}))
	pageActionValue(t, page, dependencies, Poll[bool]("scrollX > 100 && scrollY === 0", WithPollingTimeout(3*time.Second)))
	reset()
	pageActionValue(t, page, dependencies, Command(input.EmulateTouchFromMouseEvent, input.EmulateTouchFromMouseEventParams{Type: input.EmulateTouchFromMouseEventTypeMouseWheel, X: 80, Y: 100, Button: input.MouseButtonNone, DeltaY: -250}))
	pageActionValue(t, page, dependencies, Poll[bool]("document.querySelector('#box').scrollTop > 100 && scrollY === 0", WithPollingTimeout(3*time.Second)))
	pageActionValue(t, page, dependencies, ScrollIntoView(sdk.CSS("#bottom")))
	if !pageActionValue(t, page, dependencies, Evaluate[bool]("scrollY > 3000 && document.querySelector('#bottom').getBoundingClientRect().bottom <= innerHeight")) {
		t.Fatal("selector scrolling did not reveal offscreen content")
	}
	reset()
	pageActionValue(t, page, dependencies, Command(emulation.SetTouchEmulationEnabled, emulation.SetTouchEmulationEnabledParams{Enabled: true, MaxTouchPoints: 1}))
	pageActionValue(t, page, dependencies, TouchSwipeXY(300, 550, 300, 250, 320*time.Millisecond))
	pageActionValue(t, page, dependencies, Poll[bool]("scrollY > 100 && touchState.started === 1 && touchState.ended === 1 && !touchState.active", WithPollingTimeout(3*time.Second)))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, receipt, err := RunAction(ctx, page, TouchSwipeXY(300, 500, 300, 200, time.Hour))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("touch swipe did not return caller cancellation", err)
	}
	snapshot, waitErr := receipt.WaitReleased(bounded(t))
	if waitErr != nil || !errors.Is(snapshot.Err(), context.DeadlineExceeded) {
		t.Fatal("canceled swipe lost its independent failure", waitErr)
	}
	drain(t, dependencies, 1)
	if !pageActionValue(t, page, dependencies, Evaluate[bool]("touchState.started === 2 && touchState.canceled === 1 && !touchState.active")) {
		t.Fatal("canceled swipe retained its touch contact")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("input cleanup retained the owned browser")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
