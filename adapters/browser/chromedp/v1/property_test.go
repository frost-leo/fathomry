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
	goruntime "runtime"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestLiveJavascriptPropertiesPreserveNativeTypesAndCustody(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<h1 title='original'>property</h1>", 64)
	page := retained.Page()
	if _, _, err := RunAction(bounded(t), page, SetJavascriptAttribute(sdk.CSS("h1"), "title", "updated")); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	value, _, err := RunAction(bounded(t), page, JavascriptAttribute[string](sdk.CSS("h1"), "title"))
	if err != nil || value != "updated" {
		t.Fatal("named native property changed", err)
	}
	drain(t, dependencies, 1)
	zero, _, err := RunAction(bounded(t), page, JavascriptAttribute[int](sdk.CSS("h1"), "onclick"))
	if err != nil || zero != 0 {
		t.Fatal("native property null normalization changed", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), page, JavascriptAttribute[int](sdk.CSS("h1"), "missingFixtureProperty")); !errors.Is(err, sdk.ErrJSUndefined) {
		t.Fatal("undefined property lost native distinction", err)
	}
	drain(t, dependencies, 1)
	remote, receipt, err := RunAction(bounded(t), page, JavascriptAttribute[*runtime.RemoteObject](sdk.CSS("h1"), "style", NodeVisible()))
	if err != nil || remote == nil || remote.ObjectID == "" {
		t.Fatal("remote property reference missing", err)
	}
	object, err := page.Object(remote)
	if err != nil {
		t.Fatal("remote property escaped managed custody", err)
	}
	if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("property group refunded while result is live")
	}
	if err := object.Close(bounded(t)); err != nil || !object.Closed() {
		t.Fatal("property group did not close", err)
	}
	drain(t, dependencies, 1)
	if _, receipt, err := object.Call(bounded(t), "function(){return this}"); !errors.Is(err, adapters.ErrClosed) || receipt != nil {
		t.Fatal("closed property object retained authority", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("property groups survived owned cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLivePropertyConcurrentReadinessAndLostFinalReplyKeepCustody(t *testing.T) {
	const marker = "property-final-canary"
	endpoint, _ := cleanupFaultPeerReplies(t, nil, nil, func(data []byte) []byte {
		var reply struct {
			Result struct {
				Result struct{ Value string }
			}
		}
		if json.Unmarshal(data, &reply) == nil && reply.Result.Result.Value == marker {
			return nil
		}
		return data
	})
	prepared, err := Prepare(Settings{Name: "property-reply", RemoteURL: endpoint}, NativeOptions{})
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
	if _, err := retained.Page().Navigate(bounded(t), `data:text/html,<body><script>globalThis.readCount=0;for(let i=0;i<16;i++){let n=document.createElement('span');n.textContent='visible';Object.defineProperty(n,'fixture',{get(){readCount++;return '`+marker+`'}});document.body.append(n)}</script>`); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, receipt, err := RunAction(ctx, retained.Page(), JavascriptAttribute[string](sdk.CSSAll("span"), "fixture", NodeVisible(), AtLeast(16)))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("lost final reply did not remain unknown", err)
	}
	if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("earlier lookup replies falsely settled a later property producer")
	}
	count, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int]("readCount"))
	if err != nil || count != 1 {
		t.Fatal("property getter was retried or did not run", count, err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("owned context did not settle the lost property producer")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Primary(), context.DeadlineExceeded) {
		t.Fatal("property receipt lost its original failure", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

type propertyExitValue struct{}

func (*propertyExitValue) UnmarshalJSON(data []byte) error {
	if string(data) == `"goexit"` {
		goruntime.Goexit()
	}
	panic("property decoder fixture")
}

func TestLivePropertyDecoderExitKeepsIndependentCleanup(t *testing.T) {
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			endpoint, _ := cleanupFaultPeer(t, map[string]int64{runtime.ReleaseObject.Method: -1})
			prepared, err := Prepare(Settings{Name: "property-exit", RemoteURL: endpoint}, NativeOptions{})
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
			if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<h1 title='"+mode+"'>property</h1>"); err != nil {
				t.Fatal(err)
			}
			drain(t, dependencies, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _, _ = RunAction(bounded(t), retained.Page(), JavascriptAttribute[propertyExitValue](sdk.CSS("h1"), "title"))
			}()
			select {
			case <-done:
			case <-bounded(t).Done():
				t.Fatal("property decoder did not exit")
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
				t.Fatal("abnormal decoder lost independent native cleanup", err)
			}
			if err := delivery.Ack(); err != nil {
				t.Fatal(err)
			}
			_ = owner.Close(bounded(t))
			if !owner.ShutdownComplete() {
				t.Fatal("property decoder prevented native cleanup")
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding)
		})
	}
}
