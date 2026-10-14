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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestNativeDownloadEventsAreTargetScoped(t *testing.T) {
	f := bindFixture(t, browserOptions(t), 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/file" {
			w.Header().Set("Content-Disposition", "attachment; filename=fixture.txt")
			_, _ = w.Write([]byte("synthetic download"))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<a id=d href='/file'>download</a>"))
	}))
	defer server.Close()
	first, err := f.client.Retain(deadline(t), fault.Correlation{Call: "download-one"})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	second, err := f.client.Retain(deadline(t), fault.Correlation{Call: "download-two"})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(context.Background())
	for _, retained := range []*Context{first, second} {
		if err := retained.Page().Navigate(deadline(t), server.URL); err != nil {
			t.Fatal(err)
		}
	}
	target := sdk.FromContext(first.Page().native).Target
	raw, stop := target.SubscribeMany(browser.DownloadWillBegin.Method, browser.DownloadProgress.Method)
	defer stop()
	other, stopOther := sdk.FromContext(second.Page().native).Target.Subscribe(browser.DownloadProgress.Method)
	defer stopOther()
	for _, retained := range []*Context{first, second} {
		_, err := cdp.Call(deadline(t), sdk.FromContext(retained.Page().native).Target, browser.SetDownloadBehavior, browser.SetDownloadBehaviorParams{Behavior: browser.SetDownloadBehaviorBehaviorAllowAndName, DownloadPath: t.TempDir(), BrowserContextID: retained.Page().browserID, EventsEnabled: new(true)})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := RunAction(deadline(t), second.Page(), Click(sdk.CSS("#d"))); err != nil {
		t.Fatal(err)
	}
	for completed := false; !completed; {
		select {
		case data := <-other:
			var progress browser.EventDownloadProgress
			if err := json.Unmarshal(data, &progress); err != nil {
				t.Fatal(err)
			}
			completed = progress.State == browser.DownloadProgressStateCompleted
		case <-second.Context().Done():
			t.Fatal("the negative-control download did not actually complete")
		}
	}
	select {
	case <-raw:
		t.Fatal("another context's download crossed the native target event scope")
	case <-time.After(80 * time.Millisecond):
	}
	if _, err := RunAction(deadline(t), first.Page(), Click(sdk.CSS("#d"))); err != nil {
		t.Fatal(err)
	}
	begun := ""
	for {
		select {
		case data := <-raw:
			var msg cdproto.Message
			if err := json.Unmarshal(data, &msg); err != nil {
				t.Fatal(err)
			}
			switch string(msg.Method) {
			case browser.DownloadWillBegin.Method:
				var ev browser.EventDownloadWillBegin
				if err := json.Unmarshal(msg.Params, &ev); err != nil {
					t.Fatal(err)
				}
				begun = ev.GUID
			case browser.DownloadProgress.Method:
				var ev browser.EventDownloadProgress
				if err := json.Unmarshal(msg.Params, &ev); err != nil {
					t.Fatal(err)
				}
				if begun == "" || ev.GUID != begun {
					t.Fatal("progress was not associated with the target's begin")
				}
				if ev.State == browser.DownloadProgressStateCompleted {
					return
				}
			}
		case <-first.Context().Done():
			t.Fatal("native download completion unavailable")
		}
	}
}
