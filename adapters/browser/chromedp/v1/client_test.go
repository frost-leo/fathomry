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
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func mechanisms(t *testing.T, p Prepared) Dependencies {
	t.Helper()
	runtime, err := adapters.New(context.Background(), p.Policy().Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](p.Policy().Evidence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return Dependencies{Runtime: runtime, Evidence: inbox}
}
func bounded(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func drain(t *testing.T, d Dependencies, expected int) {
	t.Helper()
	for range expected {
		delivery, err := d.Evidence.NextReleased(bounded(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestOfflinePreparationAndZero(t *testing.T) {
	settings := Settings{Name: "test", ExecPath: "/no-such-browser", Flags: map[string]string{"headless": ""}}
	p, err := Prepare(settings, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	settings.Flags["headless"] = "mutated"
	if err := Validate(Settings{Name: "zero", ExecPath: "/no-browser", MaxPages: new(0)}); err == nil {
		t.Fatal("explicit zero defaulted")
	}
	if _, err := json.Marshal(p); !errors.Is(err, ErrSerialization) {
		t.Fatal("preparation serialized", err)
	}
	d := mechanisms(t, p)
	owner, err := p.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("offline cleanup", err)
	}
	drain(t, d, 1)
}
func TestLiveClosedActionsAndIndependentEvidence(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	p, err := Prepare(Settings{Name: "test", ExecPath: path, NewWindow: true, TempDir: t.TempDir(), Flags: map[string]string{"headless": "", "disable-background-networking": "", "no-first-run": "", "no-default-browser-check": ""}, MaxSessions: new(1), MaxPages: new(2)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := mechanisms(t, p)
	owner, err := p.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(bounded(t)); err != nil {
			t.Error(err)
		}
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<input id="q"><button id="b" onclick="document.getElementById('out').textContent=document.getElementById('q').value">go</button><div id="out"></div>`))
	}))
	defer server.Close()
	retained, receipt, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	page := retained.Page()
	copied := *retained
	navigation, err := page.Navigate(bounded(t), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := navigation.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	// Claim the live context record before reading child records.
	rootDelivery, err := d.Evidence.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	// The first record belongs to the source, the second to the retained context.
	contextDelivery, err := d.Evidence.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	_, _, err = RunAction(bounded(t), page, SendKeys(sdk.CSS("#q"), "managed"))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	_, _, err = RunAction(bounded(t), page, Click(sdk.CSS("#b")))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	value, actionReceipt, err := RunAction(bounded(t), page, Poll[string]("document.getElementById('out').textContent"))
	if err != nil || value != "managed" {
		t.Fatal(value, err)
	}
	delivery, err := d.Evidence.NextReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	independentReceipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	independent, err := independentReceipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	direct, err := actionReceipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	independentValue, present := independent.ValueCopy()
	directValue, directPresent := direct.ValueCopy()
	if !present || !directPresent || string(independentValue.PayloadCopy()) != string(directValue.PayloadCopy()) {
		t.Fatal("independent result lost")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	second, _, err := retained.NewPage(bounded(t))
	if err != nil || second == nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	if err := retained.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if err := copied.Close(bounded(t)); err != nil {
		t.Fatal("copied context lost repeatable cleanup", err)
	}
	if _, err := receipt.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(context.Background(), page, Evaluate[int]("42")); err == nil {
		t.Fatal("closed page revived")
	}
	if err := contextDelivery.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if err := rootDelivery.Ack(); err != nil {
		t.Fatal(err)
	}
}
