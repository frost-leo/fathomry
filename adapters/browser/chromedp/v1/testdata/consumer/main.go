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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/chromedp/cdproto/debugger"
	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	cdpruntime "github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
	browser "github.com/frost-leo/fathomry/adapters/browser/chromedp/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("browser public consumer passed")
}
func run() error {
	facts, err := browser.Build()
	if err != nil {
		return err
	}
	if len(facts.Modules) != 2 {
		return fmt.Errorf("native metadata missing")
	}
	expectedKind := os.Getenv("FATHOMRY_TEST_SDK_REPLACEMENT")
	if expectedKind == "" {
		expectedKind = "local"
	}
	var core, protocol bool
	for _, module := range facts.Modules {
		if module.Path == "github.com/chromedp/chromedp" {
			core = module.Version == "v0.21.0" && module.ReplacementKind == expectedKind
			if expectedKind == "module" {
				core = core && module.ReplacementPath == "github.com/frost-leo/fathomry/third_party/chromedp" && module.ReplacementVersion == "v0.0.0-20261013094802-1825b9d94794" && module.ReplacementSum == "h1:717+zeAcxuc9pm5U1XGQHZCsJRVaMOil9wzYHfwvQkI="
			} else if expectedKind != "local" {
				core = false
			}
		}
		if module.Path == "github.com/chromedp/cdproto" {
			protocol = module.Version == "v0.157.9" && module.Sum != ""
		}
	}
	if !core || !protocol {
		return fmt.Errorf("selected native graph differs")
	}
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	live := os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") == "1" && path != ""
	if !live {
		path = "/explicitly-not-started-browser"
	}
	dir, err := os.MkdirTemp("", "browser-public-profile-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	p, err := browser.Prepare(browser.Settings{Name: "consumer", ExecPath: path, TempDir: dir, NewWindow: true, MaxSessions: new(1), MaxPages: new(1), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, browser.NativeOptions{})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	runtime, err := adapters.New(ctx, p.Policy().Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[browser.Result](p.Policy().Evidence)
	if err != nil {
		return err
	}
	owner, err := p.Open(ctx, browser.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		return err
	}
	defer owner.Close(context.Background())
	if live {
		retained, _, err := owner.Client().Retain(ctx, browser.ContextOptions{})
		if err != nil {
			return err
		}
		if _, err := retained.Page().Navigate(ctx, "data:text/html,<h1>independent</h1>"); err != nil {
			return err
		}
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
		value, _, err := browser.RunAction(ctx, retained.Page(), browser.Text(sdk.CSS("h1")))
		if err != nil {
			return err
		}
		if value != "independent" {
			return fmt.Errorf("native selector did not execute")
		}
		events, _, err := browser.Subscribe(ctx, retained.Page(), cdpruntime.ConsoleAPICalled)
		if err != nil {
			return err
		}
		if _, _, err := browser.RunAction(ctx, retained.Page(), browser.Evaluate[sdk.Void](`console.log("independent-event")`)); err != nil {
			return err
		}
		event, _, err := events.Next(ctx)
		if err != nil {
			return err
		}
		if len(event.Args) != 1 || event.Args[0] == nil || string(event.Args[0].Value) != `"independent-event"` {
			return fmt.Errorf("native event did not retain its typed data")
		}
		if err := events.Close(ctx); err != nil {
			return err
		}
		binding, _, err := browser.BindFunc(ctx, retained.Page(), "independentBinding", func(ctx context.Context, value int) (int, error) {
			if sdk.FromContext(ctx) != nil {
				return 0, fmt.Errorf("native owner escaped through callback context")
			}
			return value * 2, nil
		})
		if err != nil {
			return err
		}
		answer, _, err := browser.RunAction(ctx, retained.Page(), browser.Evaluate[int](`independentBinding(21)`, browser.EvalAwaitPromise()))
		if err != nil {
			return err
		}
		if answer != 42 {
			return fmt.Errorf("native binding did not settle its page promise")
		}
		if err := binding.Close(ctx); err != nil {
			return err
		}
		if !binding.Closed() {
			return fmt.Errorf("binding cleanup incomplete")
		}
		executions, _, err := retained.Page().ExecutionContexts(ctx)
		if err != nil || len(executions) == 0 {
			return fmt.Errorf("initial execution identity missing: %w", err)
		}
		frames, _, err := retained.Page().Frames(ctx)
		if err != nil || len(frames) != 1 {
			return fmt.Errorf("native frame identity missing: %w", err)
		}
		isolated, _, err := retained.Page().CreateWorld(ctx, frames[0], page.CreateIsolatedWorldParams{WorldName: "independent-world"})
		if err != nil {
			return err
		}
		group, _, err := retained.Page().NewObjectGroup(ctx)
		if err != nil {
			return err
		}
		object, _, err := group.EvaluateIn(ctx, isolated, cdpruntime.EvaluateParams{Expression: "Promise.resolve(47)", AwaitPromise: new(true), ReturnByValue: new(true)})
		if err != nil {
			return err
		}
		var metadata cdpruntime.RemoteObject
		if json.Unmarshal(object.DataCopy(), &metadata) != nil || string(metadata.Value) != "47" {
			return fmt.Errorf("isolated promise data missing")
		}
		if err := group.Close(ctx); err != nil {
			return err
		}
		if err := responseConsumer(ctx, retained.Page()); err != nil {
			return err
		}
		if err := debuggerConsumer(ctx, retained.Page()); err != nil {
			return err
		}
		if err := retained.Close(ctx); err != nil {
			return err
		}
	}
	if err := owner.Close(ctx); err != nil {
		return err
	}
	for {
		status, err := inbox.Inspect()
		if err != nil {
			return err
		}
		if status.Outstanding == 0 {
			break
		}
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		receipt, err := delivery.Receipt()
		if err != nil {
			return err
		}
		value, err := receipt.WaitReleased(ctx)
		if err != nil {
			return err
		}
		if value.Err() != nil {
			return value.Err()
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
	return nil
}

func debuggerConsumer(ctx context.Context, page *browser.Page) error {
	control, _, err := page.Control(ctx, browser.ControlOptions{Debugger: &debugger.EnableParams{MaxScriptsCacheSize: 65536}, SharedIsolateEffects: true})
	if err != nil {
		return err
	}
	defer control.Close(context.Background())
	done := make(chan error, 1)
	go func() {
		value, _, err := browser.RunAction(ctx, page, browser.Evaluate[bool](`(function independentFrame(){let value=42;debugger;return value===null;})()`))
		if err == nil && !value {
			err = fmt.Errorf("explicit frame null was lost")
		}
		done <- err
	}()
	for {
		pending, err := control.Next(ctx)
		if err != nil {
			return err
		}
		if pending.Method() != debugger.Paused.Method {
			continue
		}
		frame, err := pending.CallFrame(0)
		if err != nil {
			return err
		}
		if _, err := frame.SetVariable(ctx, debugger.SetVariableValueParams{ScopeNumber: 0, VariableName: "value"}, browser.ValueArgument(nil)); err != nil {
			return err
		}
		if err := pending.Reply(ctx, browser.ResumeDebugger(debugger.ResumeParams{})); err != nil {
			return err
		}
		break
	}
	select {
	case err := <-done:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return control.Close(ctx)
}

func responseConsumer(ctx context.Context, page *browser.Page) error {
	const body = "independent response"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Fixture") != "independent" || r.UserAgent() != "IndependentFixture/1" {
			http.Error(w, "page request settings missing", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	if _, _, err := browser.RunAction(ctx, page, browser.Command(network.SetExtraHTTPHeaders, network.SetExtraHTTPHeadersParams{Headers: network.Headers{"X-Fixture": "independent"}})); err != nil {
		return err
	}
	if _, _, err := browser.RunAction(ctx, page, browser.Command(emulation.SetUserAgentOverride, emulation.SetUserAgentOverrideParams{UserAgent: "IndependentFixture/1"})); err != nil {
		return err
	}
	if _, err := page.Navigate(ctx, server.URL); err != nil {
		return err
	}
	control, _, err := page.Control(ctx, browser.ControlOptions{Network: true, Fetch: &fetch.EnableParams{Patterns: []*fetch.RequestPattern{{URLPattern: server.URL + "/body", RequestStage: fetch.RequestStageResponse}}}})
	if err != nil {
		return err
	}
	defer control.Close(context.Background())
	if _, _, err := browser.RunAction(ctx, page, browser.Evaluate[bool](`window.responsePromise=fetch('/body').then(r=>r.text());true`)); err != nil {
		return err
	}
	var identity network.RequestID
	for identity == "" {
		pending, err := control.Next(ctx)
		if err != nil {
			return err
		}
		if pending.Method() != fetch.RequestPaused.Method {
			continue
		}
		var event fetch.EventRequestPaused
		if err := json.Unmarshal(pending.DataCopy(), &event); err != nil || event.NetworkID == "" {
			return fmt.Errorf("response network identity missing: %w", err)
		}
		identity = event.NetworkID
		value, err := pending.ResponseBody(ctx, 4096)
		if err != nil || string(value.Body) != body {
			return fmt.Errorf("intercepted native body differs: %w", err)
		}
		if err := pending.Reply(ctx, browser.ContinueResponse(fetch.ContinueResponseParams{})); err != nil {
			return err
		}
	}
	value, _, err := browser.RunAction(ctx, page, browser.Evaluate[string]("window.responsePromise", browser.EvalAwaitPromise()))
	if err != nil || value != body {
		return fmt.Errorf("continued native body differs: %w", err)
	}
	for {
		pending, err := control.Next(ctx)
		if err != nil {
			return err
		}
		if pending.Method() != network.LoadingFinished.Method {
			continue
		}
		var event network.EventLoadingFinished
		if err := json.Unmarshal(pending.DataCopy(), &event); err != nil {
			return err
		}
		if event.RequestID != identity {
			continue
		}
		request, err := pending.Request()
		if err != nil {
			return err
		}
		result, _, err := browser.RunRequestCommand(ctx, request, network.GetResponseBody, network.GetResponseBodyParams{})
		if err != nil || string(result.Body) != body {
			return fmt.Errorf("observed native body differs: %w", err)
		}
		return control.Close(ctx)
	}
}
