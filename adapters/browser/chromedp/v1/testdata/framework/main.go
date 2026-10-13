/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/chromedp/cdproto/network"
	cdpruntime "github.com/chromedp/cdproto/runtime"
	browser "github.com/frost-leo/fathomry/adapters/browser/chromedp/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("browser framework consumer passed")
}
func run() error {
	for _, policy := range []resource.Policy{resource.Fixed, resource.Follow} {
		if err := exercise(policy); err != nil {
			return err
		}
	}
	return nil
}
func exercise(selection resource.Policy) (result error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	live := os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") == "1" && path != ""
	if !live {
		path = "/explicitly-not-started"
	}
	const responseBody = "<title>independent request</title>"
	var server *httptest.Server
	if live {
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(responseBody))
		}))
		defer server.Close()
	}
	directory, err := os.MkdirTemp("", "browser-framework-profile-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	config := browser.Settings{Name: "generation", ExecPath: path, TempDir: directory, MaxSessions: new(2), MaxPages: new(1), MaxStreams: new(1),
		Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}
	prepared, err := browser.Prepare(config, browser.NativeOptions{})
	if err != nil {
		return err
	}
	policy := prepared.Policy()
	policy.Runtime.MaxActive *= 2
	policy.Runtime.MaxWorkBytes *= 2
	policy.Evidence.Capacity *= 2
	policy.Evidence.MaxBytes *= 2
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, runtime.Close(context.Background())) }()
	inbox, err := adapters.NewInbox[browser.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := browser.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	ref, err := resource.Bind(runtime.Resources(), resource.Binding[int, browser.Handle]{
		Name: "browser", Policy: selection,
		Select: func(view settings.View) (int, error) {
			snapshot, err := settings.As[int](view)
			if err != nil {
				return 0, err
			}
			return snapshot.ValueCopy()
		},
		Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right },
		Build: func(lifetime context.Context, generation int) (*resource.Instance[browser.Handle], error) {
			value := config
			value.Name = fmt.Sprintf("generation-%d", generation)
			owner, err := browser.Open(lifetime, value, dependencies)
			if owner == nil {
				return nil, err
			}
			return &resource.Instance[browser.Handle]{Value: owner.Handle(), Release: owner.Release}, err
		},
	})
	if err != nil {
		return err
	}
	apply := func(value int) error {
		snapshot, err := settings.New(value, func(value int) int { return value })
		if err != nil {
			return err
		}
		update, err := runtime.Resources().Apply(ctx, snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	if err := apply(1); err != nil {
		return err
	}
	client, err := browser.Using(ctx, ref, prepared.Policy().Budget, dependencies)
	if err != nil {
		return err
	}
	var old *browser.Context
	var first *adapters.Receipt[browser.Result]
	var oldExecution *browser.ExecutionContext
	var oldRequest browser.Request
	var oldControl *browser.Control
	if live {
		old, first, err = client.Retain(ctx, browser.ContextOptions{})
		if err != nil {
			return err
		}
		defer old.Close(context.Background())
		oldControl, _, err = old.Page().Control(ctx, browser.ControlOptions{Network: true})
		if err != nil {
			return err
		}
		if _, err := old.Page().Navigate(ctx, server.URL); err != nil {
			return err
		}
		oldRequest, err = observeDocument(ctx, oldControl)
		if err != nil {
			return err
		}
		if _, _, err := browser.RunAction(ctx, old.Page(), browser.Evaluate[int]("window.marker=7")); err != nil {
			return err
		}
		values, _, err := old.Page().ExecutionContexts(ctx)
		if err != nil || len(values) == 0 {
			return errors.Join(err, errors.New("execution identity missing"))
		}
		oldExecution = values[0]
	}
	if err := apply(2); err != nil {
		return err
	}
	if live {
		current, second, err := client.Retain(ctx, browser.ContextOptions{})
		if err != nil {
			return err
		}
		defer current.Close(context.Background())
		oldInfo, _ := first.Snapshot()
		newInfo, _ := second.Snapshot()
		if oldInfo.Info().Source.Generation == 0 {
			return errors.New("source attribution absent")
		}
		same := oldInfo.Info().Source.Generation == newInfo.Info().Source.Generation
		if same != (selection == resource.Fixed) {
			return errors.New("generation selection changed")
		}
		body, bodyReceipt, err := browser.RunRequestCommand(ctx, oldRequest, network.GetResponseBody, network.GetResponseBodyParams{})
		if err != nil || string(body.Body) != responseBody {
			return errors.Join(err, errors.New("request grant lost its original response"))
		}
		bodySnapshot, err := bodyReceipt.WaitReleased(ctx)
		if err != nil {
			return err
		}
		bodyEvidence, ok := bodySnapshot.ValueCopy()
		if !ok || bodyEvidence.Attribution().Source.Generation != oldInfo.Info().Source.Generation {
			return errors.New("request evidence followed a replacement generation")
		}
		if value, _, err := browser.RunAction(ctx, old.Page(), browser.Evaluate[int]("window.marker")); err != nil || value != 7 {
			return errors.Join(err, errors.New("retained page retargeted"))
		}
		group, _, err := old.Page().NewObjectGroup(ctx)
		if err != nil {
			return err
		}
		object, record, err := group.EvaluateIn(ctx, oldExecution, cdpruntime.EvaluateParams{Expression: "window.marker", ReturnByValue: new(true)})
		if err != nil {
			return err
		}
		var metadata cdpruntime.RemoteObject
		if json.Unmarshal(object.DataCopy(), &metadata) != nil || string(metadata.Value) != "7" {
			return errors.New("execution identity followed a new generation")
		}
		observed, err := record.WaitReleased(ctx)
		if err != nil {
			return err
		}
		evidence, ok := observed.ValueCopy()
		if !ok || evidence.Attribution().Source.Generation != oldInfo.Info().Source.Generation {
			return errors.New("execution evidence lost its retained generation")
		}
		if err := group.Close(ctx); err != nil {
			return err
		}
		if value, _, err := browser.RunAction(ctx, current.Page(), browser.Evaluate[string]("typeof window.marker")); err != nil || value != "undefined" {
			return errors.Join(err, errors.New("fresh context storage leaked"))
		}
		if err := current.Close(ctx); err != nil {
			return err
		}
		if err := oldControl.Close(ctx); err != nil {
			return err
		}
		if err := old.Close(ctx); err != nil {
			return err
		}
	}
	if err := runtime.Resources().Close(ctx); err != nil {
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
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil {
			return err
		}
		if snapshot.Err() != nil {
			return snapshot.Err()
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
	return nil
}

func observeDocument(ctx context.Context, control *browser.Control) (browser.Request, error) {
	var identity network.RequestID
	for {
		pending, err := control.Next(ctx)
		if err != nil {
			return browser.Request{}, err
		}
		switch pending.Method() {
		case network.RequestWillBeSent.Method:
			var event network.EventRequestWillBeSent
			if err := json.Unmarshal(pending.DataCopy(), &event); err != nil {
				return browser.Request{}, err
			}
			if event.Type == network.ResourceTypeDocument {
				identity = event.RequestID
			}
		case network.LoadingFinished.Method:
			var event network.EventLoadingFinished
			if err := json.Unmarshal(pending.DataCopy(), &event); err != nil {
				return browser.Request{}, err
			}
			if identity != "" && event.RequestID == identity {
				return pending.Request()
			}
		}
	}
}
