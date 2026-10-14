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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestReceiptFailureFallbackKeepsPendingOwnershipSeparate(t *testing.T) {
	prepared, err := Prepare(Settings{Name: "dispatch", ExecPath: "/not-started"}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	endpoint, err := bind(dependencies)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("private-dispatch-cause")
	var call *adapters.Call[Result]
	var guard adapters.Guard
	var holdErr error
	receipt, err := endpoint.Run(context.Background(), request("dispatch", "", 1, 65536), func(accepted *adapters.Call[Result]) {
		call = accepted
		guard, holdErr = call.Hold()
	})
	if err != nil {
		t.Fatal(err)
	}
	if holdErr != nil {
		t.Fatal(holdErr)
	}
	if err := receiptError(receipt, nil); err != nil {
		t.Fatal("pending retained work reported as failed", err)
	}
	if err := call.Resolve(adapters.Outcome[Result]{Primary: want}); err != nil {
		t.Fatal(err)
	}
	if err := receiptError(receipt, nil); !errors.Is(err, want) || strings.Contains(err.Error(), want.Error()) {
		t.Fatal("resolved dispatch failure lost safe presentation or cause", err)
	}
	if err := guard.Release(); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
}

func TestRunCallbackErrorPrivacy(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	p, err := Prepare(Settings{Name: "review", ExecPath: path, TempDir: t.TempDir(), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := mechanisms(t, p)
	owner, err := p.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	const canary = "review-private-callback-canary"
	cause := errors.New(canary)
	receipt, err := owner.Client().Run(context.Background(), ContextOptions{}, func(*Scope) error { return cause })
	if !errors.Is(err, cause) {
		t.Fatal("deliberate cause inspection was lost")
	}
	if receipt == nil {
		t.Fatal("callback receipt is absent")
	}
	snapshot, waitErr := receipt.WaitReleased(bounded(t))
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	if strings.Contains(fmt.Sprint(snapshot.Err()), canary) {
		t.Fatal("independent receipt leaked the canary")
	}
	t.Log("control: independent receipt hides the canary and errors.Is retains the original cause")
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(verb, err), canary) {
			t.Errorf("Run returned an unredacted callback cause through %s", verb)
		}
	}
	var log bytes.Buffer
	slog.New(slog.NewJSONHandler(&log, nil)).Error("callback failed", "error", err)
	if strings.Contains(log.String(), canary) {
		t.Error("Run returned an unredacted callback cause through slog")
	}
}

func TestContextOptionsPrivacy(t *testing.T) {
	const canary = "review-private-proxy-canary"
	options := ContextOptions{ProxyServer: "http://user:" + canary + "@127.0.0.1:1234"}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(verb, options), canary) {
			t.Errorf("ContextOptions exposes proxy credentials through %s", verb)
		}
	}
	var log bytes.Buffer
	slog.New(slog.NewJSONHandler(&log, nil)).Info("context options", "options", options)
	if strings.Contains(log.String(), canary) {
		t.Error("ContextOptions exposes proxy credentials through slog")
	}
}

func TestRetainWithoutGenerationReportsFailure(t *testing.T) {
	for _, policy := range []resource.Policy{resource.Fixed, resource.Follow} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			prepared, err := Prepare(Settings{Name: "review", ExecPath: "/no-browser-needed"}, NativeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			dependencies := mechanisms(t, prepared)
			scope, err := resource.New(context.Background(), resource.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = scope.Close(bounded(t)) })
			ref, err := resource.Bind(scope, resource.Binding[int, Handle]{
				Name: "uninitialized", Policy: policy,
				Select: func(settings.View) (int, error) { return 1, nil },
				Clone:  func(value int) int { return value },
				Equal:  func(left, right int) bool { return left == right },
				Build: func(context.Context, int) (*resource.Instance[Handle], error) {
					t.Error("generation construction must not run before Apply")
					return nil, errors.New("unexpected construction")
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			client, err := Using(context.Background(), ref, prepared.Policy().Budget, dependencies)
			if err != nil {
				t.Fatal(err)
			}
			retained, receipt, err := client.Retain(bounded(t), ContextOptions{})
			if receipt == nil || retained != nil {
				t.Fatal("unexpected source acquisition result")
			}
			snapshot, waitErr := receipt.WaitReleased(bounded(t))
			if waitErr != nil || snapshot.Err() == nil {
				t.Fatal("control: failed source acquisition lacks evidence", waitErr)
			}
			t.Log("control: receipt correctly reports source acquisition failure")
			if err == nil {
				t.Error("Retain returned a nil error and nil Context for failed source acquisition")
			}
		})
	}
}

type reviewPollingArgument struct{ calls *atomic.Int64 }

func (argument reviewPollingArgument) MarshalJSON() ([]byte, error) {
	argument.calls.Add(1)
	return []byte("true"), nil
}

func TestPollAdmissionPrecedesArgumentEncoding(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	p, err := Prepare(Settings{Name: "review-poll", ExecPath: path, TempDir: t.TempDir(), MaxCommands: new(1), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := mechanisms(t, p)
	owner, err := p.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = retained.Close(bounded(t)) })
	value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int]("1"))
	if err != nil || value != 1 {
		t.Fatal("control: first admitted command failed", err)
	}
	var calls atomic.Int64
	_, _, err = RunAction(bounded(t), retained.Page(), PollFunction[bool]("value => value", WithPollingArgs(reviewPollingArgument{calls: &calls})))
	if err == nil {
		t.Fatal("control: exhausted command budget did not reject Poll")
	}
	t.Logf("control: exhausted command budget rejects Poll; argument codec calls=%d", calls.Load())
	if calls.Load() != 0 {
		t.Error("Poll encoded caller arguments before rejecting exhausted command admission")
	}
}
