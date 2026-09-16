/**
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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func deadline(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type fixture struct {
	cleanupCause error
	client       *Client
	assembly     *resource.Assembly
	selected     resource.Selection[Source]
	inbox        *invocation.Inbox[Result]
}

func bindFixture(t *testing.T, options OptionsV1, count int) *fixture {
	t.Helper()
	life, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	selected, err := Select(life, options)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "test", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](count, defaults(options).evidenceBytes()*int64(count))
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{client: client, assembly: assembly, selected: selected, inbox: inbox}
	t.Cleanup(func() {
		err := assembly.Close(deadline(t))
		status := assembly.Snapshot().Sources[0]
		if (f.cleanupCause == nil && err != nil || f.cleanupCause != nil && !errors.Is(err, f.cleanupCause)) || !status.Released || !status.Quiescent {
			t.Error("assembly cleanup incomplete or failed", err)
		}
	})
	return f
}
func settle(t *testing.T, f *fixture, id fault.Correlation, receipt *invocation.Receipt[Result], primary, cleanup error, check func(testing.TB, Result)) invocation.Result[Result] {
	t.Helper()
	if receipt == nil {
		t.Fatal("missing accepted receipt")
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	expected := conformance.Expected[Result]{Context: fault.Context{Operation: "session", Provider: ProviderID, Scope: "test", Source: f.client.access.Info().Configuration.Identity.Name, Correlation: id}, Source: f.client.access.Info(), Limits: f.client.access.Limits(), Shape: invocation.Session, Present: true, Final: true, Released: true, Primary: primary, Cleanup: cleanup, Attempts: invocation.Attempts{}, Value: check}
	conformance.Result(t, result, expected)
	conformance.Receive(t, deadline(t), f.inbox, []conformance.Expected[Result]{expected})
	return result
}
func inertOptions() OptionsV1 {
	return OptionsV1{Name: "browser", ExecPath: "/not-a-real-chrome", MaxSessions: 1}
}
func unitSession(t *testing.T) *Session {
	t.Helper()
	work, cancel := context.WithCancelCause(deadline(t))
	t.Cleanup(func() { cancel(context.Canceled) })
	value := defaults(inertOptions())
	return &Session{sessionState: &sessionState{browserContext: &browserContext{}, owner: &owner{settings: value}, work: work, cancel: cancel, native: context.Background(), events: make(chan Event, value.MaxEvents), data: resultData{output: make(map[string][]byte)}}}
}

func TestConfigurationPreparationAndFrozenFlags(t *testing.T) {
	life, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, change := range []func(*OptionsV1){
		func(value *OptionsV1) { value.ExecPath = "chrome" },
		func(value *OptionsV1) { value.RemoteURL = "ws://127.0.0.1:9222/devtools/browser/test" },
		func(value *OptionsV1) { value.Flags = map[string]string{"no-sandbox": ""} },
		func(value *OptionsV1) { value.Flags = map[string]string{"user-data-dir": "/not-owned"} },
		func(value *OptionsV1) { value.Flags = map[string]string{"remote-debugging-port": "9222"} },
		func(value *OptionsV1) { value.Flags = map[string]string{"-bad": ""} },
		func(value *OptionsV1) { value.MaxSessions = -1 },
		func(value *OptionsV1) { value.SessionTimeout = -time.Second },
		func(value *OptionsV1) { value.Version = 2 },
	} {
		options := inertOptions()
		change(&options)
		if _, err := Select(life, options); err == nil {
			t.Error("unsafe or invalid configuration accepted")
		}
	}
	options := inertOptions()
	options.Flags = map[string]string{"user-agent": "private-canary"}
	selected, err := Select(life, options, resource.Layer{Kind: resource.Local, Content: []byte("max_commands: 12")})
	if err != nil {
		t.Fatal(err)
	}
	options.Flags["user-agent"] = "mutated"
	assembly, err := resource.Assemble(deadline(t), deadline(t), "frozen", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(deadline(t))
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	if source.owner.settings.Flags["user-agent"] != "private-canary" || source.owner.settings.MaxCommands != 12 || source.owner.started {
		t.Fatal("configuration mutated or construction started browser")
	}
	conformance.Private(t, options, "private-canary", "mutated")
	conformance.Private(t, source, "private-canary")
	if _, err := Select(context.Background(), inertOptions()); err == nil {
		t.Fatal("uncancellable resource lifetime accepted")
	}
	if _, err := Select(life, inertOptions(), resource.Layer{Kind: resource.Local, Content: []byte("max_commands: 0")}); err == nil {
		t.Fatal("explicit layer zero reapplied default")
	}
}

func TestEvidenceRejectsBeforeStartup(t *testing.T) {
	f := bindFixture(t, inertOptions(), 1)
	held, err := invocation.Begin(deadline(t), f.client.access, invocation.Request{Name: "held", Correlation: fault.Correlation{Call: "held"}, Shape: invocation.Finite, Bytes: 1, EvidenceBytes: f.client.EvidenceBytes(), Admission: invocation.Budget{Limit: time.Second}}, f.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "rejected"}, func(*Session) error { t.Error("callback executed"); return nil })
	if receipt != nil || !errors.Is(err, invocation.ErrEvidence) || f.client.owner.started {
		t.Fatal("saturated evidence reached browser")
	}
	held.Complete(invocation.Outcome[Result]{})
	delivery, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStartupFailureKeepsCauseAndRemovesOwnedProfile(t *testing.T) {
	options := inertOptions()
	options.TempDir = t.TempDir()
	f := bindFixture(t, options, 1)
	receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "startup"}, func(*Session) error { t.Error("callback on failed startup"); return nil })
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatal("lost executable failure cause", err)
	}
	settle(t, f, fault.Correlation{Call: "startup"}, receipt, os.ErrNotExist, nil, func(t testing.TB, value Result) {
		if value.CallbackCompleted() {
			t.Error("failed startup called complete")
		}
	})
	directory := f.client.owner.profileDir
	if !strings.HasPrefix(directory, options.TempDir+string(filepath.Separator)) {
		t.Fatal("profile not owned")
	}
	if err := f.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("profile not removed")
	}
}

func TestScopedActionsRejectOwningContextAndRetainedExecutors(t *testing.T) {
	session := unitSession(t)
	var held context.Context
	if err := session.Actions(deadline(t), sdk.ActionFunc(func(ctx context.Context) error {
		held = ctx
		if sdk.FromContext(ctx) != nil {
			t.Fatal("native owner escaped")
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := cdp.Execute(context.WithoutCancel(held), "Runtime.evaluate", nil, nil); !errors.Is(err, ErrState) {
		t.Fatal("retained executor stayed live")
	}
	for _, method := range []string{"Browser.close", "Target.createTarget", "Target.exposeDevToolsProtocol", "Storage.clearDataForOrigin", "Page.setDownloadBehavior", "Network.clearBrowserCookies", "Fetch.takeResponseBodyAsStream"} {
		if permitted(method) {
			t.Errorf("authority escape allowed: %s", method)
		}
		err := session.Actions(deadline(t), sdk.ActionFunc(func(ctx context.Context) error { return cdp.Execute(ctx, method, nil, nil) }))
		if !errors.Is(err, ErrUnsupported) {
			t.Fatal("native denied command not rejected", err)
		}
	}
	var nilAction *network.EnableParams
	if err := session.Actions(deadline(t), nilAction); !errors.Is(err, ErrInput) {
		t.Fatal("typed nil action accepted")
	}
	cause := errors.New("private-error-canary")
	err := session.Actions(deadline(t), sdk.ActionFunc(func(context.Context) error { panic(cause) }))
	if !errors.Is(err, cause) || !errors.Is(err, ErrPanic) {
		t.Fatal("panic cause lost")
	}
	conformance.Private(t, err, "private-error-canary")
}

func TestOutputCopiesAndEventOverflowRemainEvidence(t *testing.T) {
	session := unitSession(t)
	data := []byte("private-output")
	if err := session.Save("value", data); err != nil {
		t.Fatal(err)
	}
	data[0] = 'X'
	if err := session.Save("empty", nil); err != nil {
		t.Fatal(err)
	}
	result := Result{data: &session.data}
	copy, ok := result.DataCopy("value")
	if !ok || string(copy) != "private-output" {
		t.Fatal("saved output aliases input")
	}
	copy[0] = 'X'
	next, _ := result.DataCopy("value")
	if string(next) != "private-output" {
		t.Fatal("copy mutates evidence")
	}
	empty, ok := result.DataCopy("empty")
	if !ok || empty == nil || len(empty) != 0 {
		t.Fatal("empty output became absent")
	}
	if _, ok := result.DataCopy("missing"); ok {
		t.Fatal("missing became present")
	}
	session.owner.settings.MaxEventBytes = 1
	session.observe(&network.EventLoadingFinished{})
	if !errors.Is(session.primary, ErrLimit) || !result.EventOverflow() || session.work.Err() == nil {
		t.Fatal("event overflow silently lost")
	}
	conformance.Private(t, result, "private-output")
}

func TestProfilesDoNotInventBrowserFacts(t *testing.T) {
	f := bindFixture(t, inertOptions(), 1)
	if profile := f.client.Profile(); profile.ServiceVersion.Kind != compatibility.UnknownFact || profile.Protocol.Kind != compatibility.UnknownFact {
		t.Fatal("lazy source advertised readiness")
	}
	f.client.owner.version = browserVersion{product: "Chrome/150.0.7871.114", protocol: "1.3"}
	profile := f.client.Profile()
	if profile.ServiceVersion.Value != "Chrome-150.0.7871.114" || profile.Protocol.Value != "cdp-1.3" {
		t.Fatal("axes conflated")
	}
	if reflect.DeepEqual(profile, compatibility.Profile{}) {
		t.Fatal("missing profile")
	}
}

func TestActionDeadlineCannotBeSwallowed(t *testing.T) {
	session := unitSession(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("action-cancellation-cause")
	err := session.Actions(ctx, sdk.ActionFunc(func(context.Context) error {
		cancel(cause)
		<-ctx.Done()
		return nil
	}))
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatal("action returned nil despite cancellation or lost cause", err)
	}
}

func TestSelectionRechecksLifetimeAtConstruction(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	selected, err := Select(ctx, inertOptions())
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("resource-lifetime-ended")
	cancel(cause)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "canceled", selected)
	if !errors.Is(err, cause) {
		t.Fatal("construction lost lifetime cancellation", err)
	}
	if assembly != nil {
		_ = assembly.Close(deadline(t))
	}
}

func FuzzCommandAuthority(f *testing.F) {
	for _, value := range []string{"Browser.close", "Runtime.evaluate", "Target.createTarget", "", "Fetch.continueRequest"} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, method string) {
		if permitted(method) && (!strings.Contains(method, ".") || strings.HasPrefix(method, "Browser.") || strings.HasPrefix(method, "Target.") || strings.HasPrefix(method, "Storage.")) {
			t.Fatal("global authority escaped")
		}
	})
}
