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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	jsonv2 "github.com/go-json-experiment/json"
)

func TestBrowserNativeInterceptionAndOriginalVersusDOM(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/html")
		io.WriteString(writer, `<html><body><div id="value">original</div><script>document.getElementById('value').textContent='rendered'</script></body></html>`)
	}))
	defer server.Close()
	f := bindFixture(t, browserOptions(t), 1)
	receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "interception"}, func(session *Session) error {
		if err := session.Actions(session.Context(), fetch.Enable().WithPatterns([]*fetch.RequestPattern{{URLPattern: server.URL + "/*", ResourceType: network.ResourceTypeDocument}})); err != nil {
			return err
		}
		navigation, cancel := context.WithCancel(session.Context())
		done := make(chan struct{})
		var navigationErr error
		go func() { navigationErr = session.Navigate(navigation, server.URL); close(done) }()
		defer func() { cancel(); <-done }()
		var requestID network.RequestID
		for requestID == "" {
			event, err := session.NextEvent(session.Context())
			if err != nil {
				return err
			}
			if event.Type() != "*fetch.EventRequestPaused" {
				continue
			}
			var paused fetch.EventRequestPaused
			if err := jsonv2.Unmarshal(event.DataCopy(), &paused, sdk.DefaultUnmarshalOptions); err != nil {
				return err
			}
			requestID = paused.NetworkID
			if err := session.Actions(session.Context(), fetch.ContinueRequest(paused.RequestID)); err != nil {
				return err
			}
		}
		<-done
		if navigationErr != nil {
			return navigationErr
		}
		var original []byte
		var rendered string
		if err := session.Actions(session.Context(), sdk.ActionFunc(func(ctx context.Context) error {
			var err error
			original, err = network.GetResponseBody(requestID).Do(ctx)
			return err
		}), sdk.Evaluate("document.getElementById('value').textContent", &rendered)); err != nil {
			return err
		}
		if !strings.Contains(string(original), ">original<") || rendered != "rendered" {
			return errors.New("original bytes and rendered DOM were conflated")
		}
		original = nil
		return session.Save("dom", []byte(rendered))
	})
	if err != nil {
		t.Fatalf("native interception failed (state=%t, limit=%t): %v", errors.Is(err, ErrState), errors.Is(err, ErrLimit), err)
	}
	settle(t, f, fault.Correlation{Call: "interception"}, receipt, nil, nil, func(t testing.TB, value Result) {
		data, ok := value.DataCopy("dom")
		if !ok || string(data) != "rendered" || !value.ContextReleased() {
			t.Error("native output mismatch")
		}
	})
}

func TestBrowserAliasesShareAdmissionAndQueuedCancellation(t *testing.T) {
	options := browserOptions(t)
	options.MaxSessions = 1
	options.QueuedCalls = 1
	f := bindFixture(t, options, 2)
	alias := resource.Borrow("alias", f.assembly, f.selected)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "borrower", alias)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(deadline(t))
	client, err := Bind(assembly, alias, f.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if client.owner != f.client.owner || client.access.Info().Configuration.Identity.Name != "browser" {
		t.Fatal("alias created another identity or owner")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	var first *invocation.Receipt[Result]
	var firstErr error
	go func() {
		first, firstErr = f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "held"}, func(*Session) error { close(entered); <-release; return nil })
		close(done)
	}()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		<-done
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("session did not start")
	}
	wait, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	receipt, err := client.Run(wait, deadline(t), fault.Correlation{Call: "queued"}, func(*Session) error { t.Error("expired queued call executed"); return nil })
	if receipt != nil || !errors.Is(err, context.DeadlineExceeded) || len(browserContexts(t, f)) != 1 {
		t.Fatal("alias exceeded shared native/admission capacity", err)
	}
	use := f.assembly.Snapshot().Sources[0].Usage
	if use.Active != 1 || use.Queued != 0 {
		t.Fatal("queue cancellation leaked accounting")
	}
	close(release)
	<-done
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	settle(t, f, fault.Correlation{Call: "held"}, first, nil, nil, func(t testing.TB, value Result) {
		if !value.ContextReleased() {
			t.Error("held context leaked")
		}
	})
	if err := assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestBrowserCancellationDoesNotProveRemoteNoEffect(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var effects atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		close(entered)
		<-release
		effects.Add(1)
		io.WriteString(writer, "completed-after-cancellation")
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f := bindFixture(t, browserOptions(t), 1)
	ctx, cancel := context.WithCancelCause(deadline(t))
	defer cancel(context.Canceled)
	cause := errors.New("synthetic navigation cancellation")
	done := make(chan struct{})
	var receipt *invocation.Receipt[Result]
	var runErr error
	go func() {
		receipt, runErr = f.client.Run(ctx, deadline(t), fault.Correlation{Call: "remote-effect"}, func(session *Session) error { return session.Navigate(session.Context(), server.URL) })
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("navigation did not reach actual peer")
	}
	cancel(cause)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("canceled navigation did not finish")
	}
	if !errors.Is(runErr, cause) || !errors.Is(runErr, context.Canceled) {
		t.Fatal("cancellation cause lost", runErr)
	}
	settle(t, f, fault.Correlation{Call: "remote-effect"}, receipt, context.Canceled, nil, func(t testing.TB, value Result) {
		if value.CallbackCompleted() || !value.ContextReleased() {
			t.Error("execution and local cleanup conflated")
		}
	})
	if effects.Load() != 0 || len(browserContexts(t, f)) != 0 {
		t.Fatal("fault control not synchronized")
	}
	close(release)
	until := time.After(time.Second)
	for effects.Load() == 0 {
		select {
		case <-until:
			t.Fatal("peer effect was not observed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestBrowserNativeBoundsAndIgnoredFailureEvidence(t *testing.T) {
	for _, kind := range []string{"input", "output"} {
		t.Run(kind, func(t *testing.T) {
			options := browserOptions(t)
			if kind == "input" {
				options.MaxCommandBytes = 64
			} else {
				options.MaxResultBytes = 64
			}
			f := bindFixture(t, options, 1)
			output := "unchanged"
			receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: kind}, func(session *Session) error {
				expression := "'" + strings.Repeat("x", 1024) + "'"
				if kind == "output" {
					expression = "'x'.repeat(1024)"
				}
				actionErr := session.Actions(session.Context(), sdk.Evaluate(expression, &output))
				if !errors.Is(actionErr, ErrLimit) {
					return errors.New("native limit was not enforced")
				}
				return nil
			})
			if !errors.Is(err, ErrLimit) || output != "unchanged" {
				t.Fatal("caught limit failure disappeared or decoded excess output", err)
			}
			settle(t, f, fault.Correlation{Call: kind}, receipt, ErrLimit, nil, func(t testing.TB, value Result) {
				if !value.ContextReleased() {
					t.Error("limit failure leaked BrowserContext")
				}
			})
		})
	}
}

func TestBrowserExplicitReadinessIsNotNavigation(t *testing.T) {
	options := browserOptions(t)
	options.CheckReady = true
	f := bindFixture(t, options, 1)
	if f.client.owner.process == nil || f.client.owner.versionCopy().product == "" || len(browserContexts(t, f)) != 0 || f.inbox.Usage().Outstanding != 0 {
		t.Fatal("readiness and operation execution were conflated")
	}
}

func TestBrowserRuntimeProxyDoesNotMutateNamedSource(t *testing.T) {
	options := browserOptions(t)
	delete(options.Flags, "no-proxy-server")
	var routed atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Host == "synthetic.invalid" {
			routed.Add(1)
		}
		writer.Header().Set("Content-Type", "text/html")
		io.WriteString(writer, "<html><body>proxy-owned-fixture</body></html>")
	}))
	defer proxy.Close()
	f := bindFixture(t, options, 1)
	identity := f.client.access.Info()
	receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "proxy"}, func(session *Session) error {
		if err := session.Navigate(session.Context(), "http://synthetic.invalid/"); err != nil {
			return err
		}
		var body string
		if err := session.Actions(session.Context(), sdk.Evaluate("document.body.innerText", &body)); err != nil {
			return err
		}
		if body != "proxy-owned-fixture" {
			return errors.New("runtime proxy not used")
		}
		return nil
	}, SessionOptionsV1{ProxyServer: proxy.URL})
	if err != nil {
		t.Fatal(err)
	}
	settle(t, f, fault.Correlation{Call: "proxy"}, receipt, nil, nil, func(t testing.TB, value Result) {
		if !value.ContextReleased() {
			t.Error("proxy context leaked")
		}
	})
	if routed.Load() == 0 || f.client.access.Info().Configuration.Revision != identity.Configuration.Revision {
		t.Fatal("proxy input changed source or never reached local proxy")
	}
}
