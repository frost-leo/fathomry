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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func browserOptions(t *testing.T) OptionsV1 {
	t.Helper()
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" {
		t.Skip("real browser opt-in absent; browser behavior is not verified")
	}
	executable := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if executable == "" || !filepath.IsAbs(executable) {
		t.Fatal("opt-in requires an explicit absolute test browser executable")
	}
	if _, err := os.Stat(executable); err != nil {
		t.Fatal("opt-in browser unavailable", err)
	}
	return OptionsV1{Name: "browser", ExecPath: executable, NewWindow: true, MaxSessions: 2, MaxEvents: 2048, Flags: map[string]string{
		"headless": "", "no-first-run": "", "no-default-browser-check": "", "no-proxy-server": "",
		"disable-background-networking": "", "disable-component-update": "", "disable-sync": "", "disable-extensions": "",
	}, TempDir: t.TempDir()}
}
func browserExecutor(t *testing.T, f *fixture) context.Context {
	t.Helper()
	if f.client.owner.root == nil || sdk.FromContext(f.client.owner.root).Browser == nil {
		t.Fatal("missing genuinely started browser")
	}
	return cdp.WithExecutor(deadline(t), sdk.FromContext(f.client.owner.root).Browser)
}
func browserContexts(t *testing.T, f *fixture) []cdp.BrowserContextID {
	t.Helper()
	ids, _, err := target.GetBrowserContexts().Do(browserExecutor(t, f))
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestBrowserRenderedDataRedirectsAndNativeExtensions(t *testing.T) {
	var peerRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		peerRequests.Add(1)
		switch request.URL.Path {
		case "/start":
			http.Redirect(writer, request, "/page", http.StatusFound)
		case "/page":
			writer.Header().Set("Content-Type", "text/html")
			io.WriteString(writer, `<html><body><div id="value">render-me</div><img src="/asset"><script>fetch("/background")</script></body></html>`)
		default:
			io.WriteString(writer, "synthetic")
		}
	}))
	defer server.Close()
	f := bindFixture(t, browserOptions(t), 1)
	if f.client.owner.started {
		t.Fatal("lazy construction started Chrome")
	}
	receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "render", Parent: "batch", Owner: "owner-a"}, func(session *Session) error {
		if count := len(browserContexts(t, f)); count != 1 {
			t.Fatalf("native BrowserContext count %d", count)
		}
		if err := session.Navigate(session.Context(), server.URL+"/start"); err != nil {
			return err
		}
		var value string
		if err := session.Actions(session.Context(), sdk.Evaluate("document.getElementById('value').textContent", &value), sdk.ActionFunc(func(ctx context.Context) error {
			if sdk.FromContext(ctx) != nil {
				return errors.New("owning native context escaped")
			}
			return nil
		})); err != nil {
			return err
		}
		if value != "render-me" {
			return errors.New("rendered data mismatch")
		}
		return session.Save("value", []byte(value))
	})
	if err != nil {
		t.Fatal(err)
	}
	settle(t, f, fault.Correlation{Call: "render", Parent: "batch", Owner: "owner-a"}, receipt, nil, nil, func(t testing.TB, value Result) {
		data, ok := value.DataCopy("value")
		if !ok || string(data) != "render-me" || !value.CallbackCompleted() || !value.ContextReleased() || value.Navigations() != 1 || value.Commands() != 1 || value.RequestEvents() < 3 {
			t.Error("data, navigation or cleanup evidence differs")
		}
	})
	if len(browserContexts(t, f)) != 0 || peerRequests.Load() < 3 {
		t.Fatal("native cleanup or peer traffic oracle failed")
	}
	args, ok := f.client.LaunchArgumentsCopy()
	if !ok || slices.Contains(args, "--no-sandbox") || !slices.Contains(args, "--remote-debugging-port=0") {
		t.Fatal("sandbox or dynamic debugging-port rule violated")
	}
	pid := f.client.owner.process.Process.Pid
	directory := f.client.owner.profileDir
	t.Log("browser", f.client.Profile().ServiceVersion.Value, "protocol", f.client.Profile().Protocol.Value)
	if err := f.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.client.owner.processDone:
	default:
		t.Fatal("process not joined")
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("owned profile remains")
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("owned browser process remains")
	}
}

func TestBrowserCookieAndStorageIsolation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/html")
		io.WriteString(writer, "<html><body>synthetic</body></html>")
	}))
	defer server.Close()
	f := bindFixture(t, browserOptions(t), 2)
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var group sync.WaitGroup
	errorsFound := make(chan error, 2)
	receipts := make(chan *invocation.Receipt[Result], 2)
	for _, identity := range []string{"alpha", "beta"} {
		group.Go(func() {
			receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: identity}, func(session *Session) error {
				if err := session.Navigate(session.Context(), server.URL); err != nil {
					return err
				}
				var cookie string
				if err := session.Actions(session.Context(), network.SetCookie("technical", identity).WithURL(server.URL), sdk.Evaluate("localStorage.setItem('technical', '"+identity+"')", nil)); err != nil {
					return err
				}
				ready <- struct{}{}
				select {
				case <-release:
				case <-session.Context().Done():
					return session.Context().Err()
				}
				if err := session.Actions(session.Context(), sdk.Evaluate("document.cookie + ':' + localStorage.getItem('technical')", &cookie)); err != nil {
					return err
				}
				if cookie != "technical="+identity+":"+identity {
					return errors.New("browser-context state mixed")
				}
				return session.Save("state", []byte(cookie))
			})
			receipts <- receipt
			errorsFound <- err
		})
	}
	for range 2 {
		select {
		case <-ready:
		case <-time.After(10 * time.Second):
			close(release)
			group.Wait()
			t.Fatal("isolated sessions did not reach barrier")
		}
	}
	if len(browserContexts(t, f)) != 2 {
		t.Error("separate Go contexts did not create separate browser contexts")
	}
	close(release)
	group.Wait()
	for range 2 {
		if err := <-errorsFound; err != nil {
			t.Error(err)
		}
	}
	// Validate each independent inbox record against its frozen correlation, not
	// against output obtained from the same result under test.
	for range 2 {
		receipt := <-receipts
		result, err := receipt.WaitReleased(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		expected := "technical=" + result.Context.Correlation.Call + ":" + result.Context.Correlation.Call
		data, ok := result.Outcome.Value.DataCopy("state")
		if !ok || string(data) != expected || result.Err() != nil || !result.Outcome.Value.ContextReleased() {
			t.Fatal("isolated result mismatch")
		}
	}
	for range 2 {
		delivery, err := f.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if len(browserContexts(t, f)) != 0 {
		t.Fatal("isolated contexts leaked")
	}
}

func TestBrowserTargetCreationFailureDisposesContext(t *testing.T) {
	options := browserOptions(t)
	options.NewWindow = false
	f := bindFixture(t, options, 1)
	receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "target-negative"}, func(*Session) error { t.Error("callback after failed target creation"); return nil })
	if err == nil {
		t.Fatal("tuple no longer reproduces target-creation counterexample; requalify default")
	}
	var native *cdproto.Error
	if !errors.As(err, &native) || native.Code != -32000 {
		t.Fatal("original native CDP failure lost", err)
	}
	settle(t, f, fault.Correlation{Call: "target-negative"}, receipt, ErrNative, nil, func(t testing.TB, value Result) {
		if value.CallbackCompleted() || !value.ContextReleased() {
			t.Error("failed target creation leaked or invented completion")
		}
	})
	if len(browserContexts(t, f)) != 0 {
		t.Fatal("partially created BrowserContext leaked")
	}
}

func TestBrowserCanceledCleanupRetainsNativeQuota(t *testing.T) {
	options := browserOptions(t)
	options.MaxSessions = 1
	f := bindFixture(t, options, 2)
	cleanup, cancel := context.WithCancel(context.Background())
	cancel()
	receipt, err := f.client.Run(deadline(t), cleanup, fault.Correlation{Call: "pending"}, func(session *Session) error { return session.Save("empty", nil) })
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cleanup budget failure lost")
	}
	settle(t, f, fault.Correlation{Call: "pending"}, receipt, nil, context.Canceled, func(t testing.TB, value Result) {
		if value.ContextReleased() {
			t.Error("unconfirmed cleanup certified")
		}
	})
	if len(browserContexts(t, f)) != 1 {
		t.Fatal("context was silently removed despite absent cleanup authority")
	}
	second, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "capacity"}, func(*Session) error { t.Error("pending native capacity bypassed"); return nil })
	if !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("pending native context did not retain quota", err)
	}
	settle(t, f, fault.Correlation{Call: "capacity"}, second, resource.ErrCapacity, nil, func(t testing.TB, value Result) {
		if value.CallbackCompleted() {
			t.Error("rejected callback completed")
		}
	})
	if err := f.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestBrowserBorrowedCancellationPreservesOtherContexts(t *testing.T) {
	host := bindFixture(t, browserOptions(t), 1)
	if err := host.client.owner.start(deadline(t)); err != nil {
		t.Fatal(err)
	}
	externalID, err := target.CreateBrowserContext().WithDisposeOnDetach(true).Do(browserExecutor(t, host))
	if err != nil {
		t.Fatal(err)
	}
	defer target.DisposeBrowserContext(externalID).Do(browserExecutor(t, host))
	address, err := host.client.owner.debugURL(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	borrowed := bindFixture(t, OptionsV1{Name: "borrowed", RemoteURL: address, NewWindow: true, MaxSessions: 2}, 2)
	entered := make(chan struct{})
	allowSibling := make(chan struct{})
	canceled, cancel := context.WithCancelCause(deadline(t))
	cause := errors.New("synthetic caller cancellation")
	results := make(chan *invocation.Receipt[Result], 2)
	failures := make(chan error, 2)
	var group sync.WaitGroup
	group.Go(func() {
		receipt, err := borrowed.client.Run(canceled, deadline(t), fault.Correlation{Call: "cancel"}, func(session *Session) error {
			close(entered)
			<-session.Context().Done()
			return session.Context().Err()
		})
		results <- receipt
		failures <- err
	})
	<-entered
	group.Go(func() {
		receipt, err := borrowed.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "sibling"}, func(session *Session) error {
			close(allowSibling)
			select {
			case <-canceled.Done():
			case <-session.Context().Done():
				return session.Context().Err()
			}
			var actual int
			if err := session.Actions(session.Context(), sdk.Evaluate("21*2", &actual)); err != nil {
				return err
			}
			if actual != 42 {
				return errors.New("sibling browser failed")
			}
			return session.Save("value", []byte("42"))
		})
		results <- receipt
		failures <- err
	})
	<-allowSibling
	cancel(cause)
	group.Wait()
	var canceledCount int
	for range 2 {
		err := <-failures
		if err != nil {
			if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
				t.Error("cancel cause lost", err)
			}
			canceledCount++
		}
		receipt := <-results
		value, err := receipt.WaitReleased(deadline(t))
		if err != nil || !value.Outcome.Value.ContextReleased() {
			t.Error("session cleanup failed", err)
		}
	}
	if canceledCount != 1 {
		t.Fatal("cancellation affected sibling")
	}
	for range 2 {
		delivery, err := borrowed.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if err := borrowed.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	ids := browserContexts(t, host)
	if len(ids) != 1 || ids[0] != externalID {
		t.Fatal("borrowed close affected foreign BrowserContext")
	}
	if _, _, _, _, _, err := browser.GetVersion().Do(browserExecutor(t, host)); err != nil {
		t.Fatal("borrowed close killed browser")
	}
}
