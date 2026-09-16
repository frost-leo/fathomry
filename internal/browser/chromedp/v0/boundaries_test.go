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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/emulation"
	cdpio "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func TestSessionCopiesCannotMultiplyOutputAllowance(t *testing.T) {
	session := unitSession(t)
	session.owner.settings.MaxResultBytes = 10
	if err := session.Save("a", []byte("1234")); err != nil {
		t.Fatal(err)
	}
	copied := *session
	if err := copied.Save("b", []byte("5678")); err != nil {
		t.Fatal(err)
	}
	if err := session.Save("c", []byte("9")); !errors.Is(err, ErrLimit) {
		t.Fatal("copy manufactured another output allowance")
	}
}
func TestEventWaitCancellationIsNotExecutionCancellation(t *testing.T) {
	// Waiting has its own cancellation fact; it is not a native command failure.
	session := unitSession(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("only-the-wait-ended")
	cancel(cause)
	if _, err := session.NextEvent(ctx); !errors.Is(err, cause) {
		t.Fatal("event wait cause lost", err)
	}
	if session.work.Err() != nil || session.primary != nil {
		t.Fatal("event wait ended browser execution")
	}
	if err := session.Save("alive", nil); err != nil {
		t.Fatal("session did not remain usable", err)
	}
}

func TestAttributionOracleRejectsWrongParentAndOwner(t *testing.T) {
	if os.Getenv("FATHOMRY_CHROMEDP_ATTRIBUTION_CONTROL") == "1" {
		f := bindFixture(t, inertOptions(), 1)
		call, err := invocation.Begin(deadline(t), f.client.access, invocation.Request{Name: "session", Correlation: fault.Correlation{Call: "call", Parent: "wrong-parent", Owner: "wrong-owner"}, Shape: invocation.Session, Bytes: 1, EvidenceBytes: f.client.EvidenceBytes(), Admission: invocation.Budget{Limit: time.Second}}, f.inbox, nil)
		if err != nil {
			t.Fatal(err)
		}
		call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: &resultData{}}})
		settle(t, f, fault.Correlation{Call: "call", Parent: "original-parent", Owner: "original-owner"}, call.Receipt(), nil, nil, func(testing.TB, Result) {})
		return
	}
	command := exec.CommandContext(deadline(t), os.Args[0], "-test.run=^TestAttributionOracleRejectsWrongParentAndOwner$")
	command.Env = append(os.Environ(), "FATHOMRY_CHROMEDP_ATTRIBUTION_CONTROL=1")
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "execution attribution") {
		t.Fatal("independent attribution oracle did not reject the wrong correlation")
	}
}

func TestBrowserInlinePDFAndTargetEmulationRemainUsable(t *testing.T) {
	f := bindFixture(t, browserOptions(t), 1)
	id := fault.Correlation{Call: "native-positive"}
	receipt, err := f.client.Run(deadline(t), deadline(t), id, func(session *Session) error {
		var pdf []byte
		var width int
		if err := session.Actions(session.Context(), sdk.ActionFunc(func(ctx context.Context) error {
			var stream cdpio.StreamHandle
			var err error
			pdf, stream, err = page.PrintToPDF().Do(ctx)
			if stream != "" {
				return errors.New("inline PDF returned an owning stream")
			}
			return err
		}), emulation.SetDeviceMetricsOverride(320, 240, 1, false), sdk.Evaluate("window.innerWidth", &width)); err != nil {
			return err
		}
		if !strings.HasPrefix(string(pdf), "%PDF-") || width != 320 {
			return errors.New("native positive control failed")
		}
		pdf = nil
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	settle(t, f, id, receipt, nil, nil, func(t testing.TB, value Result) {
		if !value.ContextReleased() {
			t.Error("native positive control leaked context")
		}
	})
}
func TestDebuggerAddressPublicationIsNotAtomic(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "DevToolsActivePort")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	own := &owner{profileDir: directory, processDone: make(chan struct{})}
	written := make(chan error, 1)
	go func() {
		time.Sleep(30 * time.Millisecond)
		written <- os.WriteFile(path, []byte("12345\n/devtools/browser/synthetic\n"), 0600)
	}()
	address, err := own.debugURL(deadline(t))
	if writeErr := <-written; writeErr != nil {
		t.Fatal(writeErr)
	}
	if err != nil || address != "ws://127.0.0.1:12345/devtools/browser/synthetic" {
		t.Fatal("partial publication became permanent startup failure", err)
	}
}
func TestBrowserRefusesGlobalScreensAndNativeStreams(t *testing.T) {
	for _, mode := range []string{"screen", "stream"} {
		t.Run(mode, func(t *testing.T) {
			f := bindFixture(t, browserOptions(t), 1)
			receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: mode}, func(session *Session) error {
				native := cdp.WithExecutor(session.Context(), sdk.FromContext(session.native).Target)
				return session.Actions(session.Context(), sdk.ActionFunc(func(ctx context.Context) error {
					if mode == "screen" {
						screen, err := emulation.AddScreen(800, 0, 320, 240).WithLabel("synthetic-gh52").Do(ctx)
						if screen != nil {
							if cleanup := emulation.RemoveScreen(screen.ID).Do(native); cleanup != nil {
								t.Error("test-created screen cleanup failed", cleanup)
							}
						}
						return err
					}
					_, stream, err := page.PrintToPDF().WithTransferMode(page.PrintToPDFTransferModeReturnAsStream).Do(ctx)
					if stream != "" {
						if cleanup := cdpio.Close(stream).Do(native); cleanup != nil {
							t.Error("test-created stream cleanup failed", cleanup)
						}
					}
					return err
				}))
			})
			if !errors.Is(err, ErrUnsupported) {
				t.Error("global/resource-producing command was accepted", err)
			}
			result, waitErr := receipt.WaitReleased(deadline(t))
			if waitErr != nil {
				t.Fatal(waitErr)
			}
			if result.Outcome.Value.Commands() != 0 || !result.Outcome.Value.ContextReleased() {
				t.Error("rejection occurred after native dispatch or leaked resources")
			}
			delivery, err := f.inbox.Next(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			if err := delivery.Release(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, method := range []string{"Emulation.updateScreen", "Emulation.removeScreen", "Emulation.setPrimaryScreen", "Network.loadNetworkResource", "Network.takeResponseBodyForInterceptionAsStream"} {
		if permitted(method) {
			t.Error("unmanaged native resource path remains permitted", method)
		}
	}
}
func TestBrowserNativeTabCleanupFailureIsNotErased(t *testing.T) {
	f := bindFixture(t, browserOptions(t), 1)
	receipt, err := f.client.Run(deadline(t), deadline(t), fault.Correlation{Call: "tab-close"}, func(session *Session) error {
		return target.CloseTarget(sdk.FromContext(session.native).Target.TargetID).Do(browserExecutor(t, f))
	})
	var native *cdproto.Error
	if !errors.Is(err, ErrCleanup) || !errors.As(err, &native) {
		t.Error("native tab cleanup failure was discarded", err)
	}
	result, waitErr := receipt.WaitReleased(deadline(t))
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	if result.Outcome.Primary != nil || result.Outcome.Cleanup == nil || !result.Outcome.Value.ContextReleased() {
		t.Error("primary, cleanup and disposal evidence were conflated")
	}
	delivery, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
}
func TestBrowserConnectionLossCancelsCooperativeSession(t *testing.T) {
	host := bindFixture(t, browserOptions(t), 1)
	if err := host.client.owner.start(deadline(t)); err != nil {
		t.Fatal(err)
	}
	address, err := host.client.owner.debugURL(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	f := bindFixture(t, OptionsV1{Name: "borrowed", RemoteURL: address, NewWindow: true, CleanupTimeout: 100 * time.Millisecond}, 1)
	ctx, cancel := context.WithCancel(deadline(t))
	defer cancel()
	entered, canceled, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		_, _ = f.client.Run(ctx, deadline(t), fault.Correlation{Call: "connection-loss"}, func(session *Session) error {
			close(entered)
			<-session.Context().Done()
			close(canceled)
			return session.Context().Err()
		})
		close(done)
	}()
	defer func() {
		cancel()
		<-done
		delivery, err := f.inbox.Next(deadline(t))
		if err != nil {
			t.Error(err)
		} else {
			_ = delivery.Release()
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("session did not start")
	}
	f.client.owner.closeAllocator()
	select {
	case <-canceled:
	case <-time.After(300 * time.Millisecond):
		t.Error("lost CDP connection did not cancel session while resource lifetime stayed live")
	}
	if f.client.owner.life.Err() != nil {
		t.Error("test accidentally canceled resource lifetime")
	}
}
