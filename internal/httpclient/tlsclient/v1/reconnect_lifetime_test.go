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

package tlsclient

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	tls "github.com/bogdanfinn/utls"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type reconnectMarker struct{}

func TestOwnerLifetimeReconnectCancellation(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1Only, Negotiated} {
		for _, block := range []string{"none", "dial", "profile", "dial-context"} {
			t.Run(string(mode)+"/"+block, func(t *testing.T) {
				var requests atomic.Int64
				accepted := make(chan net.Conn, 4)
				peer := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					requests.Add(1)
					_, _ = io.WriteString(writer, request.Proto)
				}))
				peer.Config.ConnState = func(conn net.Conn, state http.ConnState) {
					if state == http.StateNew {
						accepted <- conn
					}
				}
				peer.EnableHTTP2 = mode == Negotiated
				peer.StartTLS()
				t.Cleanup(peer.Close)
				roots := x509.NewCertPool()
				roots.AddCert(peer.Certificate())
				options := providerOptions()
				options.Mode, options.DisableSessionTickets = mode, true
				options.Native.Transport = &sdk.TransportOptions{RootCAs: roots}
				entered, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				cause := errors.New("synthetic blocked reconnect refusal")
				var dials, factories atomic.Int64
				var reconnectValue, reconnectCanceled atomic.Bool
				var callbackReturned atomic.Bool
				options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
					call := dials.Add(1)
					if call == 2 {
						reconnectValue.Store(ctx.Value(reconnectMarker{}) == "method")
						if block == "dial" || block == "dial-context" {
							defer callbackReturned.Store(true)
							close(entered)
							if block == "dial-context" {
								select {
								case <-ctx.Done():
								case <-release:
								}
							} else {
								<-release
							}
							reconnectCanceled.Store(ctx.Err() != nil)
							return nil, cause
						}
					}
					return (&net.Dialer{}).DialContext(ctx, network, address)
				}
				id := tls.HelloChrome_120
				id.SpecFactory = func() (tls.ClientHelloSpec, error) {
					if factories.Add(1) == 2 && block == "profile" {
						defer callbackReturned.Store(true)
						close(entered)
						<-release
						return tls.ClientHelloSpec{}, cause
					}
					return profiles.Chrome_120.GetClientHelloSpec()
				}
				profile := profileWithID(*options.Native.Profile, id)
				options.Native.Profile = &profile
				lifetime, cancelOwner := context.WithCancelCause(context.Background())
				defer cancelOwner(nil)
				fixture := bindOwnerLifetime(t, options, 1, lifetime)
				if factories.Load() != 0 || dials.Load() != 0 {
					t.Fatal("source selection invoked a lazy native dependency")
				}
				first, err := fixture.client.Do(testContext(t), testContext(t), providerID("first"), providerRequest(t, "GET", peer.URL, nil))
				if err != nil {
					t.Fatal(err)
				}
				firstResult := settleProvider(t, fixture, first)
				wantProtocol := "HTTP/1.1"
				if mode == Negotiated {
					wantProtocol = "HTTP/2.0"
				}
				if !firstResult.Outcome.Value.Complete() || firstResult.Outcome.Value.Metadata().Protocol() != wantProtocol || dials.Load() != 1 || factories.Load() != 1 {
					t.Fatal("normal first-connection control did not establish selected protocol")
				}
				var firstConn net.Conn
				select {
				case firstConn = <-accepted:
				case <-testContext(t).Done():
					t.Fatal("peer did not record first physical connection")
				}
				_ = firstConn.Close()
				wait := testContext(t)
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					fixture.client.owner.mu.Lock()
					remaining := fixture.client.owner.tcp
					fixture.client.owner.mu.Unlock()
					if remaining == 0 {
						break
					}
					select {
					case <-tick.C:
					case <-wait.Done():
						t.Fatal("native pool did not observe peer-side socket closure")
					}
				}
				ctx, cancel := context.WithCancel(context.WithValue(testContext(t), reconnectMarker{}, "method"))
				defer cancel()
				if block == "none" {
					receipt, err := fixture.client.Do(ctx, testContext(t), providerID("normal-reconnect"), providerRequest(t, "GET", peer.URL, nil))
					if err != nil {
						t.Fatal(err)
					}
					result := settleProvider(t, fixture, receipt)
					if !result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().Protocol() != wantProtocol || requests.Load() != 2 || dials.Load() != 2 || factories.Load() != 2 {
						t.Fatal("normal reconnect control failed")
					}
					t.Logf("normal-reconnect method-context-value=%t", reconnectValue.Load())
					return
				}
				type completion struct {
					receipt *invocation.Receipt[Result]
					err     error
				}
				done := make(chan completion, 1)
				cleanup := testContext(t)
				go func() {
					receipt, err := fixture.client.Do(ctx, cleanup, providerID("blocked-reconnect"), providerRequest(t, "GET", peer.URL, nil))
					done <- completion{receipt, err}
				}()
				select {
				case <-entered:
				case <-testContext(t).Done():
					t.Fatal("reconnect dependency did not enter")
				}
				cancel()
				var completed completion
				select {
				case completed = <-done:
				case <-testContext(t).Done():
					t.Fatal("method waiter did not return on cancellation")
				}
				if completed.receipt == nil || !errors.Is(completed.err, context.Canceled) {
					t.Fatal("canceled reconnect lost receipt/cause", completed.err)
				}
				short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer stop()
				interim, waitErr := completed.receipt.WaitReleased(short)
				retained := errors.Is(waitErr, context.DeadlineExceeded)
				returnedBeforeUnblock := callbackReturned.Load()
				cancelOwner(errors.New("synthetic owner cancellation"))
				if block == "dial-context" {
					if _, err := completed.receipt.WaitReleased(testContext(t)); err != nil {
						t.Fatal("owner cancellation did not finish cooperative reconnect", err)
					}
					if !callbackReturned.Load() || !reconnectCanceled.Load() {
						t.Fatal("cooperative dial did not observe cancellation")
					}
				} else {
					stillWaiting, stopWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
					defer stopWait()
					if _, err := completed.receipt.WaitReleased(stillWaiting); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal("owner cancellation released uncooperative callback", err)
					}
					closeCtx, stopClose := context.WithTimeout(context.Background(), 20*time.Millisecond)
					closeErr := fixture.assembly.Close(closeCtx)
					stopClose()
					if !errors.Is(closeErr, resource.ErrIncomplete) {
						t.Fatal("pending callback lost assembly ownership", closeErr)
					}
				}
				unblock()
				result := settleProvider(t, fixture, completed.receipt)
				t.Logf("root-retained=%t interim-final=%t interim-released=%t reconnect-context-value=%t reconnect-context-canceled=%t", retained, interim.Final, interim.Released, reconnectValue.Load(), reconnectCanceled.Load())
				if !retained && !returnedBeforeUnblock {
					t.Error("root released before the admitted native reconnect dependency returned")
				}
				if result.Outcome.Value.Complete() || !errors.Is(result.Err(), context.Canceled) || requests.Load() != 1 {
					t.Fatal("canceled reconnect sent a request or falsely completed", result.Err())
				}
			})
		}
	}
}

func bindOwnerLifetime(t *testing.T, options OptionsV1, capacity int, lifetime context.Context) *providerFixture {
	t.Helper()
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := prepared.SelectWithLifetime(lifetime)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, prepared.Metadata().Limits)
	construction, stopConstruction := context.WithCancel(testContext(t))
	assembly, err := resource.Assemble(construction, testContext(t), "owner-lifetime", selected)
	stopConstruction()
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*prepared.Metadata().EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &providerFixture{client: client, selected: selected, assembly: assembly, inbox: inbox}
	t.Cleanup(func() {
		if err := assembly.Close(testContext(t)); err != nil {
			t.Error("source cleanup remains incomplete", err)
		}
	})
	return fixture
}
