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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/bogdanfinn/tls-client"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestOwnerLifetimeCancelsSiblingBindings(t *testing.T) {
	for _, blockedFirst := range []bool{false, true} {
		name := "cooperative-control"
		if blockedFirst {
			name = "blocked-first-sibling"
		}
		t.Run(name, func(t *testing.T) {
			peers := make([]*httptest.Server, 2)
			accepted := make([]chan net.Conn, 2)
			roots := x509.NewCertPool()
			for index := range peers {
				accepted[index] = make(chan net.Conn, 1)
				peer := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					_, _ = io.WriteString(writer, request.Proto)
				}))
				peer.Config.ConnState = func(conn net.Conn, state http.ConnState) {
					if state == http.StateNew {
						accepted[index] <- conn
					}
				}
				peer.EnableHTTP2 = true
				peer.StartTLS()
				t.Cleanup(peer.Close)
				peers[index] = peer
				roots.AddCert(peer.Certificate())
			}
			options := providerOptions()
			options.Mode = Negotiated
			options.DisableSessionTickets = true
			options.Native.Transport = &sdk.TransportOptions{RootCAs: roots}
			var dials [2]atomic.Int64
			var canceledCount atomic.Int64
			entered, canceled := make(chan int, 2), make(chan int, 2)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				for index, peer := range peers {
					if address != peer.Listener.Addr().String() {
						continue
					}
					if dials[index].Add(1) == 2 {
						entered <- index
						<-ctx.Done()
						first := canceledCount.Add(1) == 1
						canceled <- index
						if first && blockedFirst {
							<-release
						}
						return nil, ctx.Err()
					}
				}
				return (&net.Dialer{}).DialContext(ctx, network, address)
			}
			lifetime, cancelOwner := context.WithCancel(context.Background())
			defer cancelOwner()
			fixture := bindOwnerLifetime(t, options, 2, lifetime)
			for index, peer := range peers {
				receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("initial"), providerRequest(t, "GET", peer.URL, nil))
				if err != nil {
					t.Fatal(err)
				}
				if result := settleProvider(t, fixture, receipt); result.Outcome.Value.Metadata().Protocol() != "HTTP/2.0" {
					t.Fatal("normal control did not negotiate H2")
				}
				select {
				case conn := <-accepted[index]:
					_ = conn.Close()
				case <-testContext(t).Done():
					t.Fatal("missing independent peer connection")
				}
			}
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
					t.Fatal("native pools retained closed peer connections")
				}
			}
			type completion struct {
				receipt *invocation.Receipt[Result]
				err     error
			}
			completions := make(chan completion, 2)
			method, cancelMethod := context.WithCancel(testContext(t))
			defer cancelMethod()
			for index, peer := range peers {
				request := providerRequest(t, "GET", peer.URL, nil)
				go func() {
					receipt, err := fixture.client.Do(method, context.Background(), providerID("reconnect-"+strconv.Itoa(index)), request)
					completions <- completion{receipt: receipt, err: err}
				}()
			}
			for range peers {
				select {
				case <-entered:
				case <-testContext(t).Done():
					t.Fatal("both contextless reconnects did not enter")
				}
			}
			cancelMethod()
			var receipts []*invocation.Receipt[Result]
			for range peers {
				select {
				case completed := <-completions:
					if completed.receipt == nil || !errors.Is(completed.err, context.Canceled) {
						t.Fatal("method cancellation lost admitted receipt", completed.err)
					}
					receipts = append(receipts, completed.receipt)
				case <-testContext(t).Done():
					t.Fatal("canceled method waiter did not return")
				}
			}
			cancelOwner()
			progress, stop := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer stop()
			progressComplete := true
		progressLoop:
			for count := range peers {
				select {
				case index := <-canceled:
					t.Logf("native binding %d observed owner cancellation (%d/2)", index, count+1)
				case <-progress.Done():
					t.Errorf("one blocked native binding prevented independent cancellation progress; observed %d/2", count)
					progressComplete = false
					break progressLoop
				}
			}
			if blockedFirst && progressComplete {
				short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
				err := fixture.assembly.Close(short)
				stop()
				if !errors.Is(err, resource.ErrIncomplete) {
					t.Fatal("blocked callback lost source ownership", err)
				}
			}
			unblock()
			expected := make(map[string]invocation.Result[Result])
			for _, receipt := range receipts {
				result, err := receipt.WaitReleased(testContext(t))
				if err != nil {
					t.Fatal(err)
				}
				if !result.Final || !result.Released || result.Outcome.Value.Complete() || !errors.Is(result.Err(), context.Canceled) {
					t.Fatal("canceled reconnect falsely completed", result.Err())
				}
				expected[result.Context.Correlation.Call] = result
			}
			for range receipts {
				delivery, err := fixture.inbox.Next(testContext(t))
				if err != nil {
					t.Fatal(err)
				}
				independent, err := delivery.Receipt().WaitReleased(testContext(t))
				original, found := expected[independent.Context.Correlation.Call]
				if err != nil || !found || original.Context != independent.Context || !independent.Final || !independent.Released ||
					original.Outcome.Primary != independent.Outcome.Primary || original.Outcome.Cleanup != independent.Outcome.Cleanup {
					t.Fatal("independent concurrent evidence differs", err)
				}
				delete(expected, independent.Context.Correlation.Call)
				if err := delivery.Release(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
