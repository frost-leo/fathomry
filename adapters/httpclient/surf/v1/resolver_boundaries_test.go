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

package surf

import (
	"context"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
	"golang.org/x/net/dns/dnsmessage"
)

type reviewDNSQuota struct{ tcp, udp, work atomic.Int64 }

func (quota *reviewDNSQuota) control() sdk.FathomryControlV1 {
	return sdk.FathomryControlV1{
		AcquireTCP: func() (func(), error) { quota.tcp.Add(1); return func() { quota.tcp.Add(-1) }, nil },
		AcquireUDP: func() (func(), error) { quota.udp.Add(1); return func() { quota.udp.Add(-1) }, nil },
		Enter: func(context.Context) (func(), error) {
			quota.work.Add(1)
			return func() { quota.work.Add(-1) }, nil
		},
	}
}

func (quota *reviewDNSQuota) assertReleased(t *testing.T) {
	t.Helper()
	if quota.tcp.Load() != 0 || quota.udp.Load() != 0 || quota.work.Load() != 0 {
		t.Error("native resolver retained physical capacity or work after Close", quota.tcp.Load(), quota.udp.Load(), quota.work.Load())
	}
}

func reviewNativeResolverClient(t *testing.T, resolver *net.Resolver, quota *reviewDNSQuota) *sdk.Client {
	t.Helper()
	client := sdk.NewClient()
	if err := client.ConfigureFathomry(quota.control()); err != nil {
		t.Fatal(err)
	}
	if err := client.FathomrySetResolver(resolver); err != nil {
		t.Fatal(err)
	}
	if result := client.Builder().Proxy("").ForceHTTP1().Build(); result.IsErr() {
		t.Fatal(result.Err())
	}
	t.Cleanup(func() { _ = client.Close(); quota.assertReleased(t) })
	return client
}

func TestNativeResolverPacketFramingAndRecovery(t *testing.T) {
	var requests, queries, dials atomic.Int64
	origin := httptest.NewServer(stdhttp.HandlerFunc(func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
		requests.Add(1)
		_, _ = io.WriteString(writer, "resolved")
	}))
	t.Cleanup(origin.Close)
	dns, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		buffer := make([]byte, 4096)
		for {
			count, remote, err := dns.ReadFrom(buffer)
			if err != nil {
				return
			}
			var query dnsmessage.Message
			if err := query.Unpack(buffer[:count]); err != nil || len(query.Questions) != 1 {
				t.Error("UDP resolver framing acquired a TCP length prefix", err)
				return
			}
			question := query.Questions[0]
			queries.Add(1)
			answer := dnsmessage.Message{Header: dnsmessage.Header{ID: query.ID, Response: true, Authoritative: true, RecursionDesired: true, RecursionAvailable: true}, Questions: query.Questions}
			if question.Type == dnsmessage.TypeA {
				answer.Answers = []dnsmessage.Resource{{Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}, Body: &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}}}}
			}
			wire, err := answer.Pack()
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := dns.WriteTo(wire, remote); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	t.Cleanup(func() { _ = dns.Close(); <-stopped })
	var refuse atomic.Bool
	refuse.Store(true)
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		dials.Add(1)
		if refuse.Load() {
			return nil, errors.New("temporary fixture DNS refusal")
		}
		return (&net.Dialer{}).DialContext(ctx, "udp4", dns.LocalAddr().String())
	}}
	quota := &reviewDNSQuota{}
	client := reviewNativeResolverClient(t, resolver, quota)
	endpoint, _ := url.Parse(origin.URL)
	endpoint.Host = net.JoinHostPort("review-resolver.invalid.", endpoint.Port())
	input, _ := http.NewRequestWithContext(testContext(t), "GET", endpoint.String(), nil)
	refused := client.FathomryRequest(input).Do()
	var dnsError *net.DNSError
	if !refused.IsErr() || !errors.As(refused.Err(), &dnsError) || dials.Load() == 0 || queries.Load() != 0 || requests.Load() != 0 {
		t.Fatal("normal DNS refusal escaped native routing", refused.Err())
	}
	refuse.Store(false)
	input, _ = http.NewRequestWithContext(testContext(t), "GET", endpoint.String(), nil)
	result := client.FathomryRequest(input).Do()
	if result.IsErr() {
		t.Fatal("ordinary DNS refusal poisoned route or UDP framing changed", result.Err())
	}
	response := result.Ok()
	data, readErr := io.ReadAll(response.Body.Reader)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || string(data) != "resolved" || queries.Load() == 0 || requests.Load() != 1 {
		t.Fatal("DNS packet positive control failed", readErr, closeErr)
	}
	if err := client.Close(); err != nil || !client.FathomryQuiescent() {
		t.Fatal("native resolver recovery source did not close", err)
	}
	quota.assertReleased(t)
}

type reviewPartialDNSConn struct {
	net.Conn
	closed *atomic.Int64
	cause  error
}

func (connection *reviewPartialDNSConn) Close() error {
	connection.closed.Add(1)
	return errors.Join(connection.Conn.Close(), connection.cause)
}

func TestNativeResolverPartialConnectionCleanup(t *testing.T) {
	for _, selection := range []string{"connection-and-error", "connection-and-cleanup-error", "typed-nil", "nil-success"} {
		t.Run(selection, func(t *testing.T) {
			var acquired, closed atomic.Int64
			cause := errors.New("fixture DNS constructor failure")
			cleanupCause := errors.New("fixture DNS Close failure")
			resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if selection == "typed-nil" {
					var connection *reviewPartialDNSConn
					return connection, nil
				}
				if selection == "nil-success" {
					return nil, nil
				}
				client, peer := net.Pipe()
				_ = peer.Close()
				acquired.Add(1)
				connection := &reviewPartialDNSConn{Conn: client, closed: &closed}
				if selection == "connection-and-cleanup-error" {
					connection.cause = cleanupCause
				}
				return connection, cause
			}}
			quota := &reviewDNSQuota{}
			client := reviewNativeResolverClient(t, resolver, quota)
			input, _ := http.NewRequestWithContext(testContext(t), "GET", "http://review-resolver.invalid.:443/", nil)
			result := client.FathomryRequest(input).Do()
			if !result.IsErr() {
				t.Fatal("partial/nil DNS constructor dispatched request")
			}
			closeErr := client.Close()
			if acquired.Load() != closed.Load() || !client.FathomryQuiescent() {
				t.Fatal("partially acquired DNS connection escaped Close", acquired.Load(), closed.Load())
			}
			if selection == "connection-and-cleanup-error" {
				if !errors.Is(closeErr, cleanupCause) || acquired.Load() == 0 {
					t.Fatal("partial DNS Close cause disappeared", closeErr)
				}
			} else if closeErr != nil {
				t.Fatal("ordinary/nil DNS refusal poisoned source cleanup", closeErr)
			}
			quota.assertReleased(t)
		})
	}
}

func TestNativeResolverCanceledCreatorKeepsHealthySharedLookup(t *testing.T) {
	answerGate := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(answerGate) })
	defer unblock()
	base, _, _ := reviewPublicResolver(t, "success", answerGate)
	entered := make(chan struct{})
	var once sync.Once
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		once.Do(func() { close(entered) })
		return base.Dial(ctx, network, address)
	}}
	quota := &reviewDNSQuota{}
	client := reviewNativeResolverClient(t, resolver, quota)
	selected := client.GetDialer().Resolver
	firstCtx, cancelFirst := context.WithCancel(testContext(t))
	defer cancelFirst()
	first := make(chan error, 1)
	go func() { _, err := selected.LookupIPAddr(firstCtx, "review-resolver.invalid."); first <- err }()
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("first DNS lookup did not start")
	}
	secondStarted := make(chan struct{})
	var coalesced atomic.Bool
	healthy := httptrace.WithClientTrace(testContext(t), &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { close(secondStarted) },
		DNSDone:  func(info httptrace.DNSDoneInfo) { coalesced.Store(info.Coalesced) },
	})
	type lookupResult struct {
		addresses []net.IPAddr
		err       error
	}
	second := make(chan lookupResult, 1)
	go func() {
		addresses, err := selected.LookupIPAddr(healthy, "review-resolver.invalid.")
		second <- lookupResult{addresses: addresses, err: err}
	}()
	<-secondStarted
	time.Sleep(20 * time.Millisecond)
	cancelFirst()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("creator cancellation was not independent", err)
		}
	case <-testContext(t).Done():
		t.Fatal("canceled DNS creator did not return")
	}
	unblock()
	select {
	case result := <-second:
		if result.err != nil || len(result.addresses) != 1 || !result.addresses[0].IP.Equal(net.IPv4(127, 0, 0, 1)) {
			t.Fatal("canceled creator poisoned healthy shared lookup", result.err, result.addresses)
		}
		if !coalesced.Load() {
			t.Fatal("fixture did not establish actual native DNS singleflight sharing")
		}
	case <-testContext(t).Done():
		t.Fatal("healthy DNS sibling did not finish")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	quota.assertReleased(t)
}
