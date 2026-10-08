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

package surf

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/enetx/http"
	nativeh3 "github.com/enetx/http3"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

func reviewRouteReleased(t *testing.T, fixture *fixture) {
	t.Helper()
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal("source release", err)
	}
	owner := fixture.client.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.tcp != 0 || owner.udp != 0 || owner.h3 != 0 {
		t.Fatalf("native reservations survived release: tcp=%d udp=%d h3=%d", owner.tcp, owner.udp, owner.h3)
	}
}

func TestConfiguredProxyAndRoutingLock(t *testing.T) {
	for _, locked := range []bool{false, true} {
		t.Run(fmt.Sprintf("locked-%v", locked), func(t *testing.T) {
			var calls, dials atomic.Int32
			peer := newPeers(t, func(writer stdhttp.ResponseWriter, input *stdhttp.Request) {
				calls.Add(1)
				if input.Header.Get("X-Route") != "" || input.Header.Get("Proxy-Authorization") != "" {
					t.Error("CONNECT metadata leaked to origin")
				}
				_, _ = io.WriteString(writer, "origin")
			}, false)
			proxy := newProxy(t, false, "")
			other := newProxy(t, false, "")
			configured, direct := proxy.server.URL, ""
			options := OptionsV1{Name: "configured-route", Mode: HTTP1Only, ProxyURL: configured, RoutingLocked: locked,
				Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots}, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					dials.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, address)
				}}}
			fixture := newFixture(t, options, 1)
			for index, override := range []*string{nil, &configured} {
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: fmt.Sprint(index)}, request(t, "GET", peer.tcp.URL, ""),
					RequestOptionsV1{ProxyURL: override, ConnectHeaders: http.Header{"X-Route": {"frozen"}}})
				result := settle(t, fixture, receipt)
				if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.ProxyMode() != "http" {
					t.Fatal("inherited/same configured proxy failed", err, result.Err())
				}
			}
			if proxy.connects.Load() != 1 || calls.Load() != 2 || dials.Load() != 1 {
				t.Fatal("inherited and explicit-same proxy did not reuse their native route")
			}
			for _, override := range []*string{&direct, &other.server.URL} {
				runtime := RequestOptionsV1{ProxyURL: override}
				if *override != "" {
					runtime.ConnectHeaders = http.Header{"X-Route": {"frozen"}}
				}
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "override"}, request(t, "GET", peer.tcp.URL, ""), runtime)
				if locked {
					if receipt != nil || !errors.Is(err, ErrInput) || calls.Load() != 2 || dials.Load() != 1 || other.connects.Load() != 0 {
						t.Fatal("locked routing changed or reached native effects", err)
					}
					continue
				}
				result := settle(t, fixture, receipt)
				wantMode := "http"
				if *override == "" {
					wantMode = "direct"
				}
				if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.ProxyMode() != wantMode {
					t.Fatal("unlocked explicit route override failed", err, result.Err())
				}
			}
			if !locked && (calls.Load() != 4 || proxy.connects.Load() != 1 || other.connects.Load() != 1 || dials.Load() != 3) {
				t.Fatal("configured, direct and per-call proxy effects differ")
			}
			t.Logf("locked=%v origin=%d configured CONNECT=%d alternative CONNECT=%d owned dials=%d", locked, calls.Load(), proxy.connects.Load(), other.connects.Load(), dials.Load())
			reviewRouteReleased(t, fixture)
		})
	}
}

func TestRouteCapacityKeepsExistingRoute(t *testing.T) {
	for _, maximum := range []int{1, 2} {
		t.Run(fmt.Sprintf("routes-%d", maximum), func(t *testing.T) {
			var calls atomic.Int32
			peer := newPeers(t, func(writer stdhttp.ResponseWriter, _ *stdhttp.Request) {
				calls.Add(1)
				_, _ = io.WriteString(writer, "route")
			}, false)
			proxy := newProxy(t, false, "")
			fixture := newFixture(t, OptionsV1{Name: "route-capacity", Mode: HTTP1Only, MaxRoutes: maximum,
				Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots}}}, 1)
			for _, call := range []string{"first", "other", "existing"} {
				var runtime []RequestOptionsV1
				if call == "other" {
					runtime = []RequestOptionsV1{{ProxyURL: &proxy.server.URL, ConnectHeaders: http.Header{"X-Route": {"frozen"}}}}
				}
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: call}, request(t, "GET", peer.tcp.URL, ""), runtime...)
				result := settle(t, fixture, receipt)
				if maximum == 1 && call == "other" {
					if !errors.Is(err, ErrLimit) || !errors.Is(err, resource.ErrCapacity) || !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.RoundTrips() != 0 || result.Outcome.Value.Complete() || proxy.connects.Load() != 0 || calls.Load() != 1 {
						t.Fatal("route saturation did not refuse before dispatch", err, result.Err())
					}
				} else if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() {
					t.Fatal("permitted route no longer usable", err, result.Err())
				}
			}
			fixture.client.owner.mu.Lock()
			resident := len(fixture.client.owner.routes)
			fixture.client.owner.mu.Unlock()
			if resident != maximum || calls.Load() != int32(maximum+1) {
				t.Fatalf("routes=%d origin=%d", resident, calls.Load())
			}
			t.Logf("route quota=%d resident=%d origin=%d CONNECT=%d", maximum, resident, calls.Load(), proxy.connects.Load())
			reviewRouteReleased(t, fixture)
		})
	}
}

func TestTCPCapacityHeldStreamAndRecovery(t *testing.T) {
	for _, maximum := range []int{1, 2} {
		t.Run(fmt.Sprintf("tcp-%d", maximum), func(t *testing.T) {
			var secondCalls, dials atomic.Int32
			first := httptest.NewServer(stdhttp.HandlerFunc(func(writer stdhttp.ResponseWriter, input *stdhttp.Request) {
				writer.Header().Set("Content-Length", "100")
				_, _ = io.WriteString(writer, "held")
				writer.(stdhttp.Flusher).Flush()
				<-input.Context().Done()
			}))
			t.Cleanup(first.Close)
			second := httptest.NewServer(stdhttp.HandlerFunc(func(writer stdhttp.ResponseWriter, _ *stdhttp.Request) {
				secondCalls.Add(1)
				_, _ = io.WriteString(writer, "second")
			}))
			t.Cleanup(second.Close)
			fixture := newFixture(t, OptionsV1{Name: "tcp-capacity", Mode: HTTP1Only, MaxActive: 2, MaxTCPConnections: maximum,
				Native: NativeOptionsV1{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					dials.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, address)
				}}}, 2)
			held, heldReceipt, err := fixture.client.Open(testContext(t), fault.Correlation{Call: "held"}, request(t, "GET", first.URL, ""))
			if err != nil || held == nil {
				t.Fatal("retained first socket unavailable", err)
			}
			t.Cleanup(func() { _ = held.Close(testContext(t)) })
			heldDelivery, err := fixture.inbox.Next(testContext(t))
			if err != nil {
				t.Fatal("retained stream independent receipt", err)
			}
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "contender"}, request(t, "GET", second.URL, ""))
			result := settle(t, fixture, receipt)
			if maximum == 1 {
				if !errors.Is(err, ErrLimit) || !errors.Is(err, resource.ErrCapacity) || !errors.Is(result.Err(), ErrLimit) || dials.Load() != 1 || secondCalls.Load() != 0 || result.Outcome.Value.Complete() {
					t.Fatal("TCP ceiling allowed a second owned dial", err, result.Err())
				}
			} else if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || dials.Load() != 2 || secondCalls.Load() != 1 {
				t.Fatal("available second TCP slot unusable", err, result.Err())
			}
			buffer := make([]byte, 4)
			if _, err := io.ReadFull(held, buffer); err != nil || string(buffer) != "held" {
				t.Fatal("contender damaged first stream", err)
			}
			if err := held.Close(testContext(t)); err != nil {
				t.Fatal("held stream release", err)
			}
			heldResult, err := heldReceipt.WaitReleased(testContext(t))
			if err != nil || !heldResult.Released {
				t.Fatal("held stream direct release evidence", err)
			}
			heldEvidence, err := heldDelivery.Receipt().WaitReleased(testContext(t))
			if err != nil || !heldEvidence.Released || heldEvidence.Context != heldResult.Context {
				t.Fatal("held stream independent release evidence", err)
			}
			if err := heldDelivery.Release(); err != nil {
				t.Fatal(err)
			}
			if maximum == 1 {
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "recovered"}, request(t, "GET", second.URL, ""))
				result := settle(t, fixture, receipt)
				if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || dials.Load() != 2 || secondCalls.Load() != 1 {
					t.Fatal("closed socket slot did not recover", err, result.Err())
				}
			}
			t.Logf("TCP quota=%d owned dials=%d second-origin=%d", maximum, dials.Load(), secondCalls.Load())
			reviewRouteReleased(t, fixture)
		})
	}
}

func TestUDPCapacityDirectAndSOCKS(t *testing.T) {
	for _, maximum := range []int{1, 2} {
		t.Run(fmt.Sprintf("udp-%d", maximum), func(t *testing.T) {
			var calls, listens atomic.Int32
			peer := newPeers(t, func(writer stdhttp.ResponseWriter, input *stdhttp.Request) {
				calls.Add(1)
				if input.ProtoMajor != 3 {
					t.Error("capacity control silently fell back to TCP")
				}
				_, _ = io.WriteString(writer, "h3")
			}, true)
			proxy := newSOCKS(t, peer.tcp.Listener.Addr().String())
			route := "socks5://owner:synthetic@" + proxy.address
			fixture := newFixture(t, OptionsV1{Name: "udp-capacity", Mode: PreferHTTP3, MaxRoutes: 2, MaxUDPSockets: maximum,
				Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots}, ListenPacket: func(ctx context.Context, network, address string) (net.PacketConn, error) {
					listens.Add(1)
					return (&net.ListenConfig{}).ListenPacket(ctx, network, address)
				}}}, 1)
			for _, call := range []string{"direct", "socks", "existing"} {
				var runtime []RequestOptionsV1
				if call == "socks" {
					runtime = []RequestOptionsV1{{ProxyURL: &route}}
				}
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: call}, request(t, "GET", peer.tcp.URL, ""), runtime...)
				result := settle(t, fixture, receipt)
				if maximum == 1 && call == "socks" {
					if !errors.Is(err, ErrLimit) || !errors.Is(err, resource.ErrCapacity) || !errors.Is(result.Err(), ErrLimit) || listens.Load() != 1 || calls.Load() != 1 || result.Outcome.Value.Complete() {
						t.Fatal("UDP ceiling allowed another native socket", err, result.Err())
					}
				} else if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().Protocol() != "HTTP/3.0" {
					t.Fatal("permitted H3 socket/route unusable", err, result.Err())
				}
			}
			if listens.Load() != int32(maximum) || calls.Load() != int32(maximum+1) || proxy.tcp.Load() != 0 {
				t.Fatal("UDP path/count changed")
			}
			t.Logf("UDP quota=%d listeners=%d H3 origin=%d SOCKS associations=%d", maximum, listens.Load(), calls.Load(), proxy.udp.Load())
			reviewRouteReleased(t, fixture)
			reviewH3Eventually(t, "SOCKS control survived source close", func() bool { return proxy.controls.Load() == 0 })
		})
	}
}

func TestRoundTripLimitAcrossRedirectRetry(t *testing.T) {
	for _, maximum := range []int{2, 3, 4} {
		t.Run(fmt.Sprintf("round-trips-%d", maximum), func(t *testing.T) {
			var calls, finals atomic.Int32
			peer := httptest.NewServer(stdhttp.HandlerFunc(func(writer stdhttp.ResponseWriter, input *stdhttp.Request) {
				calls.Add(1)
				if input.URL.Path == "/start" {
					writer.Header().Set("Location", "/retry")
					writer.WriteHeader(307)
					return
				}
				if finals.Add(1) == 1 {
					writer.WriteHeader(503)
					return
				}
				_, _ = io.WriteString(writer, "complete")
			}))
			t.Cleanup(peer.Close)
			fixture := newFixture(t, OptionsV1{Name: "roundtrip-limit", Mode: HTTP1Only, MaxRoundTrips: maximum, NativeRetries: 1, RetryCodes: []int{503}}, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "redirect-retry"}, request(t, "GET", peer.URL+"/start", ""))
			result := settle(t, fixture, receipt)
			if maximum < 4 {
				if !errors.Is(err, ErrLimit) || !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Complete() {
					t.Fatal("round-trip limit lost across native retries", err, result.Err())
				}
			} else if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "complete" {
				t.Fatal("exact permitted round-trip count refused", err, result.Err())
			}
			if calls.Load() != int32(maximum) || result.Outcome.Value.RoundTrips() != maximum || result.Attempts.Observed != uint64(maximum) || result.Attempts.Exact {
				t.Fatalf("count evidence differs: wire=%d entries=%d observed=%d exact=%v", calls.Load(), result.Outcome.Value.RoundTrips(), result.Attempts.Observed, result.Attempts.Exact)
			}
			t.Logf("round-trip quota=%d wire=%d observed=%d final-complete=%v", maximum, calls.Load(), result.Attempts.Observed, result.Outcome.Value.Complete())
			reviewRouteReleased(t, fixture)
		})
	}
}

type reviewRouteInput struct {
	*strings.Reader
	closed atomic.Int32
}

func (input *reviewRouteInput) Close() error { input.closed.Add(1); return nil }

func TestReplayLimitOrdinaryAndMultipart(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		for _, maximum := range []int{1, 2} {
			t.Run(fmt.Sprintf("multipart-%v-replays-%d", multipart, maximum), func(t *testing.T) {
				var calls, factories atomic.Int32
				var inputsMu sync.Mutex
				var inputs []*reviewRouteInput
				fresh := func() (io.ReadCloser, error) {
					factories.Add(1)
					input := &reviewRouteInput{Reader: strings.NewReader("payload")}
					inputsMu.Lock()
					inputs = append(inputs, input)
					inputsMu.Unlock()
					return input, nil
				}
				peer := httptest.NewServer(stdhttp.HandlerFunc(func(writer stdhttp.ResponseWriter, input *stdhttp.Request) {
					data, err := io.ReadAll(input.Body)
					if err != nil || !strings.Contains(string(data), "payload") {
						t.Error("body did not reach peer", err)
					}
					count := calls.Add(1)
					if count < 3 {
						if multipart {
							writer.WriteHeader(503)
						} else {
							writer.Header().Set("Location", fmt.Sprintf("/%d", count))
							writer.WriteHeader(307)
						}
						return
					}
					_, _ = io.WriteString(writer, "complete")
				}))
				defer peer.Close()
				options := OptionsV1{Name: "replay-limit", Mode: HTTP1Only, MaxReplays: maximum, MaxRoundTrips: 4}
				input := request(t, "POST", peer.URL, "payload")
				var runtime []RequestOptionsV1
				if multipart {
					options.NativeRetries = 2
					options.RetryCodes = []int{503}
					input = multipartRequest(t, peer.URL)
					runtime = []RequestOptionsV1{{Multipart: &Multipart{Parts: []Part{{Name: "file", FileName: "body.txt", Open: func(context.Context) (io.ReadCloser, error) { return fresh() }}}}}}
				} else {
					input.GetBody = fresh
				}
				fixture := newFixture(t, options, 1)
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "replay"}, input, runtime...)
				result := settle(t, fixture, receipt)
				if maximum == 1 {
					if !errors.Is(err, ErrLimit) || !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Complete() {
						t.Fatal("replay ceiling lost", err, result.Err())
					}
				} else if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "complete" {
					t.Fatal("exact allowed replays refused", err, result.Err())
				}
				wantFactories := maximum
				if multipart {
					wantFactories++
				}
				if calls.Load() != int32(maximum+1) || factories.Load() != int32(wantFactories) || result.Outcome.Value.RoundTrips() != maximum+1 {
					t.Fatalf("replay ceiling effects differ: calls=%d factories=%d round-trips=%d", calls.Load(), factories.Load(), result.Outcome.Value.RoundTrips())
				}
				inputsMu.Lock()
				for _, input := range inputs {
					if input.closed.Load() != 1 {
						t.Errorf("replay input close count=%d", input.closed.Load())
					}
				}
				inputsMu.Unlock()
				t.Logf("multipart=%v replay quota=%d wire=%d factories=%d complete=%v", multipart, maximum, calls.Load(), factories.Load(), result.Outcome.Value.Complete())
				reviewRouteReleased(t, fixture)
			})
		}
	}
}

func reviewRouteFallbackPeer(t *testing.T, code quic.ApplicationErrorCode, advertiseHTTP2 bool, handler stdhttp.HandlerFunc) (*peers, *atomic.Int32) {
	t.Helper()
	protocols := []string{"http/1.1"}
	if advertiseHTTP2 {
		protocols = []string{"h2", "http/1.1"}
	}
	peer := newPeers(t, handler, false, &tls.Config{NextProtos: protocols})
	packet, err := net.ListenPacket("udp", peer.tcp.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.Listen(packet, &tls.Config{Certificates: peer.tcp.TLS.Certificates, NextProtos: []string{"h3"}}, &quic.Config{})
	if err != nil {
		_ = packet.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(testContext(t))
	var requests atomic.Int32
	var group sync.WaitGroup
	group.Go(func() {
		for {
			connection, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			group.Go(func() {
				defer connection.CloseWithError(0, "")
				stop := context.AfterFunc(ctx, func() { _ = connection.CloseWithError(0, "") })
				defer stop()
				control, err := connection.OpenUniStreamSync(ctx)
				if err != nil {
					t.Error("H3 fallback control stream", err)
					return
				}
				if _, err := control.Write([]byte{0, 4, 0}); err != nil {
					t.Error("H3 fallback SETTINGS", err)
					return
				}
				stream, err := connection.AcceptStream(ctx)
				if err != nil {
					t.Error("H3 fallback request", err)
					return
				}
				reader := quicvarint.NewReader(stream)
				kind, err := quicvarint.Read(reader)
				if err != nil || kind != 1 {
					t.Errorf("H3 fallback HEADERS type=%d err=%v", kind, err)
					return
				}
				length, err := quicvarint.Read(reader)
				if err != nil || length > 64<<10 {
					t.Errorf("H3 fallback HEADERS length=%d err=%v", length, err)
					return
				}
				if _, err := io.CopyN(io.Discard, reader, int64(length)); err != nil {
					t.Error("H3 fallback HEADERS bytes", err)
					return
				}
				var body strings.Builder
				for !strings.Contains(body.String(), "payload") {
					kind, err := quicvarint.Read(reader)
					if err != nil || kind != 0 {
						t.Error("H3 fallback DATA missing", err)
						return
					}
					length, err := quicvarint.Read(reader)
					if err != nil || length > uint64(4096-body.Len()) {
						t.Error("H3 fallback DATA exceeds fixture bound", err)
						return
					}
					if _, err := io.CopyN(&body, reader, int64(length)); err != nil {
						t.Error("H3 fallback DATA bytes", err)
						return
					}
				}
				requests.Add(1)
				_ = connection.CloseWithError(code, "synthetic protocol control")
			})
		}
	})
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		_ = packet.Close()
		done := make(chan struct{})
		go func() { group.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("fallback peer did not stop")
		}
	})
	return peer, &requests
}

func reviewRouteErrorChain(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	t.Logf("synthetic local-peer error node: %T: %s", err, err.Error())
	if combined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range combined.Unwrap() {
			reviewRouteErrorChain(t, child)
		}
	} else if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		reviewRouteErrorChain(t, wrapped.Unwrap())
	}
}

func TestActualH3StandardTLSFallback(t *testing.T) {
	for _, control := range []struct {
		name                                 string
		code                                 quic.ApplicationErrorCode
		replayable, fallback, advertiseHTTP2 bool
	}{
		{name: "one-shot-refused", code: 0x110},
		{name: "replayable-fallback", code: 0x110, replayable: true, fallback: true},
		{name: "replayable-fallback-h2-advertised", code: 0x110, replayable: true, fallback: true, advertiseHTTP2: true},
		{name: "request-cancelled-refused", code: 0x10c, replayable: true},
	} {
		t.Run(control.name, func(t *testing.T) {
			var tlsCalls, factoryCalls atomic.Int32
			peer, h3Calls := reviewRouteFallbackPeer(t, control.code, control.advertiseHTTP2, func(writer stdhttp.ResponseWriter, input *stdhttp.Request) {
				tlsCalls.Add(1)
				data, err := io.ReadAll(input.Body)
				if err != nil || !strings.Contains(string(data), "payload") || input.ProtoMajor != 1 || input.TLS == nil {
					t.Error("standard TLS fallback changed protocol/body", err)
				}
				_, _ = io.WriteString(writer, "fallback")
			})
			var inputsMu sync.Mutex
			var inputs []*reviewRouteInput
			fresh := func() *reviewRouteInput {
				input := &reviewRouteInput{Reader: strings.NewReader("payload")}
				inputsMu.Lock()
				inputs = append(inputs, input)
				inputsMu.Unlock()
				return input
			}
			part := Part{Name: "file", FileName: "data.txt"}
			if control.replayable {
				part.Open = func(context.Context) (io.ReadCloser, error) {
					factoryCalls.Add(1)
					return fresh(), nil
				}
			} else {
				part.Input = fresh()
			}
			fixture := newFixture(t, OptionsV1{Name: "actual-fallback", Mode: PreferHTTP3, MaxRoutes: 1, MaxUDPSockets: 1, MaxTCPConnections: 1, MaxHTTP3Clients: 1,
				Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots}}}, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "fallback"}, multipartRequest(t, peer.tcp.URL),
				RequestOptionsV1{Multipart: &Multipart{Parts: []Part{part}}})
			result := settle(t, fixture, receipt)
			if h3Calls.Load() != 1 || result.Outcome.Value.RoundTrips() != 1 || result.Attempts.Exact {
				t.Fatal("fallback peer/transport-entry evidence differs")
			}
			if control.fallback {
				if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().Protocol() != "HTTP/1.1" || string(result.Outcome.Value.DataCopy()) != "fallback" || tlsCalls.Load() != 1 || factoryCalls.Load() != 2 {
					t.Logf("H3 HEADERS=%d TLS requests=%d factories=%d", h3Calls.Load(), tlsCalls.Load(), factoryCalls.Load())
					reviewRouteErrorChain(t, err)
					reviewRouteErrorChain(t, result.Err())
					t.Fatal("actual permitted H3-to-TLS fallback failed", err, result.Err())
				}
			} else if err == nil || result.Err() == nil || result.Outcome.Value.Complete() || tlsCalls.Load() != 0 {
				t.Fatal("one-shot multipart replay crossed TLS fallback boundary", err, result.Err())
			}
			if !control.fallback {
				var nativeError *nativeh3.Error
				if !errors.As(err, &nativeError) || nativeError.ErrorCode != nativeh3.ErrCode(control.code) {
					t.Fatal("native fallback/refusal code lost", err)
				}
				if control.replayable && factoryCalls.Load() != 1 {
					t.Fatal("non-fallback native error invoked replay factory")
				}
			}
			inputsMu.Lock()
			for _, input := range inputs {
				if input.closed.Load() != 1 {
					t.Errorf("fallback input close count=%d", input.closed.Load())
				}
			}
			inputsMu.Unlock()
			t.Logf("code=%#x replayable=%v H3 HEADERS=%d TLS requests=%d factories=%d transport entries=%d protocol=%q complete=%v", control.code, control.replayable, h3Calls.Load(), tlsCalls.Load(), factoryCalls.Load(), result.Outcome.Value.RoundTrips(), result.Outcome.Value.Metadata().Protocol(), result.Outcome.Value.Complete())
			reviewRouteReleased(t, fixture)
		})
	}
}
