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

package httpcloak

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dunglas/httpsfv"
	"github.com/frost-leo/fathomry/internal/fault"
	dnswire "github.com/miekg/dns"
	officialquic "github.com/quic-go/quic-go"
	officialh3 "github.com/quic-go/quic-go/http3"
	"github.com/sardanioss/httpcloak/transport"
)

type masqueRoute struct{ host, target string }
type masqueFixture struct {
	address                          string
	active, denied, packets, tunnels atomic.Int32
	problems                         chan error
}

func masqueRelay(t *testing.T, certificate tls.Certificate, routes map[string]masqueRoute) *masqueFixture {
	t.Helper()
	fixture := &masqueFixture{problems: make(chan error, 16)}
	fixture.address = protocolH3PeerConfig(t, &tls.Config{Certificates: []tls.Certificate{certificate}}, &officialquic.Config{InitialPacketSize: 1400}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		auth := strings.TrimPrefix(request.Header.Get("Proxy-Authorization"), "Basic ")
		credentials, _ := base64.StdEncoding.DecodeString(auth)
		route, valid := routes[string(credentials)]
		if !valid {
			fixture.denied.Add(1)
			writer.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		settings := writer.(officialh3.Settingser)
		select {
		case <-settings.ReceivedSettings():
		case <-request.Context().Done():
			return
		}
		if !settings.Settings().EnableDatagrams {
			fixture.problems <- errors.New("MASQUE client did not advertise H3_DATAGRAM")
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		target, err := net.ResolveUDPAddr("udp", route.target)
		if err != nil {
			fixture.problems <- err
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		wantPath := "/.well-known/masque/udp/" + url.QueryEscape(route.host) + "/" + strconv.Itoa(target.Port) + "/"
		if request.Method != http.MethodConnect || request.Proto != "connect-udp" || request.URL.EscapedPath() != wantPath || request.Header.Get("Capsule-Protocol") != "?1" || request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
			fixture.problems <- errors.New("CONNECT-UDP target, method, context or origin credential crossed route")
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		upstream, err := net.DialUDP("udp", nil, target)
		if err != nil {
			fixture.problems <- err
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		defer upstream.Close()
		writer.Header().Set("Capsule-Protocol", "?1; future=\"ignored\"; flag")
		writer.WriteHeader(http.StatusOK)
		stream := writer.(officialh3.HTTPStreamer).HTTPStream()
		defer stream.Close()
		fixture.active.Add(1)
		fixture.tunnels.Add(1)
		defer fixture.active.Add(-1)
		returned := make(chan struct{})
		go func() {
			defer close(returned)
			packet := make([]byte, 65535)
			for {
				count, err := upstream.Read(packet)
				if err != nil {
					return
				}
				data := append([]byte{0}, packet[:count]...)
				if err := stream.SendDatagram(data); err != nil {
					select {
					case fixture.problems <- err:
					default:
					}
					return
				}
			}
		}()
		defer func() { _ = upstream.Close(); <-returned }()
		for {
			packet, err := stream.ReceiveDatagram(request.Context())
			if err != nil {
				return
			}
			if len(packet) == 0 || packet[0] != 0 {
				fixture.problems <- errors.New("CONNECT-UDP used an unregistered context")
				return
			}
			fixture.packets.Add(1)
			if _, err := upstream.Write(packet[1:]); err != nil {
				return
			}
		}
	}))
	t.Cleanup(func() {
		select {
		case err := <-fixture.problems:
			t.Error("MASQUE independent peer", err)
		default:
		}
	})
	return fixture
}

func TestManagedMASQUERoutesRetainTunnelAndSeparateTLSIdentity(t *testing.T) {
	originCertificate, originRoots := protocolCertificate(t, "one.invalid", "two.invalid")
	proxyCertificate, proxyRoots := protocolCertificate(t, "proxy.invalid")
	var firstCalls, secondCalls atomic.Int32
	origin := func(want string, calls *atomic.Int32) http.HandlerFunc {
		return func(writer http.ResponseWriter, input *http.Request) {
			calls.Add(1)
			if input.Header.Get("Proxy-Authorization") != "" || input.Header.Get("Authorization") != "Bearer origin" || input.Header.Get("Cookie") != "origin=only" {
				t.Error("origin and proxy credentials were not isolated")
			}
			_, _ = io.WriteString(writer, want)
		}
	}
	first := protocolH3Peer(t, &tls.Config{Certificates: []tls.Certificate{originCertificate}}, origin("one", &firstCalls))
	second := protocolH3Peer(t, &tls.Config{Certificates: []tls.Certificate{originCertificate}}, origin("two", &secondCalls))
	firstURL, _ := url.Parse(first)
	secondURL, _ := url.Parse(second)
	relay := masqueRelay(t, proxyCertificate, map[string]masqueRoute{"alpha:first": {"one.invalid", firstURL.Host}, "beta:second": {"two.invalid", secondURL.Host}})
	proxyURL, _ := url.Parse(relay.address)
	var queries, originsVerified, proxiesVerified atomic.Int32
	options := OptionsV1{Name: "masque-routes", PresetName: "chrome-148", Protocol: HTTP3, DisableECH: true, MaxActive: 2, MaxBindings: 2, MaxConnections: 6, MaxQUICConnections: 4, MaxControlStreams: 2, Timeout: time.Second,
		Native: NativeOptionsV1{Verify: &transport.TLSVerify{RootCAs: originRoots, VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 || !bytes.Equal(state.PeerCertificates[0].Raw, originCertificate.Certificate[0]) {
				return errors.New("origin verified proxy identity")
			}
			originsVerified.Add(1)
			return nil
		}}, ProxyVerify: &transport.TLSVerify{RootCAs: proxyRoots, VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 || !bytes.Equal(state.PeerCertificates[0].Raw, proxyCertificate.Certificate[0]) {
				return errors.New("proxy verified origin identity")
			}
			proxiesVerified.Add(1)
			return nil
		}}}}
	options.ResolverAddress = dnsPeer(t, func(writer dnswire.ResponseWriter, input *dnswire.Msg) {
		queries.Add(1)
		response := new(dnswire.Msg)
		response.SetReply(input)
		if input.Question[0].Qtype == dnswire.TypeA {
			response.Answer = []dnswire.RR{&dnswire.A{Hdr: dnswire.RR_Header{Name: input.Question[0].Name, Rrtype: dnswire.TypeA, Class: dnswire.ClassINET, Ttl: 60}, A: net.IPv4(127, 0, 0, 1)}}
		}
		_ = writer.WriteMsg(response)
	})
	fixture := bindFixture(t, options, 2)
	ctx := testContext(t)
	for iteration := range 2 {
		var workers sync.WaitGroup
		for _, entry := range []struct{ user, password, host, port, want string }{{"alpha", "first", "one.invalid", firstURL.Port(), "one"}, {"beta", "second", "two.invalid", secondURL.Port(), "two"}} {
			workers.Add(1)
			go func() {
				defer workers.Done()
				proxy := &url.URL{Scheme: "masque", Host: proxyURL.Host, User: url.UserPassword(entry.user, entry.password)}
				input := request(t, "GET", "https://"+net.JoinHostPort(entry.host, entry.port), nil)
				input.Header.Set("Authorization", "Bearer origin")
				input.Header.Set("Cookie", "origin=only")
				receipt, err := fixture.client.Do(ctx, ctx, fault.Correlation{Call: entry.want}, input, RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: proxy.String()})
				if err != nil {
					t.Error("MASQUE route", iteration, entry.want, err)
					t.Log("MASQUE wire counters", relay.tunnels.Load(), relay.packets.Load(), originsVerified.Load(), proxiesVerified.Load(), queries.Load())
					return
				}
				result, err := receipt.WaitReleased(ctx)
				if err != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != entry.want {
					t.Error("MASQUE response crossed target", iteration, entry.want, err)
				}
			}()
		}
		workers.Wait()
		for range 2 {
			delivery, err := fixture.inbox.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := delivery.Receipt().WaitReleased(ctx); err != nil {
				t.Fatal(err)
			}
			if err := delivery.Release(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if firstCalls.Load() != 2 || secondCalls.Load() != 2 || relay.tunnels.Load() != 2 || relay.active.Load() != 2 || relay.packets.Load() == 0 || originsVerified.Load() != 2 || proxiesVerified.Load() != 2 || queries.Load() != 4 {
		t.Fatal("MASQUE route/reuse/TLS/DNS observations differ", firstCalls.Load(), secondCalls.Load(), relay.tunnels.Load(), relay.active.Load(), originsVerified.Load(), proxiesVerified.Load(), queries.Load())
	}
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	fixture.client.owner.mu.Lock()
	sockets, quic, controls := fixture.client.owner.connections, fixture.client.owner.quicActive, fixture.client.owner.controlActive
	fixture.client.owner.mu.Unlock()
	if sockets != 0 || quic != 0 || controls != 0 {
		t.Fatal("MASQUE source retained physical resources", sockets, quic, controls)
	}
	deadline := time.After(time.Second)
	for relay.active.Load() != 0 {
		select {
		case <-deadline:
			t.Fatal("independent MASQUE peer retained tunnels after source release")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestManagedMASQUEFailureNeverFallsBackAndReleasesSetup(t *testing.T) {
	for _, mode := range []string{"authentication", "capsule-refused", "capsule-conflict", "capsule-param-key", "capsule-unclosed-string", "capsule-false", "capsule-token", "capsule-list", "capsule-content-length", "capsule-content-type", "capsule-bodyless-status", "cancel-response", "inner-quic-capacity", "proxy-trust", "response-header-bound"} {
		t.Run(mode, func(t *testing.T) {
			originCertificate, originRoots := protocolCertificate(t)
			proxyCertificate, proxyRoots := protocolCertificate(t)
			var originCalls, proxyCalls atomic.Int32
			origin := protocolH3Peer(t, &tls.Config{Certificates: []tls.Certificate{originCertificate}}, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				originCalls.Add(1)
				_, _ = io.WriteString(writer, "direct fallback")
			}))
			entered, ended := make(chan struct{}), make(chan struct{})
			proxy := protocolH3Peer(t, &tls.Config{Certificates: []tls.Certificate{proxyCertificate}}, http.HandlerFunc(func(writer http.ResponseWriter, input *http.Request) {
				proxyCalls.Add(1)
				close(entered)
				defer close(ended)
				switch mode {
				case "authentication":
					writer.WriteHeader(http.StatusProxyAuthRequired)
				case "capsule-refused":
					writer.WriteHeader(http.StatusOK)
				case "capsule-conflict":
					writer.Header().Add("Capsule-Protocol", "?1")
					writer.Header().Add("Capsule-Protocol", "?1")
					writer.WriteHeader(http.StatusOK)
				case "capsule-param-key", "capsule-unclosed-string", "capsule-false", "capsule-token", "capsule-list":
					field := map[string]string{"capsule-param-key": "?1; =", "capsule-unclosed-string": "?1; value=\"unterminated", "capsule-false": "?0", "capsule-token": "true", "capsule-list": "?1; flag, ?1"}[mode]
					writer.Header().Set("Capsule-Protocol", field)
					writer.WriteHeader(http.StatusOK)
				case "capsule-content-length":
					writer.Header().Set("Capsule-Protocol", "?1")
					writer.Header().Set("Content-Length", "0")
					writer.WriteHeader(http.StatusOK)
				case "capsule-content-type":
					writer.Header().Set("Capsule-Protocol", "?1")
					writer.Header().Set("Content-Type", "application/octet-stream")
					writer.WriteHeader(http.StatusOK)
				case "capsule-bodyless-status":
					writer.Header().Set("Capsule-Protocol", "?1")
					writer.WriteHeader(http.StatusNoContent)
				case "response-header-bound":
					writer.Header().Set("Capsule-Protocol", "?1")
					writer.Header().Set("X-Large", strings.Repeat("a", 1536))
					writer.WriteHeader(http.StatusOK)
				case "cancel-response":
					<-input.Context().Done()
				case "inner-quic-capacity":
					writer.Header().Set("Capsule-Protocol", "?1")
					writer.WriteHeader(http.StatusOK)
					stream := writer.(officialh3.HTTPStreamer).HTTPStream()
					defer stream.Close()
					<-input.Context().Done()
				}
				if strings.HasPrefix(mode, "capsule-") {
					stream := writer.(officialh3.HTTPStreamer).HTTPStream()
					defer stream.Close()
					<-input.Context().Done()
				}
			}))
			proxyURL, _ := url.Parse(proxy)
			proxyURL.Scheme = "masque"
			proxyURL.User = url.UserPassword("user", "fixture-password")
			options := OptionsV1{Name: "masque-failure", PresetName: "chrome-148", Protocol: HTTP3, DisableECH: true, ProxyURL: proxyURL.String(), Native: NativeOptionsV1{Verify: &transport.TLSVerify{RootCAs: originRoots}, ProxyVerify: &transport.TLSVerify{RootCAs: proxyRoots}}}
			if mode == "inner-quic-capacity" || strings.HasPrefix(mode, "capsule-") {
				options.MaxQUICConnections = 1
			}
			if mode == "response-header-bound" {
				options.MaxHeaderBytes = 1024
			}
			if mode == "proxy-trust" {
				options.InsecureSkipVerify = true
				options.Native.ProxyVerify.RootCAs = originRoots
			}
			fixture := bindFixture(t, options, 1)
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			cancelDone := make(chan struct{})
			if mode == "cancel-response" {
				go func() {
					defer close(cancelDone)
					select {
					case <-entered:
						cancel()
					case <-ctx.Done():
					}
				}()
			}
			started := time.Now()
			receipt, err := fixture.client.Do(ctx, testContext(t), fault.Correlation{Call: mode}, request(t, "GET", origin, nil))
			if mode == "cancel-response" {
				<-cancelDone
			}
			if err == nil {
				t.Fatal("failed MASQUE setup silently succeeded")
			}
			result := settle(t, fixture, receipt)
			if strings.HasPrefix(mode, "capsule-") && errors.Is(result.Err(), ErrLimit) {
				t.Fatal("invalid capsule negotiation reached inner QUIC admission", result.Err())
			}
			if mode == "inner-quic-capacity" && !errors.Is(result.Err(), ErrLimit) {
				t.Fatal("valid negotiation did not reach the controlled inner QUIC limit", result.Err())
			}
			if mode == "capsule-refused" || mode == "capsule-conflict" || mode == "capsule-param-key" || mode == "capsule-unclosed-string" || mode == "capsule-list" {
				var parseError *httpsfv.UnmarshalError
				if !errors.As(result.Err(), &parseError) {
					t.Fatal("malformed capsule item was not refused by the selected parser", result.Err())
				}
			}
			if (mode == "cancel-response" || mode == "response-header-bound") && time.Since(started) > time.Second {
				t.Fatal("cancellation or header bound waited for a native idle timeout")
			}
			if result.Outcome.Value.Complete() || originCalls.Load() != 0 {
				t.Fatal("failed MASQUE setup reached the origin directly")
			}
			fixture.client.owner.mu.Lock()
			sockets, quic, controls := fixture.client.owner.connections, fixture.client.owner.quicActive, fixture.client.owner.controlActive
			fixture.client.owner.mu.Unlock()
			if sockets != 0 || quic != 0 || controls != 0 {
				t.Fatal("failed setup retained source resources", sockets, quic, controls)
			}
			if mode == "proxy-trust" {
				if proxyCalls.Load() != 0 {
					t.Fatal("origin insecure option weakened proxy authentication")
				}
			} else {
				select {
				case <-ended:
				case <-time.After(time.Second):
					t.Fatal("proxy request retained after failed setup")
				}
			}
		})
	}
}

func TestManagedMASQUEControlCapacityPreservesHealthyTunnel(t *testing.T) {
	originCertificate, originRoots := protocolCertificate(t)
	proxyCertificate, proxyRoots := protocolCertificate(t)
	var firstCalls, secondCalls atomic.Int32
	first := protocolH3Peer(t, &tls.Config{Certificates: []tls.Certificate{originCertificate}}, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		firstCalls.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	second := protocolH3Peer(t, &tls.Config{Certificates: []tls.Certificate{originCertificate}}, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	firstURL, _ := url.Parse(first)
	relay := masqueRelay(t, proxyCertificate, map[string]masqueRoute{"alpha:first": {"127.0.0.1", firstURL.Host}})
	proxy, _ := url.Parse(relay.address)
	proxy.Scheme, proxy.User = "masque", url.UserPassword("alpha", "first")
	fixture := bindFixture(t, OptionsV1{Name: "masque-control-bound", PresetName: "chrome-148", Protocol: HTTP3, DisableECH: true, ProxyURL: proxy.String(), MaxBindings: 2, MaxControlStreams: 1, MaxQUICConnections: 4,
		Native: NativeOptionsV1{Verify: &transport.TLSVerify{RootCAs: originRoots}, ProxyVerify: &transport.TLSVerify{RootCAs: proxyRoots}}}, 1)
	for _, entry := range []struct {
		address string
		refused bool
	}{{first, false}, {second, true}, {first, false}} {
		receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "control-bound"}, request(t, "GET", entry.address, nil))
		if entry.refused != (err != nil) {
			t.Fatal("MASQUE control admission result changed", entry.refused, err)
		}
		result := settle(t, fixture, receipt)
		if entry.refused {
			if !errors.Is(result.Outcome.Primary, ErrLimit) || result.Outcome.Value.Complete() {
				t.Fatal("actual control-stream capacity refusal lost its cause", result.Outcome.Primary)
			}
		} else if !result.Outcome.Value.Complete() {
			t.Fatal("healthy tunnel was canceled by a sibling control refusal")
		}
	}
	fixture.client.owner.mu.Lock()
	controls, quic := fixture.client.owner.controlActive, fixture.client.owner.quicActive
	fixture.client.owner.mu.Unlock()
	if firstCalls.Load() != 2 || secondCalls.Load() != 0 || relay.tunnels.Load() != 1 || controls != 1 || quic != 2 {
		t.Fatal("control cap or existing tunnel ownership changed", firstCalls.Load(), secondCalls.Load(), relay.tunnels.Load(), controls, quic)
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	fixture.client.owner.mu.Lock()
	defer fixture.client.owner.mu.Unlock()
	if fixture.client.owner.connections != 0 || fixture.client.owner.controlActive != 0 || fixture.client.owner.quicActive != 0 {
		t.Fatal("control-cap failure left source resources behind")
	}
}
